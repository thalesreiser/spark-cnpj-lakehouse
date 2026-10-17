"""RF-02: CSV ingestion into the bronze layer (Delta) with ``_ingest_ts`` and ``_source_file``.

One run ingests the tables of one reference month (``competencia``) downloaded by RF-01 into
``data/raw/<competencia>/``. For each table:

1. the source files come from ``manifest.json`` (zip name -> extracted CSVs), mapped to their
   table with `cnpj_lakehouse.schemas.table_for_file`;
2. **layout check** (ADR 008): the raw lines are read as text and the number of ``;``-separated
   fields of each line is counted outside quoted values (doubled quotes honoured). With a fixed
   schema in ``PERMISSIVE`` mode Spark would silently drop extra fields and null missing ones, so
   any line whose field count differs from the schema fails the run (threshold configurable);
3. the CSV is read with the explicit bronze schema (all strings, no ``inferSchema``) and the
   shared `CSV_READ_OPTIONS`;
4. metadata columns are added: ``_ingest_ts`` (one UTC timestamp for the whole run),
   ``_source_file`` (path of the source CSV, from Spark's ``_metadata.file_path``) and the
   partition column ``competencia`` (ADR 002);
5. ``SPARK_SAMPLE_FRACTION`` (RNF-03) samples the rows deterministically (fixed seed);
6. the rows are written to ``s3a://<bucket>/bronze/<table>/`` partitioned by ``competencia``.
   Re-running a month replaces its partition (Delta ``replaceWhere``), never duplicates it;
7. rows read and written are logged and returned in a `TableReport`.

Steps 2-5 are pure DataFrame functions (RNF-04); `ingest_table` and `main` orchestrate them.

Usage (inside the ``spark-client`` container, see ``make bronze``)::

    python -m cnpj_lakehouse.bronze --competencia 2026-09 [--tables empresas cnaes]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse.download import MANIFEST_NAME, competencia_dir, load_manifest, parse_competencia
from cnpj_lakehouse.schemas import (
    CSV_READ_OPTIONS,
    QUOTE,
    SEPARATOR,
    TABLES,
    TableSpec,
    get_table,
    table_for_file,
)
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger("cnpj_lakehouse.bronze")

# Metadata columns added to every bronze table.
INGEST_TS = "_ingest_ts"
SOURCE_FILE = "_source_file"
PARTITION_COLUMN = "competencia"
METADATA_COLUMNS: tuple[str, ...] = (INGEST_TS, SOURCE_FILE, PARTITION_COLUMN)

DEFAULT_RAW_DIR = Path("data/raw")
BRONZE_LAYER = "bronze"
SAMPLE_FRACTION_ENV = "SPARK_SAMPLE_FRACTION"
# Fixed seed: the same input and fraction always produce the same sample (RNF-03).
SAMPLE_SEED = 42
# Lines with a wrong field count tolerated before the run fails (default: none).
DEFAULT_MAX_BAD_LINES = 0
# Bad lines kept in the report and the log, to diagnose a layout change.
BAD_LINE_EXAMPLES = 3

# A quoted value: opening quote, any characters with quotes doubled inside, closing quote
# (an "unrolled loop", which avoids deep regex backtracking on long lines).
_QUOTED_VALUE = f"{QUOTE}[^{QUOTE}]*(?:{QUOTE}{QUOTE}[^{QUOTE}]*)*{QUOTE}"


class LayoutError(RuntimeError):
    """Source lines whose field count differs from the table schema (possible layout change)."""


@dataclass(frozen=True)
class LayoutCheck:
    """Result of the field-count check of a table's source lines."""

    table: str
    expected_fields: int
    lines: int
    bad_lines: int
    field_counts: dict[int, int]
    examples: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.bad_lines == 0


@dataclass(frozen=True)
class TableReport:
    """What one table ingestion read and wrote (logged and written to the run summary)."""

    table: str
    competencia: str
    path: str
    source_files: tuple[str, ...]
    rows_read: int
    rows_written: int
    sample_fraction: float
    sampled: bool
    layout_bad_lines: int
    ingest_ts: str


# --------------------------------------------------------------------------------------------
# Pure functions
# --------------------------------------------------------------------------------------------


def parse_sample_fraction(value: str | None) -> float:
    """Validates ``SPARK_SAMPLE_FRACTION``: a number in (0, 1]; unset or empty means 1.0."""
    if value is None or value.strip() == "":
        return 1.0
    try:
        fraction = float(value)
    except ValueError:
        raise ValueError(
            f"{SAMPLE_FRACTION_ENV} must be a number in (0, 1], got {value!r}"
        ) from None
    if not 0.0 < fraction <= 1.0:
        raise ValueError(f"{SAMPLE_FRACTION_ENV} must be in (0, 1], got {fraction}")
    return fraction


def source_files_by_table(
    manifest: Mapping[str, Any], raw_dir: Path, tables: Iterable[str] | None = None
) -> dict[str, list[Path]]:
    """Extracted CSVs of each table, from ``manifest.json`` (tables in `TABLES` order).

    ``raw_dir`` is the folder of the reference month (``data/raw/<competencia>``). With
    ``tables``, only those tables are returned and each must be present in the manifest.
    """
    found: dict[str, list[Path]] = {}
    for entry in manifest.get("files", []):
        table = table_for_file(str(entry["name"])).name
        found.setdefault(table, []).extend(raw_dir / str(name) for name in entry["extracted"])
    if tables is not None:
        wanted = [get_table(name).name for name in tables]
        missing = [name for name in wanted if name not in found]
        if missing:
            raise ValueError(
                f"table(s) {', '.join(missing)} not in {raw_dir / MANIFEST_NAME}"
                f" (available: {', '.join(found) or 'none'}); run `make download` first"
            )
        found = {name: found[name] for name in wanted}
    return {name: sorted(found[name]) for name in TABLES if name in found}


def field_count(line: Column) -> Column:
    """Number of ``;``-separated fields of a raw CSV line, ignoring ``;`` inside quoted values."""
    separators_only = F.regexp_replace(line, _QUOTED_VALUE, "")
    return (
        F.length(separators_only) - F.length(F.regexp_replace(separators_only, SEPARATOR, "")) + 1
    )


def check_layout(lines: DataFrame, table: TableSpec) -> LayoutCheck:
    """Counts the fields of every non-empty raw line (``value`` column of ``spark.read.text``).

    Empty lines are skipped, as the CSV reader does, so ``lines`` of the result is the number of
    records the CSV holds.
    """
    expected = len(table.columns)
    counted = lines.where(F.length(F.trim(F.col("value"))) > 0).select(
        F.col("value"), field_count(F.col("value")).alias("fields")
    )
    histogram = {
        int(row["fields"]): int(row["count"]) for row in counted.groupBy("fields").count().collect()
    }
    bad_lines = sum(count for fields, count in histogram.items() if fields != expected)
    examples: tuple[str, ...] = ()
    if bad_lines:
        rows = counted.where(F.col("fields") != expected).limit(BAD_LINE_EXAMPLES).collect()
        examples = tuple(f"{row['fields']} fields: {str(row['value'])[:200]}" for row in rows)
    return LayoutCheck(
        table=table.name,
        expected_fields=expected,
        lines=sum(histogram.values()),
        bad_lines=bad_lines,
        field_counts=dict(sorted(histogram.items())),
        examples=examples,
    )


def read_raw_lines(spark: SparkSession, paths: Sequence[Path]) -> DataFrame:
    """Raw lines of the source files (the encoding does not matter: ``;`` and ``"`` are ASCII)."""
    return spark.read.text([_uri(path) for path in paths])


def read_source(spark: SparkSession, table: TableSpec, paths: Sequence[Path]) -> DataFrame:
    """Reads the CSVs of a table with its explicit bronze schema (all strings, no inference)."""
    return (
        spark.read.options(**CSV_READ_OPTIONS)
        .schema(table.bronze_schema())
        .csv([_uri(path) for path in paths])
    )


def add_metadata(df: DataFrame, competencia: str, ingest_ts: datetime) -> DataFrame:
    """Adds ``_ingest_ts`` (one UTC timestamp for the batch), ``_source_file`` and the partition
    column ``competencia``. ``df`` must come straight from a file source (`read_source`)."""
    if ingest_ts.tzinfo is None:
        raise ValueError("ingest_ts must be timezone-aware (UTC)")
    return df.select(
        "*",
        F.lit(ingest_ts.astimezone(UTC)).cast("timestamp").alias(INGEST_TS),
        F.col("_metadata.file_path").alias(SOURCE_FILE),
        F.lit(parse_competencia(competencia)).alias(PARTITION_COLUMN),
    )


def apply_sample(df: DataFrame, fraction: float, seed: int = SAMPLE_SEED) -> DataFrame:
    """Deterministic sample (RNF-03); ``fraction`` 1.0 returns ``df`` untouched."""
    if fraction >= 1.0:
        return df
    return df.sample(withReplacement=False, fraction=fraction, seed=seed)


def build_bronze(
    spark: SparkSession,
    table: TableSpec,
    paths: Sequence[Path],
    competencia: str,
    ingest_ts: datetime,
    sample_fraction: float = 1.0,
) -> DataFrame:
    """The bronze DataFrame of a table: source columns + metadata columns, optionally sampled."""
    df = add_metadata(read_source(spark, table, paths), competencia, ingest_ts)
    return apply_sample(df, sample_fraction)


def write_bronze(df: DataFrame, path: str, competencia: str, fmt: str = "delta") -> None:
    """Writes the month's partition, replacing it if it already exists (idempotent re-runs).

    Delta uses ``replaceWhere`` on ``competencia``, which also rejects rows of another month;
    other formats (Parquet, used by the unit tests) use dynamic partition overwrite.
    """
    writer = df.write.format(fmt).mode("overwrite").partitionBy(PARTITION_COLUMN)
    if fmt == "delta":
        writer = writer.option("replaceWhere", f"{PARTITION_COLUMN} = '{competencia}'")
    else:
        writer = writer.option("partitionOverwriteMode", "dynamic")
    writer.save(path)


def _uri(path: Path) -> str:
    # A plain absolute path: `as_uri()` would percent-encode the `$` of names like
    # `F.K03200$Z.D60912.CNAECSV`, which Hadoop does not decode.
    return path.resolve().as_posix()


# --------------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------------


def ingest_table(
    spark: SparkSession,
    table: TableSpec,
    paths: Sequence[Path],
    competencia: str,
    output_path: str,
    ingest_ts: datetime,
    sample_fraction: float = 1.0,
    max_bad_lines: int = DEFAULT_MAX_BAD_LINES,
    fmt: str = "delta",
) -> TableReport:
    """Layout check, read, write and count of one table; raises `LayoutError` on a bad layout."""
    if not paths:
        raise ValueError(f"no source files for table {table.name}")
    log.info("%s: %d source file(s) -> %s", table.name, len(paths), output_path)

    layout = check_layout(read_raw_lines(spark, paths), table)
    log.info(
        "%s: layout check %s - %d line(s), field counts %s (expected %d)",
        table.name,
        "OK" if layout.ok else "FAILED",
        layout.lines,
        layout.field_counts,
        layout.expected_fields,
    )
    if layout.bad_lines > max_bad_lines:
        for example in layout.examples:
            log.error("%s: bad line (%s)", table.name, example)
        raise LayoutError(
            f"{table.name}: {layout.bad_lines} of {layout.lines} line(s) do not have"
            f" {layout.expected_fields} fields (tolerated: {max_bad_lines}); field counts"
            f" {layout.field_counts}. The source layout may have changed (ADR 008)."
        )

    df = build_bronze(spark, table, paths, competencia, ingest_ts, sample_fraction)
    write_bronze(df, output_path, competencia, fmt)
    rows_written = (
        spark.read.format(fmt)
        .load(output_path)
        .where(F.col(PARTITION_COLUMN) == competencia)
        .count()
    )
    sampled = sample_fraction < 1.0
    log.info(
        "%s: rows read %d, rows written %d (%s)",
        table.name,
        layout.lines,
        rows_written,
        f"sampled, fraction {sample_fraction}" if sampled else "no sampling, fraction 1.0",
    )
    return TableReport(
        table=table.name,
        competencia=competencia,
        path=output_path,
        source_files=tuple(path.name for path in paths),
        rows_read=layout.lines,
        rows_written=rows_written,
        sample_fraction=sample_fraction,
        sampled=sampled,
        layout_bad_lines=layout.bad_lines,
        ingest_ts=ingest_ts.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def run(
    spark: SparkSession,
    competencia: str,
    raw_dir: Path,
    output_root: str,
    tables: Iterable[str] | None = None,
    sample_fraction: float = 1.0,
    max_bad_lines: int = DEFAULT_MAX_BAD_LINES,
    fmt: str = "delta",
    ingest_ts: datetime | None = None,
) -> list[TableReport]:
    """Ingests the tables of one reference month into ``<output_root>/<table>``."""
    month_dir = competencia_dir(raw_dir, competencia)
    manifest = load_manifest(month_dir / MANIFEST_NAME)
    if not manifest:
        raise FileNotFoundError(
            f"{month_dir / MANIFEST_NAME} not found; run `make download COMPETENCIA={competencia}`"
        )
    files = source_files_by_table(manifest, month_dir, tables)
    ingest_ts = ingest_ts or datetime.now(UTC)
    log.info(
        "bronze %s: %d table(s) %s, sample fraction %s, ingest_ts %s",
        competencia,
        len(files),
        list(files),
        sample_fraction,
        ingest_ts.isoformat(),
    )
    return [
        ingest_table(
            spark,
            get_table(name),
            paths,
            competencia,
            f"{output_root.rstrip('/')}/{name}",
            ingest_ts,
            sample_fraction,
            max_bad_lines,
            fmt,
        )
        for name, paths in files.items()
    ]


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.bronze",
        description="RF-02: ingest one CNPJ release (reference month) into the bronze layer.",
    )
    parser.add_argument(
        "--competencia", required=True, type=_competencia_arg, help="reference month, YYYY-MM"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=f"tables to ingest ({', '.join(TABLES)}); default: every table in the manifest",
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="default: data/raw")
    parser.add_argument(
        "--output", default=None, help="bronze root; default: s3a://<LAKEHOUSE_BUCKET>/bronze"
    )
    parser.add_argument("--format", default="delta", help="table format (default: delta)")
    parser.add_argument(
        "--max-bad-lines",
        type=int,
        default=DEFAULT_MAX_BAD_LINES,
        help="lines with a wrong field count tolerated per table (default: 0)",
    )
    parser.add_argument("--summary", type=Path, default=None, help="write a JSON run summary")
    return parser.parse_args(argv)


def _competencia_arg(value: str) -> str:
    try:
        return parse_competencia(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; returns the process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        fraction = parse_sample_fraction(os.environ.get(SAMPLE_FRACTION_ENV))
    except ValueError as exc:
        log.error("%s", exc)
        return 2
    cfg = LakehouseConfig.from_env(os.environ)
    output_root = args.output or cfg.path(BRONZE_LAYER)
    spark = get_spark(f"bronze-{args.competencia}", cfg)
    try:
        reports = run(
            spark,
            args.competencia,
            args.raw_dir,
            output_root,
            tables=args.tables,
            sample_fraction=fraction,
            max_bad_lines=args.max_bad_lines,
            fmt=args.format,
        )
    except (LayoutError, FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        summary = {"competencia": args.competencia, "tables": [asdict(r) for r in reports]}
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    for report in reports:
        print(
            f"[bronze] {report.table}: read {report.rows_read}, written {report.rows_written}"
            f" (sample fraction {report.sample_fraction})"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
