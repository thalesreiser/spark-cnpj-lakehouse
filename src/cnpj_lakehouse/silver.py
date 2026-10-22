"""RF-03 / RF-04 / RN-01: typed, deduplicated silver layer (Delta), current state per business key.

One run loads one reference month (``competencia``) of the bronze tables written by RF-02 into
the silver tables. For each table:

1. the month's partition of ``s3a://<bucket>/bronze/<table>/`` is read;
2. the documented target types are applied (`cnpj_lakehouse.normalize.apply_silver_types`,
   ADR 008/009); ``estabelecimentos`` also gets the full ``cnpj`` (RN-01) and the ``cnpj_valido``
   flag (check digits, all-zero rule);
3. rows whose business key is missing after normalization are discarded (a row without a key
   cannot be deduplicated nor joined); rows with an invalid CNPJ are kept with the flag or
   discarded, according to `INVALID_CNPJ_POLICY` (ADR 010);
4. **deduplication**: within each business key (`SilverRule.key`) a window function keeps the
   *current record*, the first row of an explicit, deterministic ordering (`SilverRule.order`,
   always ending with ``competencia``, ``_ingest_ts``, ``_source_file`` and ``_row_hash``, so
   ties never depend on the physical order of the rows);
5. **incremental load (RF-04, ADR 011)**: the month's current records are merged into
   ``s3a://<bucket>/silver/<table>/``, which holds the *current state* of every business key
   across months (not partitioned): a new key is inserted, a known key is updated only when
   its ``_row_hash`` (SHA-256 of the business columns, stored in the table) changed and the
   month is not older than the stored one, and keys absent from the month are kept. The first
   run creates the table. Re-running a month changes nothing (0 inserted, 0 updated);
6. quality counts (rows in and out, duplicates removed, rows without key, invalid and all-zero
   CNPJs, dates that failed to parse) and the Delta operation metrics (rows inserted, updated,
   unchanged, table version) are logged and returned in a `SilverReport`.

Steps 2-4 and the MERGE conditions are pure functions (RNF-04); `merge_silver`, `build_table`
and `main` do the I/O.

Usage (inside the ``spark-client`` container, see ``make silver``)::

    python -m cnpj_lakehouse.silver --competencia 2026-09 [--tables empresas cnaes]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from cnpj_lakehouse.bronze import BRONZE_LAYER, INGEST_TS, PARTITION_COLUMN, SOURCE_FILE
from cnpj_lakehouse.download import parse_competencia
from cnpj_lakehouse.normalize import CNPJ_LENGTH, apply_silver_types, cnpj_root, root_check_digits
from cnpj_lakehouse.schemas import EMPTY_DATE_VALUES, TABLES, get_table
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger("cnpj_lakehouse.silver")

SILVER_LAYER = "silver"
# Columns added to `estabelecimentos` (RN-01 key and its quality flag).
CNPJ_COLUMN = "cnpj"
CNPJ_VALID_COLUMN = "cnpj_valido"
# SHA-256 of the business columns, stored in silver: change detection of the MERGE (ADR 011).
ROW_HASH = "_row_hash"
_ROW_HASH = ROW_HASH
# Bronze metadata: written to silver but not part of the business content (not hashed).
METADATA_COLUMNS = (PARTITION_COLUMN, INGEST_TS, SOURCE_FILE)
# Internal helper columns, never written to silver.
_RAW_PREFIX = "_raw_"
_ROW_NUMBER = "_row_number"
_KEY_MISSING = "_key_missing"
_CNPJ_INVALID = "_cnpj_invalid"
_CNPJ_ZERO = "_cnpj_zero"
_CNPJ_ROOT = "_cnpj_root"
_CNPJ_DV = "_cnpj_dv_computed"
ZERO_CNPJ = "0" * CNPJ_LENGTH

# Spark properties of the silver job (task 6.2, docs/benchmark.md). Bronze holds one month in
# files of ~10 MB, which Spark packs into input splits of up to (month size / cores): 2-3 tasks
# for the heaviest stage (typing, row hash, MERGE source) on the 2 executor cores, one of them
# running alone at the end. 16 MB splits give one task per file and keep both cores busy. Set
# here, not in `spark.TUNING_CONF`, because the bronze job reads the source CSVs and its
# deterministic sample (RNF-03) depends on the input splits.
SILVER_CONF: Mapping[str, str] = MappingProxyType({"spark.sql.files.maxPartitionBytes": "16m"})


class InvalidCnpjPolicy(StrEnum):
    """What silver does with an establishment whose CNPJ fails validation (ADR 010)."""

    FLAG = "flag"  # keep the row, ``cnpj_valido = false``, count it
    DROP = "drop"  # discard the row, count it as rejected


# ADR 010 (proposed): the two open decisions, each a one-line switch.
INVALID_CNPJ_POLICY = InvalidCnpjPolicy.FLAG
ZERO_CNPJ_IS_INVALID = True


@dataclass(frozen=True)
class OrderBy:
    """One criterion of the current-record ordering: most recent (``desc``) first, nulls last."""

    column: str
    descending: bool = True

    def expression(self) -> Column:
        col = F.col(self.column)
        return col.desc_nulls_last() if self.descending else col.asc_nulls_last()


@dataclass(frozen=True)
class SilverRule:
    """Business key and current-record ordering of one silver table.

    ``order`` holds the table-specific criteria; `ordering` appends the common tie-breakers
    (``competencia``, ``_ingest_ts``, ``_source_file``, then the row hash), so the choice is
    deterministic even when every business column ties.
    """

    table: str
    key: tuple[str, ...]
    order: tuple[OrderBy, ...] = ()
    # Key columns that must be filled for the row to be kept (the not-nullable ones).
    required: tuple[str, ...] = ()

    def ordering(self) -> tuple[OrderBy, ...]:
        return (
            OrderBy(PARTITION_COLUMN),
            *self.order,
            OrderBy(INGEST_TS),
            OrderBy(SOURCE_FILE),
            OrderBy(_ROW_HASH),
        )


# Latest event among the Simples Nacional / MEI dates of a row (computed column).
_SIMPLES_LATEST_EVENT = "_latest_event"
_SIMPLES_DATES = (
    "data_opcao_simples",
    "data_exclusao_simples",
    "data_opcao_mei",
    "data_exclusao_mei",
)

RN01_KEY = ("cnpj_basico", "cnpj_ordem", "cnpj_dv")
SOCIOS_KEY = ("cnpj_basico", "identificador_socio", "cpf_cnpj_socio", "nome_socio_razao_social")


def _rules() -> dict[str, SilverRule]:
    rules = {
        "empresas": SilverRule("empresas", ("cnpj_basico",), required=("cnpj_basico",)),
        "estabelecimentos": SilverRule(
            "estabelecimentos",
            RN01_KEY,
            (OrderBy("data_situacao_cadastral"), OrderBy("data_inicio_atividade")),
            required=RN01_KEY,
        ),
        "socios": SilverRule(
            "socios", SOCIOS_KEY, (OrderBy("data_entrada_sociedade"),), required=("cnpj_basico",)
        ),
        "simples": SilverRule(
            "simples",
            ("cnpj_basico",),
            (OrderBy(_SIMPLES_LATEST_EVENT),),
            required=("cnpj_basico",),
        ),
    }
    for name, spec in TABLES.items():
        if name not in rules:  # lookup tables: one row per code
            rules[name] = SilverRule(name, ("codigo",), required=("codigo",))
        assert set(rules[name].key) <= set(spec.column_names), name
    return rules


RULES: Mapping[str, SilverRule] = MappingProxyType(_rules())


@dataclass(frozen=True)
class SilverOptions:
    """The ADR 010 switches (defaults: the module constants)."""

    invalid_cnpj_policy: InvalidCnpjPolicy = INVALID_CNPJ_POLICY
    zero_cnpj_is_invalid: bool = ZERO_CNPJ_IS_INVALID


DEFAULT_OPTIONS = SilverOptions()


@dataclass(frozen=True)
class QualityCounts:
    """Counts of one silver build (no data, RST-01)."""

    rows_in: int
    rows_without_key: int
    invalid_cnpj: int
    zero_cnpj: int
    invalid_cnpj_dropped: int
    rows_deduplicated_in: int
    date_values: dict[str, int]
    date_parse_failures: dict[str, int]


@dataclass(frozen=True)
class SilverReport:
    """What one silver table build read, discarded and wrote (logged and in the run summary)."""

    table: str
    competencia: str
    path: str
    key: tuple[str, ...]
    rows_in: int
    rows_out: int  # current records of the month (the MERGE source)
    duplicates_removed: int
    rows_without_key: int
    invalid_cnpj: int
    zero_cnpj: int
    invalid_cnpj_policy: str
    invalid_cnpj_dropped: int
    date_values: dict[str, int]
    date_parse_failures: dict[str, int]
    operation: str  # "create" (first run) or "merge"
    table_version: int  # Delta version written (or the current one when nothing changed)
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    table_rows: int  # rows of the silver table after the run (every month)


@dataclass(frozen=True)
class WriteMetrics:
    """What one silver write did to the Delta table (from its commit's operation metrics)."""

    operation: str
    version: int
    source_rows: int
    inserted: int
    updated: int

    @property
    def unchanged(self) -> int:
        return self.source_rows - self.inserted - self.updated


# --------------------------------------------------------------------------------------------
# Pure functions
# --------------------------------------------------------------------------------------------


def get_rule(table: str) -> SilverRule:
    return RULES[get_table(table).name]


def date_columns(table: str) -> tuple[str, ...]:
    """Columns typed as ``date`` in silver."""
    return tuple(
        name
        for name, dtype in get_table(table).silver_types().items()
        if isinstance(dtype, DateType)
    )


def is_empty_date(column: Column) -> Column:
    """True for a source date meaning "no date" (``null``, ``""``, ``0``, ``00000000``)."""
    value = F.trim(column)
    return value.isNull() | value.isin(*EMPTY_DATE_VALUES)


def type_and_flag(
    bronze: DataFrame, table: str, options: SilverOptions = DEFAULT_OPTIONS
) -> DataFrame:
    """Bronze rows -> typed rows plus the helper flags used by the counts and the filters.

    The raw date strings are kept as ``_raw_<column>`` so failed parses can be counted;
    ``estabelecimentos`` gets ``cnpj`` (RN-01: base + order + DV) and ``cnpj_valido``.
    """
    rule = get_rule(table)
    dates = date_columns(table)
    raw = bronze.select("*", *(F.col(name).alias(_RAW_PREFIX + name) for name in dates))
    typed = apply_silver_types(raw, table)
    missing = F.lit(False)
    for name in rule.required:
        missing = missing | F.col(name).isNull()
    typed = typed.withColumn(_KEY_MISSING, missing)
    if rule.key == RN01_KEY:
        # Built in steps over plain column references: nesting the normalizers inside the
        # check-digit expression would make the generated code explode (driver OOM). The root
        # (12 characters) and the computed check digits are helper columns of their own: the
        # check digits read the root 12 times, so Spark keeps the root in its own projection
        # and computes it once per row instead of 12 times inside a conditional (task 6.2).
        missing = F.col(_KEY_MISSING)
        cnpj = F.col(CNPJ_COLUMN)
        zero = F.coalesce(cnpj == ZERO_CNPJ, F.lit(False))
        valid = F.coalesce(F.substring(cnpj, 13, 2) == F.col(_CNPJ_DV), F.lit(False))
        if options.zero_cnpj_is_invalid:
            valid = valid & ~zero
        typed = (
            typed.withColumn(CNPJ_COLUMN, F.concat(*map(F.col, RN01_KEY)))
            .withColumn(_CNPJ_ROOT, cnpj_root(F.substring(cnpj, 1, 12)))
            .withColumn(_CNPJ_DV, root_check_digits(_CNPJ_ROOT))
            .withColumn(CNPJ_VALID_COLUMN, F.when(~missing, valid))
            .withColumn(_CNPJ_ZERO, ~missing & zero)
            .withColumn(_CNPJ_INVALID, ~missing & ~F.col(CNPJ_VALID_COLUMN))
        )
    else:
        typed = typed.withColumn(_CNPJ_ZERO, F.lit(False)).withColumn(_CNPJ_INVALID, F.lit(False))
    if table == "simples":
        typed = typed.withColumn(_SIMPLES_LATEST_EVENT, F.greatest(*map(F.col, _SIMPLES_DATES)))
    return typed


def quality_counts(
    flagged: DataFrame, table: str, options: SilverOptions = DEFAULT_OPTIONS
) -> QualityCounts:
    """Counts of a `type_and_flag` DataFrame in one aggregation (one pass over the data).

    A date parse failure is a non-empty source value (not ``""``, ``0``, ``00000000``) that
    became ``null``; ``date_values`` counts the non-empty source values of each date column.
    """
    dates = date_columns(table)
    drop = options.invalid_cnpj_policy is InvalidCnpjPolicy.DROP

    def total(condition: Column) -> Column:
        return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

    aggs = [
        F.count(F.lit(1)).alias("rows_in"),
        total(F.col(_KEY_MISSING)).alias("rows_without_key"),
        total(F.col(_CNPJ_INVALID)).alias("invalid_cnpj"),
        total(F.col(_CNPJ_ZERO)).alias("zero_cnpj"),
    ]
    for name in dates:
        filled = ~is_empty_date(F.col(_RAW_PREFIX + name))
        aggs.append(total(filled).alias(f"values:{name}"))
        aggs.append(total(filled & F.col(name).isNull()).alias(f"failures:{name}"))
    row = flagged.agg(*aggs).first()
    assert row is not None
    invalid = int(row["invalid_cnpj"])
    without_key = int(row["rows_without_key"])
    dropped = invalid if drop else 0
    return QualityCounts(
        rows_in=int(row["rows_in"]),
        rows_without_key=without_key,
        invalid_cnpj=invalid,
        zero_cnpj=int(row["zero_cnpj"]),
        invalid_cnpj_dropped=dropped,
        rows_deduplicated_in=int(row["rows_in"]) - without_key - dropped,
        date_values={name: int(row[f"values:{name}"]) for name in dates},
        date_parse_failures={name: int(row[f"failures:{name}"]) for name in dates},
    )


def select_rows(flagged: DataFrame, options: SilverOptions = DEFAULT_OPTIONS) -> DataFrame:
    """Discards rows without a business key and, with the ``drop`` policy, invalid CNPJs."""
    kept = flagged.where(~F.col(_KEY_MISSING))
    if options.invalid_cnpj_policy is InvalidCnpjPolicy.DROP:
        kept = kept.where(~F.col(_CNPJ_INVALID))
    return kept


def row_hash(columns: Sequence[str]) -> Column:
    """SHA-256 of the given values (JSON of a struct, nulls included)."""
    return F.sha2(F.to_json(F.struct(*columns), {"ignoreNullFields": "false"}), 256)


def content_columns(df: DataFrame) -> list[str]:
    """Business columns hashed into ``_row_hash``: silver columns minus the bronze metadata.

    ``competencia``, ``_ingest_ts`` and ``_source_file`` are left out, so the same record
    loaded from another month or ingestion has the same hash (change detection, ADR 011).
    """
    excluded = {*METADATA_COLUMNS, _ROW_HASH}
    return [c for c in silver_columns(df) if c not in excluded]


def deduplicate(df: DataFrame, rule: SilverRule) -> DataFrame:
    """Keeps the current record of each business key: the first row of `rule.ordering()`.

    ``row_number`` over ``partitionBy(key)`` (nulls in optional key columns form one group).
    ``_row_hash`` (the business content) is added and is the last tie-breaker: rows that still
    tie have the same content. Helper columns are kept for the caller to drop (`silver_columns`).
    """
    with_hash = df.withColumn(_ROW_HASH, row_hash(content_columns(df)))
    window = Window.partitionBy(*rule.key).orderBy(*(o.expression() for o in rule.ordering()))
    return (
        with_hash.withColumn(_ROW_NUMBER, F.row_number().over(window))
        .where(F.col(_ROW_NUMBER) == 1)
        .drop(_ROW_NUMBER)
    )


def silver_columns(df: DataFrame) -> list[str]:
    """Output columns: source columns, ``cnpj``/``cnpj_valido`` when present, bronze metadata
    and ``_row_hash`` when present."""
    helpers = {
        _ROW_NUMBER,
        _KEY_MISSING,
        _CNPJ_INVALID,
        _CNPJ_ZERO,
        _CNPJ_ROOT,
        _CNPJ_DV,
        _SIMPLES_LATEST_EVENT,
    }
    return [c for c in df.columns if c not in helpers and not c.startswith(_RAW_PREFIX)]


def build_silver(
    bronze: DataFrame, table: str, options: SilverOptions = DEFAULT_OPTIONS
) -> DataFrame:
    """Bronze DataFrame -> silver DataFrame (typed, filtered, one current record per key)."""
    rule = get_rule(table)
    current = deduplicate(select_rows(type_and_flag(bronze, table, options), options), rule)
    return current.select(*silver_columns(current))


def duplicate_keys(df: DataFrame, table: str) -> int:
    """Number of business keys that appear more than once (0 in a correct silver table)."""
    return df.groupBy(*get_rule(table).key).count().where(F.col("count") > 1).count()


TARGET = "t"
SOURCE = "s"


def merge_condition(key: Sequence[str]) -> str:
    """MERGE join on the business key, null-safe (``<=>``) like the deduplication groups."""
    return " AND ".join(f"{TARGET}.`{k}` <=> {SOURCE}.`{k}`" for k in key)


def change_condition() -> str:
    """When a matched key is updated: its content changed and the month is not older.

    A re-run of the same month is a no-op (same hash); re-running an older month never
    overwrites a newer current record.
    """
    return (
        f"NOT ({TARGET}.`{_ROW_HASH}` <=> {SOURCE}.`{_ROW_HASH}`)"
        f" AND {SOURCE}.`{PARTITION_COLUMN}` >= {TARGET}.`{PARTITION_COLUMN}`"
    )


def write_metrics(version: int, operation: str, metrics: Mapping[str, str]) -> WriteMetrics:
    """`WriteMetrics` from a Delta commit (``DeltaTable.history``): ``WRITE`` or ``MERGE``."""

    def get(name: str) -> int:
        return int(metrics.get(name) or 0)

    if operation == "MERGE":
        return WriteMetrics(
            "merge",
            version,
            get("numSourceRows"),
            get("numTargetRowsInserted"),
            get("numTargetRowsUpdated"),
        )
    rows = get("numOutputRows")
    return WriteMetrics("create", version, rows, rows, 0)


# --------------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------------


def read_month(spark: SparkSession, path: str, competencia: str, fmt: str = "delta") -> DataFrame:
    return spark.read.format(fmt).load(path).where(F.col(PARTITION_COLUMN) == competencia)


def _last_commit(table: DeltaTable) -> tuple[int, str, dict[str, str]]:
    row = table.history(1).select("version", "operation", "operationMetrics").first()
    assert row is not None
    return int(row["version"]), str(row["operation"]), dict(row["operationMetrics"] or {})


def merge_silver(
    spark: SparkSession, batch: DataFrame, path: str, key: Sequence[str]
) -> WriteMetrics:
    """Loads one month's current records into the silver Delta table (RF-04, ADR 011).

    First run: the table is created (version 0 = that month). Later runs: one ``MERGE`` by
    business key, ``whenMatchedUpdateAll`` under `change_condition` and
    ``whenNotMatchedInsertAll``; keys absent from the month are kept. If Delta skips the
    commit (nothing to write), the metrics are 0 inserted, 0 updated and the batch is counted.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        batch.write.format("delta").mode("errorifexists").save(path)
        return write_metrics(*_last_commit(DeltaTable.forPath(spark, path)))
    target = DeltaTable.forPath(spark, path)
    before = _last_commit(target)[0]
    (
        target.alias(TARGET)
        .merge(batch.alias(SOURCE), merge_condition(key))
        .whenMatchedUpdateAll(condition=change_condition())
        .whenNotMatchedInsertAll()
        .execute()
    )
    version, operation, metrics = _last_commit(DeltaTable.forPath(spark, path))
    if version == before:  # empty commit skipped by Delta
        return WriteMetrics("merge", version, batch.count(), 0, 0)
    return write_metrics(version, operation, metrics)


def build_table(
    spark: SparkSession,
    table: str,
    competencia: str,
    bronze_path: str,
    output_path: str,
    options: SilverOptions = DEFAULT_OPTIONS,
    fmt: str = "delta",
) -> SilverReport:
    """Reads the month from bronze (format `fmt`), merges it into the silver Delta table and
    returns the counts."""
    rule = get_rule(table)
    bronze = read_month(spark, bronze_path, competencia, fmt)
    counts = quality_counts(type_and_flag(bronze, table, options), table, options)
    written = merge_silver(spark, build_silver(bronze, table, options), output_path, rule.key)
    rows_out = written.source_rows
    table_rows = spark.read.format("delta").load(output_path).count()
    report = SilverReport(
        table=table,
        competencia=competencia,
        path=output_path,
        key=rule.key,
        rows_in=counts.rows_in,
        rows_out=rows_out,
        duplicates_removed=counts.rows_deduplicated_in - rows_out,
        rows_without_key=counts.rows_without_key,
        invalid_cnpj=counts.invalid_cnpj,
        zero_cnpj=counts.zero_cnpj,
        invalid_cnpj_policy=options.invalid_cnpj_policy.value,
        invalid_cnpj_dropped=counts.invalid_cnpj_dropped,
        date_values=counts.date_values,
        date_parse_failures=counts.date_parse_failures,
        operation=written.operation,
        table_version=written.version,
        rows_inserted=written.inserted,
        rows_updated=written.updated,
        rows_unchanged=written.unchanged,
        table_rows=table_rows,
    )
    log.info(
        "%s: rows in %d, out %d, duplicates removed %d, without key %d, invalid CNPJ %d"
        " (all-zero %d, policy %s, dropped %d), date parse failures %s; %s v%d: inserted %d,"
        " updated %d, unchanged %d, table rows %d",
        table,
        report.rows_in,
        report.rows_out,
        report.duplicates_removed,
        report.rows_without_key,
        report.invalid_cnpj,
        report.zero_cnpj,
        report.invalid_cnpj_policy,
        report.invalid_cnpj_dropped,
        report.date_parse_failures,
        report.operation,
        report.table_version,
        report.rows_inserted,
        report.rows_updated,
        report.rows_unchanged,
        report.table_rows,
    )
    return report


def _has_rows(spark: SparkSession, path: str, competencia: str, fmt: str) -> bool:
    try:
        return read_month(spark, path, competencia, fmt).limit(1).count() > 0
    except AnalysisException:
        return False


def run(
    spark: SparkSession,
    competencia: str,
    bronze_root: str,
    output_root: str,
    tables: Iterable[str] | None = None,
    options: SilverOptions = DEFAULT_OPTIONS,
    fmt: str = "delta",
) -> list[SilverReport]:
    """Loads one month into silver for the requested tables (default: every table with bronze
    rows that month). `fmt` is the bronze format; silver is always Delta (MERGE, ADR 011).

    A table requested explicitly must have bronze rows for the month, otherwise `ValueError`.
    """
    competencia = parse_competencia(competencia)

    def bronze_path(name: str) -> str:
        return f"{bronze_root.rstrip('/')}/{name}"

    if tables is None:
        names = [n for n in TABLES if _has_rows(spark, bronze_path(n), competencia, fmt)]
    else:
        names = [get_table(n).name for n in tables]
        missing = [n for n in names if not _has_rows(spark, bronze_path(n), competencia, fmt)]
        if missing:
            raise ValueError(
                f"no bronze rows for {competencia} in table(s) {', '.join(missing)};"
                " run `make bronze` first"
            )
    if not names:
        raise ValueError(f"no bronze table has rows for {competencia}; run `make bronze` first")
    log.info("silver %s: %d table(s) %s, options %s", competencia, len(names), names, options)
    return [
        build_table(
            spark,
            name,
            competencia,
            bronze_path(name),
            f"{output_root.rstrip('/')}/{name}",
            options,
            fmt,
        )
        for name in names
    ]


def summary(
    competencia: str, options: SilverOptions, reports: Sequence[SilverReport]
) -> dict[str, Any]:
    return {
        "competencia": competencia,
        "invalid_cnpj_policy": options.invalid_cnpj_policy.value,
        "zero_cnpj_is_invalid": options.zero_cnpj_is_invalid,
        "tables": [asdict(r) for r in reports],
    }


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.silver",
        description=(
            "RF-03/RF-04: merge one reference month into the typed, deduplicated silver layer."
        ),
    )
    parser.add_argument(
        "--competencia", required=True, type=_competencia_arg, help="reference month, YYYY-MM"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=f"tables ({', '.join(TABLES)}); default: every bronze table with rows that month",
    )
    parser.add_argument("--bronze", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/bronze")
    parser.add_argument("--output", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/silver")
    parser.add_argument("--format", default="delta", help="bronze table format (default: delta)")
    parser.add_argument(
        "--invalid-cnpj",
        choices=[p.value for p in InvalidCnpjPolicy],
        default=INVALID_CNPJ_POLICY.value,
        help=f"invalid CNPJ policy, ADR 010 (default: {INVALID_CNPJ_POLICY.value})",
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
    options = SilverOptions(invalid_cnpj_policy=InvalidCnpjPolicy(args.invalid_cnpj))
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark(f"silver-{args.competencia}", cfg, SILVER_CONF)
    try:
        reports = run(
            spark,
            args.competencia,
            args.bronze or cfg.path(BRONZE_LAYER),
            args.output or cfg.path(SILVER_LAYER),
            tables=args.tables,
            options=options,
            fmt=args.format,
        )
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(
            json.dumps(summary(args.competencia, options, reports), indent=2) + "\n",
            encoding="utf-8",
        )
    for r in reports:
        print(
            f"[silver] {r.table}: in {r.rows_in}, out {r.rows_out}, duplicates removed"
            f" {r.duplicates_removed}, without key {r.rows_without_key}, invalid CNPJ"
            f" {r.invalid_cnpj} ({r.invalid_cnpj_policy}), date parse failures"
            f" {sum(r.date_parse_failures.values())}; {r.operation} v{r.table_version}: inserted"
            f" {r.rows_inserted}, updated {r.rows_updated}, unchanged {r.rows_unchanged},"
            f" table rows {r.table_rows}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
