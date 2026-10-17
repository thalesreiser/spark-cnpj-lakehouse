"""RF-02 proof on CI: source line counts of a release == bronze Delta row counts on MinIO.

Run inside the `spark-client` container after `make bronze` (with SPARK_SAMPLE_FRACTION=1.0).
The source side is counted in plain Python, independently of Spark's CSV reader: non-empty lines
of every extracted CSV listed in ``manifest.json``. Lines with an odd number of quotes are
counted too, since they would reveal a quoted line break (one record over several lines).

For each table, checks that the ``competencia`` partition of ``s3a://<bucket>/bronze/<table>``
has as many rows as the source has lines, that ``_ingest_ts`` and ``_source_file`` are never
null, and that the whole batch shares one ``_ingest_ts``. Writes a JSON and a Markdown summary
(counts only, never data) and exits with a non-zero code on any mismatch.

    python scripts/bronze_check.py --competencia 2026-09 --out build/bronze-check
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from cnpj_lakehouse.bronze import (
    BRONZE_LAYER,
    INGEST_TS,
    PARTITION_COLUMN,
    SOURCE_FILE,
    source_files_by_table,
)
from cnpj_lakehouse.download import MANIFEST_NAME, competencia_dir, load_manifest
from cnpj_lakehouse.spark import LakehouseConfig, get_spark


def count_source_lines(paths: list[Path]) -> tuple[int, int]:
    """Non-empty lines and lines with an odd number of quotes of the given files."""
    lines = odd_quotes = 0
    for path in paths:
        with path.open("rb") as handle:
            for raw in handle:
                if raw.strip():
                    lines += 1
                    odd_quotes += raw.count(b'"') % 2
    return lines, odd_quotes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competencia", required=True)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--tables", nargs="+", default=None)
    parser.add_argument("--out", type=Path, default=Path("build/bronze-check"))
    args = parser.parse_args()

    month_dir = competencia_dir(args.raw_dir, args.competencia)
    files = source_files_by_table(load_manifest(month_dir / MANIFEST_NAME), month_dir, args.tables)
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("bronze-check", cfg)
    results: list[dict[str, Any]] = []
    try:
        for table, paths in files.items():
            source_lines, odd_quote_lines = count_source_lines(paths)
            path = cfg.path(BRONZE_LAYER, table)
            df = spark.read.format("delta").load(path)
            month = df.where(F.col(PARTITION_COLUMN) == args.competencia)
            stats = month.agg(
                F.count(F.lit(1)).alias("rows"),
                F.count(INGEST_TS).alias("ingest_ts_filled"),
                F.count(SOURCE_FILE).alias("source_file_filled"),
                F.countDistinct(INGEST_TS).alias("ingest_ts_distinct"),
                F.countDistinct(SOURCE_FILE).alias("source_files_distinct"),
            ).first()
            assert stats is not None
            versions = DeltaTable.forPath(spark, path).history().count()
            result: dict[str, Any] = {
                "table": table,
                "source_files": [p.name for p in paths],
                "source_lines": source_lines,
                "odd_quote_lines": odd_quote_lines,
                "bronze_rows": int(stats["rows"]),
                "ingest_ts_filled": int(stats["ingest_ts_filled"]),
                "source_file_filled": int(stats["source_file_filled"]),
                "ingest_ts_distinct": int(stats["ingest_ts_distinct"]),
                "source_files_distinct": int(stats["source_files_distinct"]),
                "delta_versions": versions,
                "partitions": sorted(
                    r[PARTITION_COLUMN] for r in df.select(PARTITION_COLUMN).distinct().collect()
                ),
            }
            result["ok"] = (
                result["bronze_rows"] == source_lines > 0
                and result["ingest_ts_filled"] == source_lines
                and result["source_file_filled"] == source_lines
                and result["ingest_ts_distinct"] == 1
                and result["source_files_distinct"] == len(paths)
                and odd_quote_lines == 0
            )
            print(f"[bronze-check] {json.dumps(result)}")
            results.append(result)
    finally:
        spark.stop()

    ok = bool(results) and all(r["ok"] for r in results)
    args.out.mkdir(parents=True, exist_ok=True)
    summary = {"competencia": args.competencia, "ok": ok, "tables": results}
    (args.out / "bronze-check.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        f"### Bronze row counts - competencia {args.competencia}",
        "",
        "| Table | Source lines | Bronze rows | `_ingest_ts` filled | `_source_file` filled"
        " | Delta versions | OK |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    lines += [
        f"| {r['table']} | {r['source_lines']:,} | {r['bronze_rows']:,} | {r['ingest_ts_filled']:,}"
        f" | {r['source_file_filled']:,} | {r['delta_versions']} | {'yes' if r['ok'] else 'NO'} |"
        for r in results
    ]
    (args.out / "bronze-check.md").write_text("\n".join(lines) + "\n")
    print("[bronze-check] OK" if ok else "[bronze-check] FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
