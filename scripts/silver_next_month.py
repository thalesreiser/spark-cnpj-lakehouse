"""RF-04 proof on CI: a synthetic next month derived from a real bronze month (no download).

Reads the ``--source`` month of ``s3a://<bucket>/bronze/<table>`` and writes the ``--target``
month of the same bronze table. Rows are split by a deterministic hash of the business key:

* bucket 0 (about 1%): ``--column`` gets a suffix, so silver must **update** those keys;
* bucket 1 (about 1%): copied once more with new keys (digits of the first key column mapped
  to letters, ``0-9 -> A-J``; real keys are numeric), so silver must **insert** them;
* bucket 2 (about 1%): left out of the new month, so silver must **keep** those keys;
* every other row is copied unchanged, so silver must leave it **unchanged**.

The expected numbers of updated and inserted keys are computed with
`cnpj_lakehouse.silver.build_silver` (same key rules) and written to ``--out`` as JSON, to be
compared with the silver run summary. Only counts are written, never data (RST-01).

    python scripts/silver_next_month.py --source 2026-09 --target 2026-10 --table empresas \\
        --column razao_social --out build/next-month.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cnpj_lakehouse.bronze import BRONZE_LAYER, INGEST_TS, PARTITION_COLUMN, write_bronze
from cnpj_lakehouse.silver import build_silver, get_rule
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

BUCKETS = 100
ALTERED, NEW, ABSENT = 0, 1, 2
SUFFIX = " (SYNTHETIC CHANGE)"


def next_month(
    source: DataFrame, table: str, column: str, target: str, ingest_ts: datetime
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Returns (next month rows, rows whose keys are updated, rows with the new keys)."""
    key = get_rule(table).key
    bucket = F.pmod(F.xxhash64(*key), F.lit(BUCKETS))
    tagged = source.withColumn("_bucket", bucket)
    altered = tagged.where(F.col("_bucket") == ALTERED).withColumn(
        column, F.concat(F.coalesce(F.col(column), F.lit("")), F.lit(SUFFIX))
    )
    new = tagged.where(F.col("_bucket") == NEW).withColumn(
        key[0], F.translate(F.col(key[0]), "0123456789", "ABCDEFGHIJ")
    )
    kept = tagged.where(~F.col("_bucket").isin(ALTERED, NEW, ABSENT))
    original_new = tagged.where(F.col("_bucket") == NEW)
    rows = (
        kept.unionByName(altered)
        .unionByName(original_new)
        .unionByName(new)
        .drop("_bucket")
        .withColumn(PARTITION_COLUMN, F.lit(target))
        .withColumn(INGEST_TS, F.lit(ingest_ts).cast("timestamp"))
    )
    return rows, altered.drop("_bucket"), new.drop("_bucket")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="real bronze month, YYYY-MM")
    parser.add_argument("--target", required=True, help="synthetic month to write, YYYY-MM")
    parser.add_argument("--table", default="empresas")
    parser.add_argument("--column", default="razao_social", help="column changed in bucket 0")
    parser.add_argument("--out", type=Path, default=Path("build/next-month.json"))
    args = parser.parse_args()
    if args.target <= args.source:
        parser.error("--target must be a later month than --source")

    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("silver-next-month", cfg)
    path = cfg.path(BRONZE_LAYER, args.table)
    try:
        source = spark.read.format("delta").load(path).where(F.col(PARTITION_COLUMN) == args.source)
        rows, altered, new = next_month(
            source, args.table, args.column, args.target, datetime.now(UTC)
        )
        result = {
            "table": args.table,
            "source": args.source,
            "target": args.target,
            "expected_updated": build_silver(altered, args.table).count(),
            "expected_inserted": build_silver(new, args.table).count(),
        }
        write_bronze(rows, path, args.target)
        result["rows_target"] = (
            spark.read.format("delta").load(path).where(F.col(PARTITION_COLUMN) == args.target)
        ).count()
    finally:
        spark.stop()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"[next-month] {json.dumps(result)}")
    return 0 if result["expected_updated"] > 0 and result["expected_inserted"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
