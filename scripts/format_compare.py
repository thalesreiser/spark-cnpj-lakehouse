"""ADR 001 / task 6.2: small CSV x Parquet x Delta measurement on one bronze month.

Copies the month of one bronze table (all strings, ADR 008) to ``s3a://<bucket>/benchmark/formats/
<format>`` in each format and measures the write time, the size and number of files, and the
median of ``--runs`` timings of three queries: a full count, a count by ``uf`` (two columns:
shows column pruning) and the distinct companies with an active establishment (filter + one
more column). CSV is read with the bronze schema (no inference). Only metadata is written
(RST-01): ``formats.json`` and ``formats.md`` in ``--out``.

Usage (inside the ``spark-client`` container, after ``make bronze``)::

    python scripts/format_compare.py --competencia 2026-09 --table estabelecimentos \\
        --out build/benchmark/formats
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse.benchmark import FORMAT_QUERIES, median, render_formats_markdown
from cnpj_lakehouse.bronze import BRONZE_LAYER, INGEST_TS, PARTITION_COLUMN, SOURCE_FILE
from cnpj_lakehouse.schemas import bronze_schema
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

FORMATS = ("csv", "parquet", "delta")


def _queries() -> dict[str, Callable[[DataFrame], Any]]:
    queries: dict[str, Callable[[DataFrame], Any]] = {
        "count": lambda df: df.count(),
        "group_by_uf": lambda df: df.groupBy("uf").count().collect(),
        "active_companies": lambda df: df.where(F.col("situacao_cadastral") == "02")
        .select("cnpj_basico")
        .distinct()
        .count(),
    }
    assert tuple(queries) == FORMAT_QUERIES
    return queries


def _read(spark: SparkSession, fmt: str, path: str, table: str) -> DataFrame:
    if fmt == "csv":
        return spark.read.schema(bronze_schema(table)).option("header", "true").csv(path)
    return spark.read.format(fmt).load(path)


def _content(spark: SparkSession, path: str) -> tuple[int, int]:
    jvm = spark.sparkContext._jvm  # type: ignore[attr-defined]
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    summary = hadoop_path.getFileSystem(conf).getContentSummary(hadoop_path)
    return int(summary.getLength()), int(summary.getFileCount())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--competencia", required=True)
    parser.add_argument("--table", default="estabelecimentos")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--out", type=Path, default=Path("build/benchmark/formats"))
    args = parser.parse_args()

    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("benchmark-formats", cfg)
    results: list[dict[str, Any]] = []
    try:
        month = (
            spark.read.format("delta")
            .load(cfg.path(BRONZE_LAYER, args.table))
            .where(F.col(PARTITION_COLUMN) == args.competencia)
            .drop(PARTITION_COLUMN, INGEST_TS, SOURCE_FILE)
            .cache()
        )
        rows = month.count()
        for fmt in FORMATS:
            path = cfg.path("benchmark", "formats", fmt)
            writer = month.write.format(fmt).mode("overwrite")
            if fmt == "csv":
                writer = writer.option("header", "true")
            start = time.monotonic()
            writer.save(path)
            write_s = round(time.monotonic() - start, 1)
            size, files = _content(spark, path)
            timings: dict[str, float] = {}
            for name, query in _queries().items():
                values = []
                for _ in range(args.runs):
                    start = time.monotonic()
                    query(_read(spark, fmt, path, args.table))
                    values.append(round(time.monotonic() - start, 2))
                timings[name] = median(values)
            results.append(
                {
                    "format": fmt,
                    "write_s": write_s,
                    "size_bytes": size,
                    "num_files": files,
                    "queries": timings,
                }
            )
            print(f"[formats] {results[-1]}", flush=True)
    finally:
        spark.stop()
    source = f"bronze `{args.table}` {args.competencia}"
    args.out.mkdir(parents=True, exist_ok=True)
    payload = {"source": source, "rows": rows, "runs": args.runs, "results": results}
    (args.out / "formats.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    (args.out / "formats.md").write_text(
        render_formats_markdown(source, rows, results), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
