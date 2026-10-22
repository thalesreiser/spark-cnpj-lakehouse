"""RF-07 (task 6.3): Delta table maintenance, `OPTIMIZE` (compaction / Z-order) and `VACUUM`.

* **OPTIMIZE** compacts the small files of a table into files of about
  ``TARGET_FILE_BYTES`` (128 MiB, [ADR 004](../../docs/adr/004-target-file-size.md)) with
  ``DeltaTable.optimize().executeCompaction()``, or clusters them with ``executeZOrderBy`` when
  Z-order columns are given. It rewrites files only (``dataChange = false``): the rows, the
  table content and the earlier versions stay the same.
* **VACUUM** deletes the data files that no version inside the retention window references.
  The retention is at least ``MIN_RETENTION_HOURS`` (7 days, Delta's own default) so
  ``versionAsOf`` / ``timestampAsOf`` keep working for a week of loads
  ([ADR 013](../../docs/adr/013-vacuum-retention-and-time-travel.md)); a shorter one is refused
  here, and Delta's retention check is never disabled.

Both return a summary of metadata only (files, bytes, partitions; never rows, RST-01). CLI::

    python -m cnpj_lakehouse.maintenance optimize --layer silver --tables empresas estabelecimentos
    python -m cnpj_lakehouse.maintenance optimize --layer bronze --where "competencia = '2026-10'"
    python -m cnpj_lakehouse.maintenance vacuum --layer silver --retention-hours 168
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from pyspark.sql import SparkSession

from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger(__name__)

MIB = 1024 * 1024
TARGET_FILE_BYTES = 128 * MIB  # ADR 004
MIN_RETENTION_HOURS = 168  # ADR 013: 7 days, Delta's default `deletedFileRetentionDuration`
DEFAULT_TABLES = ("empresas", "estabelecimentos")  # the large tables (lookups are 1 file each)
# Z-order columns per "<layer>/<table>"; empty: compaction only (ADR 013, measured in
# docs/benchmark.md "OPTIMIZE and VACUUM").
ZORDER_BY: Mapping[str, tuple[str, ...]] = {}


def table_files(spark: SparkSession, path: str) -> dict[str, Any]:
    """Live files of a Delta table: count, bytes and files per partition value (no data read)."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    if detail is None:  # pragma: no cover (DESCRIBE DETAIL always returns one row)
        raise ValueError(f"no Delta table at {path}")
    columns = list(detail["partitionColumns"] or [])
    partitions: Counter[str] = Counter()
    if columns:
        for file in spark.read.format("delta").load(path).inputFiles():
            parts = [unquote(p) for p in file.split("/") if "=" in p]
            partitions["/".join(p for p in parts if p.split("=", 1)[0] in columns)] += 1
    num_files = int(detail["numFiles"] or 0)
    size = int(detail["sizeInBytes"] or 0)
    counts = sorted(partitions.values()) or [num_files]
    return {
        "num_files": num_files,
        "size_bytes": size,
        "avg_file_bytes": round(size / num_files) if num_files else 0,
        "partitions": dict(sorted(partitions.items())),
        "files_per_partition_min": counts[0],
        "files_per_partition_max": counts[-1],
    }


def storage_files(spark: SparkSession, path: str) -> dict[str, int]:
    """Data files on storage (live + kept for time travel), outside ``_delta_log``."""
    jvm = spark.sparkContext._jvm
    assert jvm is not None
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    files = size = 0
    iterator = fs.listFiles(hadoop_path, True)
    while iterator.hasNext():
        status = iterator.next()
        name = status.getPath().toString()
        if "/_delta_log/" in name or not name.endswith(".parquet"):
            continue
        files += 1
        size += int(status.getLen())
    return {"files": files, "bytes": size}


def optimize(
    spark: SparkSession,
    path: str,
    zorder: Sequence[str] = (),
    where: str | None = None,
    max_file_bytes: int = TARGET_FILE_BYTES,
) -> dict[str, Any]:
    """Compacts (or Z-orders) one Delta table; returns files before/after and Delta's metrics.

    Files of at least ``max_file_bytes`` are left alone (``minFileSize`` = ``maxFileSize``), so a
    second run on a compacted table rewrites nothing. Z-order rewrites every file in scope.
    """
    from delta.tables import DeltaTable

    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(max_file_bytes))
    spark.conf.set("spark.databricks.delta.optimize.minFileSize", str(max_file_bytes))
    before = table_files(spark, path)
    builder = DeltaTable.forPath(spark, path).optimize()
    if where:
        builder = builder.where(where)
    start = time.monotonic()
    result = builder.executeZOrderBy(*zorder) if zorder else builder.executeCompaction()
    row = result.first()
    seconds = round(time.monotonic() - start, 1)
    metrics = row["metrics"].asDict(recursive=True) if row is not None else {}
    after = table_files(spark, path)
    summary = {
        "path": path,
        "operation": f"ZORDER BY ({', '.join(zorder)})" if zorder else "compaction",
        "where": where,
        "max_file_bytes": max_file_bytes,
        "seconds": seconds,
        "files_removed": int(metrics.get("numFilesRemoved") or 0),
        "files_added": int(metrics.get("numFilesAdded") or 0),
        "bytes_removed": int((metrics.get("filesRemoved") or {}).get("totalSize") or 0),
        "bytes_added": int((metrics.get("filesAdded") or {}).get("totalSize") or 0),
        "partitions_optimized": int(metrics.get("partitionsOptimized") or 0),
        "before": before,
        "after": after,
    }
    log.info("optimize %s: %s", path, {k: summary[k] for k in ("files_removed", "files_added")})
    return summary


def vacuum(
    spark: SparkSession, path: str, retention_hours: float = MIN_RETENTION_HOURS
) -> dict[str, Any]:
    """Deletes the files no version younger than ``retention_hours`` needs (ADR 013).

    Raises ``ValueError`` below ``MIN_RETENTION_HOURS``: a shorter retention would break time
    travel to the versions of the last week, and the Delta safety check stays on.
    """
    from delta.tables import DeltaTable

    if retention_hours < MIN_RETENTION_HOURS:
        raise ValueError(
            f"VACUUM retention {retention_hours} h is below the {MIN_RETENTION_HOURS} h minimum "
            "that keeps time travel (ADR 013)"
        )
    before = storage_files(spark, path)
    start = time.monotonic()
    DeltaTable.forPath(spark, path).vacuum(float(retention_hours))
    seconds = round(time.monotonic() - start, 1)
    after = storage_files(spark, path)
    live = table_files(spark, path)
    summary = {
        "path": path,
        "retention_hours": retention_hours,
        "seconds": seconds,
        "files_deleted": before["files"] - after["files"],
        "bytes_deleted": before["bytes"] - after["bytes"],
        "storage_before": before,
        "storage_after": after,
        "live_files": live["num_files"],
        "live_bytes": live["size_bytes"],
    }
    log.info("vacuum %s: %s files deleted", path, summary["files_deleted"])
    return summary


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m cnpj_lakehouse.maintenance")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("optimize", "vacuum"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--layer", default="silver", help="bronze, silver or gold")
        cmd.add_argument("--tables", nargs="+", default=list(DEFAULT_TABLES))
        cmd.add_argument(
            "--base", default=None, help="tables root (default s3a://<bucket>/<layer>)"
        )
        cmd.add_argument("--summary", type=Path, default=None, help="JSON summary to write")
    optimize_cmd = sub.choices["optimize"]
    optimize_cmd.add_argument("--where", default=None, help="partition predicate (bronze month)")
    optimize_cmd.add_argument(
        "--zorder", nargs="*", default=None, help="Z-order columns (default: ZORDER_BY per table)"
    )
    optimize_cmd.add_argument("--max-file-mib", type=int, default=TARGET_FILE_BYTES // MIB)
    sub.choices["vacuum"].add_argument(
        "--retention-hours", type=float, default=float(MIN_RETENTION_HOURS)
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None, spark: SparkSession | None = None) -> int:
    """CLI entry point: runs OPTIMIZE or VACUUM on each table and writes the JSON summary."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    cfg = LakehouseConfig.from_env(os.environ)
    base = (args.base or cfg.path(args.layer)).rstrip("/")
    session = spark or get_spark(f"maintenance-{args.command}-{args.layer}", cfg)
    results: list[dict[str, Any]] = []
    try:
        for table in args.tables:
            path = f"{base}/{table}"
            if args.command == "optimize":
                zorder = args.zorder
                if zorder is None:
                    zorder = ZORDER_BY.get(f"{args.layer}/{table}", ())
                result = optimize(
                    session, path, zorder, args.where, max_file_bytes=args.max_file_mib * MIB
                )
            else:
                result = vacuum(session, path, args.retention_hours)
            results.append({"table": table, **result})
    finally:
        if spark is None:
            session.stop()
    payload = {"command": args.command, "layer": args.layer, "tables": results}
    text = json.dumps(payload, indent=2) + "\n"
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
