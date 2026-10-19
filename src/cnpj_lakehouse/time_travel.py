"""RF-06: Delta time travel on a silver table (ADR 011: every silver run is a Delta version).

* `history` is ``DESCRIBE HISTORY``: version, commit timestamp, operation and the MERGE metrics;
* `snapshot` aggregates one version (rows, a content checksum of ``_row_hash`` and the row
  counts per category column, e.g. ``porte_empresa``) read with ``versionAsOf`` or
  ``timestampAsOf``;
* `changed_keys` compares two versions by business key (``_row_hash`` differs, key only in one);
* `demo` puts it together for versions N-1 and N and checks that ``versionAsOf N-1`` reproduces
  the snapshot recorded right after that run (`--expected`) and that ``timestampAsOf`` of the
  N-1 commit resolves to the same state.

Only aggregates and metadata are written, never rows, names or CNPJs (RST-01/LGPD).

    python -m cnpj_lakehouse.time_travel --table empresas --snapshot build/tt-before.json
    python -m cnpj_lakehouse.time_travel --table empresas --expected build/tt-before.json \\
        --out build/time-travel
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse.silver import ROW_HASH, SILVER_LAYER, get_rule
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger(__name__)

DEFAULT_GROUP_BY = ("porte_empresa", "natureza_juridica")
NULL_LABEL = "(null)"
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"  # session time zone, both ways
TOP = 10  # categories shown per column in the Markdown report


def history(spark: SparkSession, path: str) -> list[dict[str, Any]]:
    """``DESCRIBE HISTORY``, oldest version first; ``timestamp`` is usable as ``timestampAsOf``."""
    rows = (
        spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
        .select(
            "version",
            F.date_format("timestamp", TS_FORMAT).alias("timestamp"),
            "operation",
            "operationMetrics",
        )
        .orderBy("version")
        .collect()
    )

    def metric(metrics: dict[str, str], name: str) -> int | None:
        value = metrics.get(name)
        return int(value) if value is not None else None

    out = []
    for row in rows:
        metrics = dict(row["operationMetrics"] or {})
        out.append(
            {
                "version": int(row["version"]),
                "timestamp": row["timestamp"],
                "operation": row["operation"],
                "source_rows": metric(metrics, "numSourceRows"),
                "output_rows": metric(metrics, "numOutputRows"),
                "inserted": metric(metrics, "numTargetRowsInserted"),
                "updated": metric(metrics, "numTargetRowsUpdated"),
            }
        )
    return out


def read_version(spark: SparkSession, path: str, version: int) -> DataFrame:
    return spark.read.format("delta").option("versionAsOf", version).load(path)


def read_timestamp(spark: SparkSession, path: str, timestamp: str) -> DataFrame:
    return spark.read.format("delta").option("timestampAsOf", timestamp).load(path)


def snapshot(df: DataFrame, group_by: Sequence[str] = DEFAULT_GROUP_BY) -> dict[str, Any]:
    """Aggregates of one version: rows, ``_row_hash`` checksum, row counts per category."""
    total = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(ROW_HASH).cast("decimal(38,0)")).alias("checksum"),
    ).first()
    assert total is not None
    by: dict[str, dict[str, int]] = {}
    for name in (c for c in group_by if c in df.columns):
        counts = df.groupBy(name).count().collect()
        by[name] = {
            (NULL_LABEL if r[name] is None else str(r[name])): int(r["count"])
            for r in sorted(counts, key=lambda r: (r[name] is None, str(r[name])))
        }
    return {"rows": int(total["rows"]), "checksum": str(total["checksum"] or 0), "by": by}


def changed_keys(old: DataFrame, new: DataFrame, key: Sequence[str]) -> dict[str, int]:
    """Keys whose ``_row_hash`` differs between two versions, and keys present in only one."""
    a, b = old.select(*key, ROW_HASH).alias("a"), new.select(*key, ROW_HASH).alias("b")
    cond = reduce(lambda x, y: x & y, [F.col(f"a.{k}").eqNullSafe(F.col(f"b.{k}")) for k in key])
    joined = a.join(b, cond, "full_outer")
    in_a, in_b = F.col(f"a.{ROW_HASH}").isNotNull(), F.col(f"b.{ROW_HASH}").isNotNull()
    row = joined.agg(
        F.sum((in_a & in_b & (F.col(f"a.{ROW_HASH}") != F.col(f"b.{ROW_HASH}"))).cast("int")).alias(
            "changed"
        ),
        F.sum((~in_a & in_b).cast("int")).alias("only_new"),
        F.sum((in_a & ~in_b).cast("int")).alias("only_old"),
    ).first()
    assert row is not None
    return {name: int(row[name] or 0) for name in ("changed", "only_new", "only_old")}


def demo(
    spark: SparkSession,
    path: str,
    table: str,
    expected: dict[str, Any] | None = None,
    group_by: Sequence[str] = DEFAULT_GROUP_BY,
) -> dict[str, Any]:
    """History, versions N-1 and N, their differences and the reproducibility checks.

    ``expected`` is a `snapshot` recorded right after an earlier run (``{"version": v, ...}``);
    when given, version N-1 is that version and ``versionAsOf`` must reproduce it exactly.
    """
    commits = history(spark, path)
    if len(commits) < 2:
        raise ValueError(f"{path} has {len(commits)} version(s); time travel needs at least 2")
    latest = commits[-1]["version"]
    previous = int(expected["version"]) if expected else commits[-2]["version"]
    if previous >= latest:
        raise ValueError(f"version N-1 ({previous}) must be older than the latest ({latest})")
    old, new = read_version(spark, path, previous), read_version(spark, path, latest)
    old_snap, new_snap = snapshot(old, group_by), snapshot(new, group_by)
    commit_ts = next(c["timestamp"] for c in commits if c["version"] == previous)
    ts_snap = snapshot(read_timestamp(spark, path, commit_ts), group_by)
    checks = {"timestamp_as_of_equals_version_as_of": ts_snap == old_snap}
    if expected:
        recorded = {k: expected[k] for k in ("rows", "checksum", "by")}
        checks["version_as_of_reproduces_recorded"] = old_snap == recorded
    return {
        "table": table,
        "history": commits,
        "previous": {"version": previous, "timestamp": commit_ts, **old_snap},
        "latest": {"version": latest, "timestamp": commits[-1]["timestamp"], **new_snap},
        "changes": changed_keys(old, new, get_rule(table).key),
        "checks": checks,
        "ok": all(checks.values()),
    }


def to_markdown(report: dict[str, Any]) -> str:
    """Markdown summary of `demo` (aggregates and metadata only)."""
    prev, last, ch = report["previous"], report["latest"], report["changes"]
    lines = [
        f"### Time travel (RF-06): silver `{report['table']}`",
        "",
        "`DESCRIBE HISTORY`:",
        "",
        "| version | timestamp | operation | source rows | inserted | updated |",
        "|---:|---|---|---:|---:|---:|",
    ]
    for c in report["history"]:
        source = c["source_rows"] if c["source_rows"] is not None else c["output_rows"]
        inserted = c["inserted"] if c["inserted"] is not None else c["output_rows"]
        cells = [c["version"], c["timestamp"], c["operation"], source, inserted, c["updated"]]
        lines.append("| " + " | ".join("" if v is None else str(v) for v in cells) + " |")
    lines += [
        "",
        f"N-1 = `versionAsOf {prev['version']}` (= `timestampAsOf '{prev['timestamp']}'`),"
        f" N = version {last['version']}.",
        "",
        "| | N-1 | N | N - (N-1) |",
        "|---|---:|---:|---:|",
        f"| rows | {prev['rows']} | {last['rows']} | {last['rows'] - prev['rows']} |",
    ]
    for name, counts in last["by"].items():
        old = prev["by"].get(name, {})
        top = sorted(counts, key=lambda v: -counts[v])[:TOP]
        for value in top:
            n, o = counts[value], old.get(value, 0)
            lines.append(f"| {name} = {value} | {o} | {n} | {n - o} |")
    lines += [
        "",
        f"Keys whose `_row_hash` changed: {ch['changed']}; keys only in N: {ch['only_new']};"
        f" keys only in N-1: {ch['only_old']}.",
        "",
    ]
    for name, ok in report["checks"].items():
        lines.append(f"- {'PASS' if ok else 'FAIL'}: {name.replace('_', ' ')}")
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m cnpj_lakehouse.time_travel")
    parser.add_argument("--table", default="empresas")
    parser.add_argument("--path", default=None, help="Delta table (default: silver/<table>)")
    parser.add_argument("--snapshot", type=Path, default=None, help="only record the current one")
    parser.add_argument("--expected", type=Path, default=None, help="snapshot recorded earlier")
    parser.add_argument("--out", type=Path, default=Path("build/time-travel"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None, spark: SparkSession | None = None) -> int:
    """CLI entry point; returns the process exit code (1 when a check fails)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    cfg = LakehouseConfig.from_env(os.environ)
    path = args.path or cfg.path(SILVER_LAYER, args.table)
    session = spark or get_spark(f"time-travel-{args.table}", cfg)
    try:
        if args.snapshot:
            version = history(session, path)[-1]["version"]
            snap = {"version": version, **snapshot(read_version(session, path, version))}
            args.snapshot.parent.mkdir(parents=True, exist_ok=True)
            args.snapshot.write_text(json.dumps(snap, indent=2) + "\n", encoding="utf-8")
            print(f"[time-travel] {args.table}: recorded version {version}, rows {snap['rows']}")
            return 0
        expected = json.loads(args.expected.read_text(encoding="utf-8")) if args.expected else None
        report = demo(session, path, args.table, expected)
    finally:
        if spark is None:
            session.stop()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "time-travel.json").write_text(json.dumps(report, indent=2) + "\n", "utf-8")
    markdown = to_markdown(report)
    (args.out / "time-travel.md").write_text(markdown, encoding="utf-8")
    print(markdown)
    return 0 if report["ok"] else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
