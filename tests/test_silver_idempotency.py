"""RF-04 idempotency (task 4.2, Delta): running a month twice gives the same silver table.

"The same table" is checked with `cnpj_lakehouse.fingerprint.table_fingerprint` (row count and
an order-independent digest of every column except the load metadata) on top of the Delta MERGE
metrics. Same Delta JVM as ``test_silver_merge.py`` (marker ``delta``, own pytest run).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import test_silver as base
import test_silver_merge as merge_tests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test_silver_merge import FIXTURE_RAW, MONTH1, MONTH2, batch, company, merge

from cnpj_lakehouse import bronze, silver
from cnpj_lakehouse.bronze import INGEST_TS
from cnpj_lakehouse.fingerprint import Fingerprint, table_fingerprint

pytestmark = [
    pytest.mark.delta,
    pytest.mark.skipif(
        sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
        reason="local Spark writes on Windows need HADOOP_HOME/winutils",
    ),
]

# the Delta-enabled SparkSession of the MERGE tests (same pytest process, same JVM)
spark = merge_tests.spark
MONTH0 = "2026-08"


def fingerprint(spark: SparkSession, path: str, without: Iterable[str] = ()) -> Fingerprint:
    """Fingerprint of the silver table, optionally without some `cnpj_basico` keys."""
    table = spark.read.format("delta").load(path)
    keys = list(without)
    return table_fingerprint(table.where(~F.col("cnpj_basico").isin(*keys)) if keys else table)


def at(rows: list[dict[str, Any]], ts: datetime) -> list[dict[str, Any]]:
    return [{**row, INGEST_TS: ts} for row in rows]


MONTH1_ROWS = [
    company("10000001", "A"),
    company("10000002", "B"),
    company("10000003", "C"),
    {**company("10000003", "C OLD"), INGEST_TS: datetime(2026, 9, 30)},  # deduplicated away
]


def test_same_month_twice_gives_the_same_table(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "empresas").as_posix()
    first = merge(spark, batch(spark, "empresas", MONTH1_ROWS, MONTH1), path)
    before = fingerprint(spark, path)
    again = merge(spark, batch(spark, "empresas", MONTH1_ROWS, MONTH1), path)
    assert (again.inserted, again.updated, again.unchanged) == (0, 0, 3)
    assert again.version == first.version + 1  # an empty MERGE commit, no rewritten row
    assert fingerprint(spark, path) == before and before.rows == 3
    # the same rows ingested again (new _ingest_ts / _source_file) change nothing either
    reloaded = at(MONTH1_ROWS[:3], base.TS2) + MONTH1_ROWS[3:]
    third = merge(spark, batch(spark, "empresas", reloaded, MONTH1), path)
    assert (third.inserted, third.updated) == (0, 0)
    assert fingerprint(spark, path) == before


def test_next_month_changes_only_the_updated_and_inserted_keys(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = (tmp_path / "empresas").as_posix()
    merge(spark, batch(spark, "empresas", MONTH1_ROWS, MONTH1), path)
    before = fingerprint(spark, path)
    changed, new = ["10000002"], ["10000004", "10000005"]
    untouched = fingerprint(spark, path, changed)
    month2 = at(  # A unchanged, B changed, C absent (kept), D and E new
        [
            company("10000001", "A"),
            company("10000002", "B LTDA"),
            company("10000004", "D"),
            company("10000005", "E"),
        ],
        base.TS2,
    )
    second = merge(spark, batch(spark, "empresas", month2, MONTH2), path)
    assert (second.inserted, second.updated, second.unchanged) == (2, 1, 1)
    after = fingerprint(spark, path)
    assert after.rows == before.rows + 2 and after.digest != before.digest
    assert fingerprint(spark, path, changed + new) == untouched
    # re-running the new month is a no-op too
    again = merge(spark, batch(spark, "empresas", month2, MONTH2), path)
    assert (again.inserted, again.updated) == (0, 0)
    assert fingerprint(spark, path) == after


def test_an_older_month_never_overwrites_newer_rows(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "empresas").as_posix()
    merge(spark, batch(spark, "empresas", MONTH1_ROWS, MONTH1), path)
    month2 = at([company("10000002", "B LTDA"), company("10000003", "C SA")], base.TS2)
    merge(spark, batch(spark, "empresas", month2, MONTH2), path)
    newest = fingerprint(spark, path)
    # month 1 again, then an even older month: old values of B and C, plus a key seen only there
    for month, extra in ((MONTH1, []), (MONTH0, [company("10000009", "OLD ONLY")])):
        rows = at(MONTH1_ROWS[:3] + extra, datetime(2026, 11, 1))
        old = merge(spark, batch(spark, "empresas", rows, month), path)
        assert (old.inserted, old.updated) == (len(extra), 0)
        assert fingerprint(spark, path, ["10000009"]) == newest
    current = base.by_key(spark.read.format("delta").load(path), "cnpj_basico")
    assert current[("10000002",)]["razao_social"] == "B LTDA"
    assert current[("10000009",)]["competencia"] == MONTH0  # first seen in an older month


def test_pipeline_run_twice_on_the_synthetic_release_is_identical(
    spark: SparkSession, tmp_path: Path
) -> None:
    ts = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
    bronze_path, silver_path = (tmp_path / "bronze").as_posix(), (tmp_path / "silver").as_posix()
    bronze.run(spark, MONTH1, FIXTURE_RAW, bronze_path, fmt="parquet", ingest_ts=ts)
    first = silver.run(spark, MONTH1, bronze_path, silver_path, fmt="parquet")
    before = {r.table: fingerprint(spark, f"{silver_path}/{r.table}") for r in first}
    second = silver.run(spark, MONTH1, bronze_path, silver_path, fmt="parquet")
    assert {r.table for r in second} == set(before)
    for r in second:
        assert (r.operation, r.rows_inserted, r.rows_updated) == ("merge", 0, 0)
        after = fingerprint(spark, f"{silver_path}/{r.table}")
        assert after == before[r.table] and after.rows == r.table_rows > 0
