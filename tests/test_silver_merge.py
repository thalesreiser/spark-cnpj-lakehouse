"""RF-04 silver MERGE tests (Delta): create, no-op re-run, update/insert/keep on a new month.

Delta needs its jars on the JVM classpath when the JVM starts, so these tests run in their own
pytest process (marker ``delta``; ``make test`` runs ``-m delta`` after the other tests) with a
Delta-enabled local SparkSession (`delta.configure_spark_with_delta_pip`, jars from Maven).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import test_silver as base
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType

from cnpj_lakehouse import bronze, silver
from cnpj_lakehouse.bronze import INGEST_TS, PARTITION_COLUMN
from cnpj_lakehouse.silver import (
    RULES,
    build_silver,
    date_columns,
    duplicate_keys,
    merge_silver,
)

pytestmark = [
    pytest.mark.delta,
    pytest.mark.skipif(
        sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
        reason="local Spark writes on Windows need HADOOP_HOME/winutils",
    ),
]

FIXTURE_RAW = Path(__file__).parent / "fixtures" / "release"
MONTH1, MONTH2 = "2026-09", "2026-10"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver-merge")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def batch(spark: SparkSession, table: str, rows: list[dict[str, Any]], month: str) -> DataFrame:
    """One month's current records, as `build_table` merges them."""
    month_rows = [{PARTITION_COLUMN: month, **row} for row in rows]
    return build_silver(base.bronze_df(spark, table, month_rows), table)


def company(base_cnpj: str, name: str) -> dict[str, Any]:
    return {"cnpj_basico": base_cnpj, "razao_social": name, "porte_empresa": "01"}


def merge(spark: SparkSession, df: DataFrame, path: str, table: str = "empresas") -> Any:
    return merge_silver(spark, df, path, RULES[table].key)


def test_first_run_creates_the_table_and_a_rerun_changes_nothing(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = (tmp_path / "empresas").as_posix()
    rows = [company("10000001", "A"), company("10000002", "B"), company("10000003", "C")]
    first = merge(spark, batch(spark, "empresas", rows, MONTH1), path)
    assert (first.operation, first.version, first.inserted, first.updated) == ("create", 0, 3, 0)
    again = merge(spark, batch(spark, "empresas", rows, MONTH1), path)
    assert (again.operation, again.inserted, again.updated, again.unchanged) == ("merge", 0, 0, 3)
    table = spark.read.format("delta").load(path)
    assert table.count() == 3 and duplicate_keys(table, "empresas") == 0


def test_new_month_updates_changed_inserts_new_and_keeps_absent_keys(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = (tmp_path / "empresas").as_posix()
    month1 = [company("10000001", "A"), company("10000002", "B"), company("10000003", "C")]
    month2 = [  # A unchanged, B changed, C absent, D new
        {**company("10000001", "A"), INGEST_TS: base.TS2},
        {**company("10000002", "B LTDA"), INGEST_TS: base.TS2},
        {**company("10000004", "D"), INGEST_TS: base.TS2},
    ]
    merge(spark, batch(spark, "empresas", month1, MONTH1), path)
    second = merge(spark, batch(spark, "empresas", month2, MONTH2), path)
    assert (second.inserted, second.updated, second.unchanged, second.source_rows) == (1, 1, 1, 3)

    rows = base.by_key(spark.read.format("delta").load(path), "cnpj_basico")
    assert set(rows) == {("10000001",), ("10000002",), ("10000003",), ("10000004",)}
    assert rows[("10000002",)]["razao_social"] == "B LTDA"
    # competencia / _ingest_ts are those of the last change of each key
    assert rows[("10000001",)][PARTITION_COLUMN] == MONTH1  # unchanged: not rewritten
    assert rows[("10000002",)][PARTITION_COLUMN] == MONTH2
    assert rows[("10000003",)][PARTITION_COLUMN] == MONTH1  # absent from MONTH2: kept
    assert rows[("10000004",)][PARTITION_COLUMN] == MONTH2

    # re-running the older month never overwrites a newer current record
    old = merge(spark, batch(spark, "empresas", month1, MONTH1), path)
    assert (old.inserted, old.updated) == (0, 0)
    current = spark.read.format("delta").load(path)
    assert base.by_key(current, "cnpj_basico")[("10000002",)]["razao_social"] == "B LTDA"
    assert duplicate_keys(current, "empresas") == 0
    # time travel (task 4.3): version 0 is the first month
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    assert base.by_key(v0, "cnpj_basico")[("10000002",)]["razao_social"] == "B"


def test_null_parts_of_a_natural_key_match_on_rerun(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "socios").as_posix()
    rows = [
        {"cnpj_basico": "10000001", "identificador_socio": "2", "nome_socio_razao_social": "X"},
        {"cnpj_basico": "10000001", "identificador_socio": "2", "cpf_cnpj_socio": "***1**"},
    ]
    merge(spark, batch(spark, "socios", rows, MONTH1), path, "socios")
    again = merge(spark, batch(spark, "socios", rows, MONTH2), path, "socios")
    assert (again.inserted, again.updated, again.unchanged) == (0, 0, 2)


# --- end to end on the synthetic release (bronze Parquet -> silver Delta) ---------------------


@pytest.fixture(scope="module")
def lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("lake")
    ts = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
    bronze.run(
        spark, MONTH1, FIXTURE_RAW, (root / "bronze").as_posix(), fmt="parquet", ingest_ts=ts
    )
    return root


def test_run_builds_every_bronze_table_of_the_fixture(spark: SparkSession, lake: Path) -> None:
    reports = silver.run(
        spark, MONTH1, (lake / "bronze").as_posix(), (lake / "silver").as_posix(), fmt="parquet"
    )
    by_table = {r.table: r for r in reports}
    assert set(by_table) == {"empresas", "simples", "cnaes"}
    for r in reports:
        out = spark.read.format("delta").load(f"{lake.as_posix()}/silver/{r.table}")
        assert r.operation == "create" and r.rows_inserted == r.rows_out
        assert out.count() == r.table_rows == r.rows_out > 0
        assert duplicate_keys(out, r.table) == 0
        assert r.rows_in == r.rows_out + r.duplicates_removed + r.rows_without_key
        for name in date_columns(r.table):
            assert isinstance(out.schema[name].dataType, DateType)
            typed = out.where(out[name].isNotNull()).count()
            assert typed == r.date_values[name] - r.date_parse_failures[name]
    assert by_table["simples"].date_values["data_opcao_simples"] > 0


def test_rerun_merges_nothing_and_requested_tables_must_exist(
    spark: SparkSession, lake: Path
) -> None:
    args = (spark, MONTH1, (lake / "bronze").as_posix(), (lake / "silver2").as_posix())
    first = silver.run(*args, tables=["cnaes"], fmt="parquet")[0]
    second = silver.run(*args, tables=["cnaes"], fmt="parquet")[0]
    assert (second.operation, second.rows_inserted, second.rows_updated) == ("merge", 0, 0)
    assert second.rows_unchanged == second.rows_out == first.rows_out
    assert second.table_rows == first.table_rows
    with pytest.raises(ValueError, match="socios"):
        silver.run(*args, tables=["socios"], fmt="parquet")
    with pytest.raises(ValueError, match="no bronze table"):
        silver.run(spark, "2020-01", *args[2:], fmt="parquet")
