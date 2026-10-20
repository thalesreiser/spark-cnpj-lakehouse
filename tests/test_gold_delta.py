"""RF-05 gold end to end (Delta): synthetic silver -> gold tables, sums reconciled with silver.

Silver ``empresas`` and ``cnaes`` come from the synthetic release (``tests/fixtures/release``,
bronze Parquet -> silver Delta); the release has no establishments file, so silver
``estabelecimentos`` is merged from a few synthetic rows (fixture CNPJ roots, RST-01) with the
real silver functions. Marker ``delta``: own pytest run with the Delta jars (see ``make test``).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import test_silver as base
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse import bronze, gold, silver
from cnpj_lakehouse.gold import CNAE_UF_ANO, DISTRIBUICAO_PORTE, GOLD_TABLES, SITUACAO_SETOR
from cnpj_lakehouse.silver import RULES, build_silver, merge_silver

pytestmark = [
    pytest.mark.delta,
    pytest.mark.skipif(
        sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
        reason="local Spark writes on Windows need HADOOP_HOME/winutils",
    ),
]

FIXTURE_RAW = Path(__file__).parent / "fixtures" / "release"
MONTH = "2026-09"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("test-gold")
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


def establishments() -> list[dict[str, str]]:
    def row(order: str, root: str, **values: str) -> dict[str, str]:
        return base.establishment(order, root, **values)

    return [
        row(
            "0001",
            "80300911",
            cnae_fiscal_principal="4711302",
            uf="SP",
            data_inicio_atividade="20200110",
            situacao_cadastral="02",
        ),
        row(
            "0002",
            "80300911",
            cnae_fiscal_principal="4711302",
            uf="RJ",
            data_inicio_atividade="20210301",
            situacao_cadastral="08",
        ),
        row(
            "0001",
            "80300912",
            cnae_fiscal_principal="5611201",
            uf="SP",
            data_inicio_atividade="20200615",
            situacao_cadastral="02",
        ),
        row("0001", "80300913"),  # no CNAE, UF, date nor status
        {
            **row(
                "0001",
                "80300914",
                cnae_fiscal_principal="0111301",
                uf="MG",
                situacao_cadastral="04",
            ),
            "cnpj_dv": "00",
        },  # invalid check digits
        row(
            "0001",
            "80300919",
            cnae_fiscal_principal="4711302",
            uf="SP",
            data_inicio_atividade="20190101",
            situacao_cadastral="02",
        ),  # no company
    ]


@pytest.fixture(scope="module")
def silver_root(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> str:
    root = tmp_path_factory.mktemp("lake")
    ts = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
    bronze_root = (root / "bronze").as_posix()
    bronze.run(spark, MONTH, FIXTURE_RAW, bronze_root, fmt="parquet", ingest_ts=ts)
    out = (root / "silver").as_posix()
    silver.run(spark, MONTH, bronze_root, out, tables=["empresas", "cnaes"], fmt="parquet")
    rows = build_silver(
        base.bronze_df(spark, "estabelecimentos", establishments()), "estabelecimentos"
    )
    merge_silver(spark, rows, f"{out}/estabelecimentos", RULES["estabelecimentos"].key)
    return out


def test_gold_tables_are_delta_and_reconcile_with_silver(
    spark: SparkSession, silver_root: str, tmp_path: Path
) -> None:
    out = (tmp_path / "gold").as_posix()
    report = gold.run(spark, MONTH, silver_root, out)
    assert report.passed and len(report.checks) == 6 and report.skipped == {}
    assert set(report.tables) == set(GOLD_TABLES)
    for name in GOLD_TABLES:
        assert DeltaTable.isDeltaTable(spark, f"{out}/{name}")
        assert spark.read.format("delta").load(f"{out}/{name}").count() == report.tables[name]

    estab = spark.read.format("delta").load(f"{silver_root}/estabelecimentos")
    empresas = spark.read.format("delta").load(f"{silver_root}/empresas")
    counts = report.silver
    assert counts.estabelecimentos == counts.eligible == estab.count() == 6
    assert counts.invalid_cnpj == 1 and counts.eligible_active == 3
    assert counts.empresas == empresas.count() == 10 and counts.without_company == 1

    def total(name: str, column: str) -> int:
        value = spark.read.format("delta").load(f"{out}/{name}").agg(F.sum(column)).first()
        return int(value[0]) if value else -1

    assert total(CNAE_UF_ANO, "estabelecimentos") == total(SITUACAO_SETOR, "estabelecimentos") == 6
    assert total(DISTRIBUICAO_PORTE, "empresas") == 10
    assert total(DISTRIBUICAO_PORTE, "estabelecimentos") == 5  # minus the one without company
    retail = (
        spark.read.format("delta")
        .load(f"{out}/{CNAE_UF_ANO}")
        .where("cnae_fiscal_principal = '4711302' AND uf = 'SP' AND ano_inicio_atividade = 2020")
        .first()
    )
    assert retail is not None and retail["cnae_descricao"] == "Comércio varejista; supermercados"

    # re-run: whole-table overwrite, same content, one more Delta version
    again = gold.run(spark, MONTH, silver_root, out)
    assert again.tables == report.tables and again.passed
    history = DeltaTable.forPath(spark, f"{out}/{CNAE_UF_ANO}").history().count()
    assert history == 2


def test_excluding_invalid_cnpjs_and_missing_inputs(
    spark: SparkSession, silver_root: str, tmp_path: Path
) -> None:
    strict = gold.run(spark, MONTH, silver_root, (tmp_path / "g1").as_posix(), False)
    assert strict.passed and strict.silver.eligible == 5 and strict.silver.estabelecimentos == 6

    # only empresas in silver (the CI case): the establishment tables are skipped
    only = tmp_path / "silver-empresas"
    spark.read.format("delta").load(f"{silver_root}/empresas").write.format("delta").save(
        (only / "empresas").as_posix()
    )
    porte = gold.run(spark, MONTH, only.as_posix(), (tmp_path / "g2").as_posix())
    assert set(porte.tables) == {DISTRIBUICAO_PORTE} and set(porte.skipped) == {
        CNAE_UF_ANO,
        SITUACAO_SETOR,
    }
    assert porte.passed and [c.name for c in porte.checks] == [
        f"{DISTRIBUICAO_PORTE}.empresas",
        f"{DISTRIBUICAO_PORTE}.participacao",
    ]
    with pytest.raises(ValueError, match="make silver"):
        gold.run(spark, MONTH, (tmp_path / "empty").as_posix(), (tmp_path / "g3").as_posix())
