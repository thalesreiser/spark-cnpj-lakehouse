"""RF-05 gold tests: each aggregation's numbers, null groups, broadcast hints and sum checks.

The aggregations are pure functions, tested on tiny in-memory DataFrames shaped like the silver
tables (no file I/O, so also on Windows). The Delta reads and writes of `gold.run` live in
``tests/test_gold_delta.py`` (marker ``delta``).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType

from cnpj_lakehouse import gold
from cnpj_lakehouse.gold import (
    CNAE_SECOES,
    CNAE_UF_ANO,
    DISTRIBUICAO_PORTE,
    SITUACAO_SETOR,
    Check,
    GoldReport,
    SilverCounts,
    cnae_divisions,
    distribuicao_porte,
    eligible_establishments,
    empresas_cnae_uf_ano,
    reconcile,
    situacao_setor,
)

ESTAB_SCHEMA = StructType(
    [
        StructField("cnpj_basico", StringType()),
        StructField("cnae_fiscal_principal", StringType()),
        StructField("uf", StringType()),
        StructField("data_inicio_atividade", DateType()),
        StructField("situacao_cadastral", StringType()),
        StructField("cnpj_valido", BooleanType()),
        StructField("nome_fantasia", StringType()),  # an unused silver column
    ]
)
ESTABS = [
    ("80300911", "4711302", "SP", date(2020, 1, 10), "02", True, "x"),
    ("80300911", "4711302", "SP", date(2020, 5, 1), "08", True, "x"),  # same company and group
    ("80300912", "5611201", "RJ", date(2021, 3, 3), "02", True, "x"),
    ("80300913", None, None, None, None, True, "x"),  # every category null
    ("80300914", "0111301", "SP", date(2020, 2, 2), "04", False, "x"),  # invalid CNPJ
    ("80300919", "9900800", "MG", date(2019, 7, 7), "02", True, "x"),  # no company; not in cnaes
]
CNAES = [
    ("0111301", "Cultivo de arroz"),
    ("4711302", "Comércio varejista"),
    ("5611201", "Restaurantes"),
]
EMPRESAS = [
    ("80300911", "01"),
    ("80300912", "01"),
    ("80300913", "03"),
    ("80300914", None),
    ("80300915", "05"),
    ("80300916", "99"),  # code out of the documented domain
]


@pytest.fixture(scope="module")
def frames(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "estabelecimentos": spark.createDataFrame(ESTABS, ESTAB_SCHEMA),
        "cnaes": spark.createDataFrame(CNAES, "codigo string, descricao string"),
        "empresas": spark.createDataFrame(EMPRESAS, "cnpj_basico string, porte_empresa string"),
    }


def rows(df: DataFrame, *key: str) -> dict[tuple[Any, ...], dict[str, Any]]:
    return {tuple(r[k] for k in key): r.asDict() for r in df.collect()}


def plans(df: DataFrame) -> tuple[str, str]:
    execution = df._jdf.queryExecution()
    return execution.optimizedPlan().toString(), execution.executedPlan().toString()


def test_eligible_keeps_invalid_cnpjs_by_default_and_projects_the_needed_columns(
    frames: dict[str, DataFrame],
) -> None:
    all_rows = eligible_establishments(frames["estabelecimentos"])
    assert all_rows.count() == len(ESTABS)
    assert "nome_fantasia" not in all_rows.columns and "cnpj_valido" not in all_rows.columns
    valid = eligible_establishments(frames["estabelecimentos"], include_invalid_cnpj=False)
    assert valid.count() == len(ESTABS) - 1


def test_cnae_uf_ano_counts_establishments_active_and_distinct_companies(
    frames: dict[str, DataFrame],
) -> None:
    df = empresas_cnae_uf_ano(eligible_establishments(frames["estabelecimentos"]), frames["cnaes"])
    got = rows(df, "cnae_fiscal_principal", "uf", "ano_inicio_atividade")
    assert len(got) == 5
    retail = got[("4711302", "SP", 2020)]
    assert retail["cnae_descricao"] == "Comércio varejista"
    assert (retail["estabelecimentos"], retail["estabelecimentos_ativos"], retail["empresas"]) == (
        2,
        1,
        1,
    )
    unknown = got[(None, None, None)]  # null codes and date: their own group, not dropped
    assert unknown["cnae_descricao"] is None and unknown["estabelecimentos"] == 1
    assert got[("9900800", "MG", 2019)]["cnae_descricao"] is None  # not in the lookup
    total = df.groupBy().sum("estabelecimentos", "estabelecimentos_ativos").first()
    assert total is not None and tuple(total) == (len(ESTABS), 3)


def test_lookups_are_broadcast(frames: dict[str, DataFrame]) -> None:
    eligible = eligible_establishments(frames["estabelecimentos"])
    for df in (
        empresas_cnae_uf_ano(eligible, frames["cnaes"]),
        situacao_setor(eligible),
        distribuicao_porte(frames["empresas"], eligible),
    ):
        optimized, executed = plans(df)
        assert "strategy=broadcast" in optimized  # the hint reached the join
        assert "BroadcastHashJoin" in executed


def test_distribuicao_porte_counts_companies_share_and_their_establishments(
    frames: dict[str, DataFrame],
) -> None:
    eligible = eligible_establishments(frames["estabelecimentos"])
    got = rows(distribuicao_porte(frames["empresas"], eligible), "porte_empresa")
    assert set(got) == {("01",), ("03",), ("05",), ("99",), (None,)}
    micro = got[("01",)]
    assert micro["porte_descricao"] == "MICRO EMPRESA"
    assert (micro["empresas"], micro["estabelecimentos"], micro["estabelecimentos_ativos"]) == (
        2,
        3,
        2,
    )
    assert micro["empresas_ativas"] == 2 and micro["participacao"] == pytest.approx(2 / 6)
    assert got[("05",)]["estabelecimentos"] == 0  # a company without establishments
    assert got[("99",)]["porte_descricao"] is None and got[(None,)]["porte_descricao"] is None
    assert sum(r["participacao"] for r in got.values()) == pytest.approx(1.0)
    # the establishment of a company missing from silver empresas is not counted here
    assert sum(r["estabelecimentos"] for r in got.values()) == len(ESTABS) - 1


def test_distribuicao_porte_without_establishments_has_null_measures(
    frames: dict[str, DataFrame],
) -> None:
    got = rows(distribuicao_porte(frames["empresas"], None), "porte_empresa")
    assert sum(r["empresas"] for r in got.values()) == len(EMPRESAS)
    assert all(r["estabelecimentos"] is None for r in got.values())


def test_situacao_setor_maps_divisions_to_sections(frames: dict[str, DataFrame]) -> None:
    df = situacao_setor(eligible_establishments(frames["estabelecimentos"]))
    got = rows(df, "situacao_cadastral", "cnae_divisao")
    assert got[("02", "47")]["cnae_secao"] == "G" and got[("02", "47")]["situacao_descricao"] == (
        "ATIVA"
    )
    assert got[("08", "47")]["situacao_descricao"] == "BAIXADA"
    assert got[("04", "01")]["cnae_secao"] == "A"
    assert got[("02", "99")]["cnae_secao"] == "U"
    assert got[(None, None)]["cnae_secao"] is None and got[(None, None)]["estabelecimentos"] == 1
    assert sum(r["estabelecimentos"] for r in got.values()) == len(ESTABS)


def test_every_division_belongs_to_one_section(spark: SparkSession) -> None:
    divisions = cnae_divisions(spark)
    assert divisions.count() == divisions.select("cnae_divisao").distinct().count() == 87
    assert {s[0] for s in CNAE_SECOES} == set("ABCDEFGHIJKLMNOPQRSTU")


def test_reconcile_passes_on_consistent_counts_and_fails_otherwise(
    frames: dict[str, DataFrame],
) -> None:
    eligible = eligible_establishments(frames["estabelecimentos"])
    gold_frames = {
        CNAE_UF_ANO: empresas_cnae_uf_ano(eligible, frames["cnaes"]),
        SITUACAO_SETOR: situacao_setor(eligible),
        DISTRIBUICAO_PORTE: distribuicao_porte(frames["empresas"], eligible),
    }
    counts = SilverCounts(
        empresas=len(EMPRESAS), eligible=len(ESTABS), eligible_active=3, without_company=1
    )
    checks = reconcile(gold_frames, counts)
    assert len(checks) == 6 and all(c.passed for c in checks), checks
    wrong = reconcile(gold_frames, SilverCounts(empresas=len(EMPRESAS) + 1))
    assert [c.passed for c in wrong] == [False, True]  # empresas wrong, share still 1
    assert reconcile(gold_frames, SilverCounts()) == []


def report(passed: bool = True) -> GoldReport:
    return GoldReport(
        "2026-09",
        True,
        {DISTRIBUICAO_PORTE: 5},
        {DISTRIBUICAO_PORTE: "s3a://b/gold/gold_distribuicao_porte"},
        {CNAE_UF_ANO: "no silver estabelecimentos"},
        SilverCounts(empresas=1_234_567),
        (Check("x.empresas", 1_234_567, 1_234_567 if passed else 1), Check("x.share", 1, 1.0)),
    )


class _FakeSpark:
    stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_main_writes_the_summary_and_fails_on_a_failed_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeSpark()
    seen: dict[str, Any] = {}

    def fake_run(spark: Any, competencia: str, silver_root: str, gold_root: str, **kw: Any) -> Any:
        seen.update(silver=silver_root, gold=gold_root, **kw)
        return report()

    monkeypatch.setattr(gold, "get_spark", lambda *a, **k: fake)
    monkeypatch.setattr(gold, "run", fake_run)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "b")
    out = tmp_path / "gold.json"
    assert gold.main(["--competencia", "2026-09", "--summary", str(out)]) == 0 and fake.stopped
    assert seen == {
        "silver": "s3a://b/silver",
        "gold": "s3a://b/gold",
        "include_invalid_cnpj": True,
    }
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["passed"] and data["tables"][0]["rows"] == 5
    assert data["skipped"] == {CNAE_UF_ANO: "no silver estabelecimentos"}
    markdown = out.with_suffix(".md").read_text(encoding="utf-8")
    assert "| x.empresas | 1,234,567 | 1,234,567 | pass |" in markdown

    monkeypatch.setattr(gold, "run", lambda *a, **k: report(passed=False))
    assert gold.main(["--competencia", "2026-09", "--exclude-invalid-cnpj"]) == 1
    assert "**FAIL**" in report(passed=False).to_markdown()


def test_main_returns_1_without_silver(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*a: Any, **k: Any) -> None:
        raise ValueError("no silver empresas nor estabelecimentos")

    monkeypatch.setattr(gold, "get_spark", lambda *a, **k: _FakeSpark())
    monkeypatch.setattr(gold, "run", fail)
    assert gold.main(["--competencia", "2026-09"]) == 1
