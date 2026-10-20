"""RF-05 / task 5.2: silver x gold reconciliation on tiny in-memory data with known totals.

The silver frames are those of ``tests/test_gold.py`` (synthetic CNPJ roots of the verified
block, RST-01): 6 establishments (one invalid CNPJ, one with every category null, one without
company) and 6 companies (one null size, one size out of the domain). Gold is built with the
real gold functions and then deliberately broken to prove that each check fails. The Delta
end-to-end run (`reconcile.run` on gold written by `gold.run`) is in ``tests/test_gold_delta.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import test_gold as base
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse import reconcile
from cnpj_lakehouse.gold import (
    CNAE_UF_ANO,
    DISTRIBUICAO_PORTE,
    SITUACAO_SETOR,
    distribuicao_porte,
    eligible_establishments,
    empresas_cnae_uf_ano,
    situacao_setor,
)
from cnpj_lakehouse.reconcile import (
    GRAIN,
    TOTAL,
    Comparison,
    Group,
    ReconcileReport,
    compare,
    reconcile_frames,
)

N_ESTABS = len(base.ESTABS)  # 6
N_EMPRESAS = len(base.EMPRESAS)  # 6


@pytest.fixture(scope="module")
def silver(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "estabelecimentos": spark.createDataFrame(base.ESTABS, base.ESTAB_SCHEMA),
        "cnaes": spark.createDataFrame(base.CNAES, "codigo string, descricao string"),
        "empresas": spark.createDataFrame(
            base.EMPRESAS, "cnpj_basico string, porte_empresa string"
        ),
    }


def build_gold(silver: dict[str, DataFrame], include_invalid_cnpj: bool = True) -> dict[str, Any]:
    eligible = eligible_establishments(silver["estabelecimentos"], include_invalid_cnpj)
    return {
        CNAE_UF_ANO: empresas_cnae_uf_ano(eligible, silver["cnaes"]),
        SITUACAO_SETOR: situacao_setor(eligible),
        DISTRIBUICAO_PORTE: distribuicao_porte(silver["empresas"], eligible),
    }


def check(
    silver: dict[str, DataFrame], gold: dict[str, DataFrame], include_invalid_cnpj: bool = True
) -> dict[tuple[str, str, str], Comparison]:
    comparisons, skipped, errors = reconcile_frames(
        gold, silver["empresas"], silver["estabelecimentos"], include_invalid_cnpj
    )
    assert skipped == {} and errors == []
    return {(c.table, c.level, c.measure): c for c in comparisons}


def failed(comparisons: dict[tuple[str, str, str], Comparison]) -> set[tuple[str, str, str]]:
    return {k for k, c in comparisons.items() if not c.passed}


def test_compare_matches_null_keys_and_counts_missing_and_duplicated_groups(
    spark: SparkSession,
) -> None:
    expected = spark.createDataFrame(
        [("SP", 2), ("SP", 1), (None, 1), ("RJ", 4)], "uf string, n long"
    )
    same = spark.createDataFrame([("SP", 3), (None, 1), ("RJ", 4)], "uf string, n long")
    ok = compare("t", "uf", ["uf"], "n", expected, same)
    assert ok.passed and ok.groups == 3 and ok.expected_total == ok.actual_total == 8
    assert [g.key for g in ok.listed] == [(None,), ("RJ",), ("SP",)]  # null group matched

    # RJ missing in gold, MG only in gold, SP duplicated
    broken = spark.createDataFrame(
        [("SP", 3), ("SP", 3), (None, 1), ("MG", 4)], "uf string, n long"
    )
    bad = compare("t", "uf", ["uf"], "n", expected, broken)
    assert bad.mismatches == 3 and bad.groups == 4
    assert {g.key: g.passed for g in bad.listed} == {
        (None,): True,
        ("MG",): False,
        ("RJ",): False,
        ("SP",): False,
    }
    total = compare("t", TOTAL, [], "n", expected, same)
    assert total.passed and total.groups == 1 and total.listed == (Group((), 8.0, 8.0),)
    grain = compare("t", GRAIN, ["uf"], "n", expected, broken, additive=False, list_all=False)
    assert grain.expected_total is None and len(grain.listed) == 3  # mismatches only


def test_consistent_gold_reconciles_by_total_uf_status_and_size(
    silver: dict[str, DataFrame],
) -> None:
    comparisons = check(silver, build_gold(silver))
    assert failed(comparisons) == set()
    assert {(t, level) for t, level, _ in comparisons} == {
        (CNAE_UF_ANO, TOTAL),
        (CNAE_UF_ANO, "uf"),
        (CNAE_UF_ANO, GRAIN),
        (SITUACAO_SETOR, TOTAL),
        (SITUACAO_SETOR, "situacao_cadastral"),
        (SITUACAO_SETOR, GRAIN),
        (DISTRIBUICAO_PORTE, TOTAL),
        (DISTRIBUICAO_PORTE, "porte_empresa"),
    }
    # 1) grand totals: every eligible establishment, 3 active, every company
    assert comparisons[(CNAE_UF_ANO, TOTAL, "estabelecimentos")].actual_total == N_ESTABS
    assert comparisons[(CNAE_UF_ANO, TOTAL, "estabelecimentos_ativos")].actual_total == 3
    assert comparisons[(SITUACAO_SETOR, TOTAL, "estabelecimentos")].actual_total == N_ESTABS
    assert comparisons[(DISTRIBUICAO_PORTE, TOTAL, "empresas")].actual_total == N_EMPRESAS
    porte_total = comparisons[(DISTRIBUICAO_PORTE, TOTAL, "participacao")]
    assert porte_total.actual_total == pytest.approx(1.0)
    # minus the establishment whose company is not in silver (ADR 012)
    assert comparisons[(DISTRIBUICAO_PORTE, TOTAL, "estabelecimentos")].actual_total == 5
    # 2) by UF: SP 3, RJ 1, MG 1 and the null UF as its own group
    by_uf = comparisons[(CNAE_UF_ANO, "uf", "estabelecimentos")]
    assert {g.key: g.actual for g in by_uf.listed} == {
        (None,): 1,
        ("MG",): 1,
        ("RJ",): 1,
        ("SP",): 3,
    }
    by_size = comparisons[(DISTRIBUICAO_PORTE, "porte_empresa", "empresas")]
    assert {g.key: g.actual for g in by_size.listed} == {
        (None,): 1,
        ("01",): 2,
        ("03",): 1,
        ("05",): 1,
        ("99",): 1,
    }
    # distinct companies compared at the gold grain only, never summed
    empresas = comparisons[(CNAE_UF_ANO, GRAIN, "empresas")]
    assert empresas.expected_total is None and empresas.groups == 5
    assert (CNAE_UF_ANO, "uf", "empresas") not in comparisons


def test_broken_gold_fails_where_it_is_broken(silver: dict[str, DataFrame]) -> None:
    gold = build_gold(silver)
    sp = (F.col("uf") == "SP") & (F.col("ano_inicio_atividade") == 2020)
    sp_retail = sp & (F.col("cnae_fiscal_principal") == "4711302")
    one_more = gold[CNAE_UF_ANO].withColumn(
        "estabelecimentos",
        F.when(sp_retail, F.col("estabelecimentos") + 1).otherwise(F.col("estabelecimentos")),
    )
    got = check(silver, {**gold, CNAE_UF_ANO: one_more})
    assert failed(got) == {
        (CNAE_UF_ANO, TOTAL, "estabelecimentos"),
        (CNAE_UF_ANO, "uf", "estabelecimentos"),
        (CNAE_UF_ANO, GRAIN, "estabelecimentos"),
    }
    by_uf = got[(CNAE_UF_ANO, "uf", "estabelecimentos")]
    assert [g.key for g in by_uf.listed if not g.passed] == [("SP",)]

    # empresas summed instead of counted distinct: only the distinct check catches it
    summed = gold[CNAE_UF_ANO].withColumn("empresas", F.col("estabelecimentos"))
    assert failed(check(silver, {**gold, CNAE_UF_ANO: summed})) == {
        (CNAE_UF_ANO, GRAIN, "empresas")
    }

    # null status group dropped (ADR 012 says it must be kept)
    no_null = gold[SITUACAO_SETOR].where(F.col("situacao_cadastral").isNotNull())
    assert failed(check(silver, {**gold, SITUACAO_SETOR: no_null})) == {
        (SITUACAO_SETOR, TOTAL, "estabelecimentos"),
        (SITUACAO_SETOR, "situacao_cadastral", "estabelecimentos"),
        (SITUACAO_SETOR, GRAIN, "estabelecimentos"),
        (SITUACAO_SETOR, GRAIN, "empresas"),
    }

    # a size group duplicated: every measure of the size table is off
    doubled = gold[DISTRIBUICAO_PORTE].unionByName(
        gold[DISTRIBUICAO_PORTE].where(F.col("porte_empresa") == "05")
    )
    broken = failed(check(silver, {**gold, DISTRIBUICAO_PORTE: doubled}))
    assert (DISTRIBUICAO_PORTE, "porte_empresa", "empresas") in broken
    assert (DISTRIBUICAO_PORTE, TOTAL, "participacao") in broken


def test_invalid_cnpj_switch_must_match_the_gold_run(silver: dict[str, DataFrame]) -> None:
    counted = build_gold(silver)
    assert failed(check(silver, counted, include_invalid_cnpj=False))  # gold counted one more
    strict = build_gold(silver, include_invalid_cnpj=False)
    got = check(silver, strict, include_invalid_cnpj=False)
    assert failed(got) == set()
    assert got[(CNAE_UF_ANO, TOTAL, "estabelecimentos")].actual_total == N_ESTABS - 1


def test_missing_inputs_are_skipped_or_errors(silver: dict[str, DataFrame]) -> None:
    gold = build_gold(silver)
    porte_only = {DISTRIBUICAO_PORTE: distribuicao_porte(silver["empresas"], None)}
    comparisons, skipped, errors = reconcile_frames(porte_only, silver["empresas"], None)
    assert errors == [] and set(skipped) == {CNAE_UF_ANO, SITUACAO_SETOR}
    assert {c.measure for c in comparisons} == {"empresas", "participacao"}
    assert all(c.passed for c in comparisons)

    _, _, errors = reconcile_frames(gold, None, None)
    assert len(errors) == 3 and all("does not" in e for e in errors)
    _, _, errors = reconcile_frames({}, silver["empresas"], silver["estabelecimentos"])
    assert len(errors) == 3 and all("is missing" in e for e in errors)


def report(passed: bool = True) -> ReconcileReport:
    good = Comparison(
        DISTRIBUICAO_PORTE,
        "porte_empresa",
        ("porte_empresa",),
        "empresas",
        2,
        0,
        1_234_567.0,
        1_234_567.0,
        (Group(("01",), 1_234_560, 1_234_560), Group((None,), 7, 7)),
    )
    bad = Comparison(
        CNAE_UF_ANO, GRAIN, ("uf",), "empresas", 1, 1, None, None, (Group(("SP",), 2, 3),)
    )
    return ReconcileReport(
        "2026-09",
        True,
        (good,) if passed else (good, bad),
        {SITUACAO_SETOR: "no silver estabelecimentos nor gold table"},
    )


def test_report_markdown_and_json() -> None:
    ok = report()
    assert ok.passed and ok.to_dict()["tables"] == [DISTRIBUICAO_PORTE]
    markdown = ok.to_markdown()
    assert "all 1 comparisons match" in markdown
    assert "| null | 7 | 7 | pass |" in markdown and "| 01 | 1,234,560 | 1,234,560 |" in markdown
    bad = report(passed=False)
    assert not bad.passed and "**RECONCILIATION FAILED**" in bad.to_markdown()
    assert "non-additive" in bad.to_markdown() and "| SP | 2 | 3 | **FAIL** |" in bad.to_markdown()
    assert not ReconcileReport("2026-09", True, ()).passed  # nothing compared is not a pass
    assert not ReconcileReport("2026-09", True, ok.comparisons, errors=("x",)).passed


class _FakeSpark:
    stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_main_writes_the_report_and_fails_on_a_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeSpark()
    seen: dict[str, Any] = {}

    def fake_run(spark: Any, competencia: str, silver_root: str, gold_root: str, **kw: Any) -> Any:
        seen.update(silver=silver_root, gold=gold_root, **kw)
        return report()

    monkeypatch.setattr(reconcile, "get_spark", lambda *a, **k: fake)
    monkeypatch.setattr(reconcile, "run", fake_run)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "b")
    out = tmp_path / "rec"
    assert reconcile.main(["--competencia", "2026-09", "--out", str(out)]) == 0 and fake.stopped
    assert seen == {
        "silver": "s3a://b/silver",
        "gold": "s3a://b/gold",
        "include_invalid_cnpj": True,
    }
    data = json.loads((out / "reconcile.json").read_text(encoding="utf-8"))
    assert data["passed"] and data["comparisons"][0]["listed"][1]["key"] == [None]
    assert (out / "reconcile.md").read_text(encoding="utf-8").startswith("### Silver x gold")

    monkeypatch.setattr(reconcile, "run", lambda *a, **k: report(passed=False))
    args = ["--competencia", "2026-09", "--exclude-invalid-cnpj", "--out", str(out)]
    assert reconcile.main(args) == 1

    def fail(*a: Any, **k: Any) -> None:
        raise ValueError("bad competencia")

    monkeypatch.setattr(reconcile, "run", fail)
    assert reconcile.main(["--competencia", "x", "--out", str(out)]) == 1
