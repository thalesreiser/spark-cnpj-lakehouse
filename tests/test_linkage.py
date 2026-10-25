"""RF-10: partner record linkage on synthetic partners (ADR 014, docs/linkage.md).

Every name, masked CPF and CNPJ base here is synthetic: obviously fake names ("... TESTE ..."),
masked CPFs with made-up digits and CNPJ bases from the verified block 80300910-80300929.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse import linkage
from cnpj_lakehouse.linkage import (
    Thresholds,
    block_key,
    candidate_pairs,
    ensure_private_path,
    jaro,
    jaro_winkler,
    masked_cpf_digits,
    normalize_name,
    precision_from_review,
    prepare,
    public_candidates,
    report,
    review_sample,
    wilson_interval,
    write_review_sample,
)

needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="local Spark writes on Windows need HADOOP_HOME/winutils",
)

COLUMNS = ("cnpj_basico", "identificador_socio", "nome_socio_razao_social", "cpf_cnpj_socio")
CPF_A = "***000111**"
CPF_B = "***000222**"
SOCIOS = [
    ("80300910", "2", "MARIA TESTE DA SILVA", CPF_A),
    ("80300911", "2", "Maria Teste  Silva", CPF_A),  # same normalized name
    ("80300912", "2", "MARIA TESTI SILVA", CPF_A),  # one-letter typo
    ("80300913", "2", "MARIA FICTICIA SILVA", CPF_A),  # same block, other middle name
    ("80300914", "2", "MARIA TESTE DA SILVA", CPF_B),  # other masked digits: other block
    ("80300915", "1", "EMPRESA TESTE SILVA LTDA", "80300915000100"),  # legal entity: ignored
    ("80300916", "2", "JOSE EXEMPLO SOUZA", CPF_B),  # alone in its block
    ("80300916", "2", "JOSE EXEMPLO SOUZA", CPF_B),  # identical row: never paired with itself
]
NAMES = {
    "MARIA TESTE DA SILVA",
    "Maria Teste  Silva",
    "MARIA TESTI SILVA",
    "MARIA FICTICIA SILVA",
    "JOSE EXEMPLO SOUZA",
    "MARIA TESTE SILVA",
    "MARIA TESTI",
    "000111",
    "000222",
}


def socios(spark: SparkSession, rows: list[tuple[str, ...]] = SOCIOS) -> DataFrame:
    return spark.createDataFrame(rows, list(COLUMNS))


def one(spark: SparkSession, column: Any) -> Any:
    return spark.range(1).select(column.alias("v")).first()["v"]


# --- similarity --------------------------------------------------------------------------------


def test_jaro_and_jaro_winkler_reference_values() -> None:
    assert jaro("MARTHA", "MARHTA") == pytest.approx(0.9444, abs=1e-4)
    assert jaro_winkler("MARTHA", "MARHTA") == pytest.approx(0.9611, abs=1e-4)
    assert jaro_winkler("DIXON", "DICKSONX") == pytest.approx(0.8133, abs=1e-4)
    assert jaro_winkler("ABC", "ABC") == 1.0
    assert jaro("ABC", "XYZ") == 0.0
    assert jaro("", "ABC") == 0.0
    assert jaro_winkler(None, "ABC") is None


def test_thresholds_bands_and_validation(spark: SparkSession) -> None:
    t = Thresholds()
    assert (t.band(0.92), t.band(0.9199), t.band(0.85), t.band(0.84), t.band(None)) == (
        "match",
        "review",
        "review",
        "non_match",
        "non_match",
    )
    assert one(spark, Thresholds(0.9, 0.8).band_column(F.lit(0.85))) == "review"
    with pytest.raises(ValueError):
        Thresholds(match=0.8, review=0.9)
    with pytest.raises(ValueError):
        Thresholds(match=1.2)


# --- normalization and blocking ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected", "sort_tokens"),
    [
        (" Maria  da Silva-Teste ", "MARIA SILVA TESTE", False),
        ("JOÃO D'ÁVILA DOS TESTES E CIA", "JOAO AVILA TESTES CIA", False),
        ("Ana dos Exemplos 2", "ANA EXEMPLOS", False),
        ("SILVA TESTE MARIA", "MARIA SILVA TESTE", True),
        ("   ", None, False),
        ("DA DE", None, False),
        (None, None, False),
    ],
)
def test_normalize_name(
    spark: SparkSession, raw: str | None, expected: str | None, sort_tokens: bool
) -> None:
    assert one(spark, normalize_name(F.lit(raw).cast("string"), sort_tokens)) == expected


def test_block_key_uses_first_last_token_and_masked_digits(spark: SparkSession) -> None:
    name = F.lit("MARIA TESTE SILVA")
    assert one(spark, block_key(name, F.lit(CPF_A))) == "MARIA|SILVA|000111"
    assert one(spark, block_key(name, F.lit("80300915000100"))) == "MARIA|SILVA|"
    assert one(spark, block_key(name, None)) == "MARIA|SILVA"
    assert one(spark, block_key(F.lit(None).cast("string"), F.lit(CPF_A))) is None
    assert one(spark, masked_cpf_digits(F.lit(None).cast("string"))) == ""


def test_prepare_keeps_individuals_once_with_block_sizes(spark: SparkSession) -> None:
    rows = prepare(socios(spark)).collect()
    assert len(rows) == 6  # legal entity out, identical row collapsed
    sizes = {r["cnpj_basico"]: r["block_size"] for r in rows}
    assert sizes == {
        "80300910": 4,
        "80300911": 4,
        "80300912": 4,
        "80300913": 4,
        "80300914": 1,
        "80300916": 1,
    }
    assert all(len(r["row_id"]) == 64 for r in rows)


# --- candidate pairs ---------------------------------------------------------------------------


def test_candidate_pairs_within_blocks_only_scored_and_banded(spark: SparkSession) -> None:
    pairs = candidate_pairs(prepare(socios(spark))).collect()
    assert len(pairs) == 6  # C(4, 2) inside the only block of 2+ rows; no cross-block pair
    assert all(p["left_row_id"] < p["right_row_id"] for p in pairs)
    by_companies = {frozenset((p["left_cnpj_basico"], p["right_cnpj_basico"])): p for p in pairs}
    same = by_companies[frozenset(("80300910", "80300911"))]
    assert (same["score"], same["levenshtein_ratio"], same["band"]) == (1.0, 1.0, "match")
    typo = by_companies[frozenset(("80300910", "80300912"))]
    assert 0.92 <= typo["score"] < 1.0 and typo["band"] == "match"
    other = by_companies[frozenset(("80300910", "80300913"))]
    assert other["score"] < 0.92
    assert {p["band"] for p in pairs} <= set(linkage.BANDS)


def test_blocking_switches_and_oversized_blocks(spark: SparkSession) -> None:
    df = socios(spark)
    without_cpf = candidate_pairs(prepare(df, use_cpf=False)).count()
    assert without_cpf == 10  # the CPF_B row joins the MARIA|SILVA block: C(5, 2)
    assert candidate_pairs(prepare(df), max_block_size=3).count() == 0
    reordered = socios(
        spark,
        [
            ("80300920", "2", "SILVA TESTE MARIA", CPF_A),
            ("80300921", "2", "MARIA TESTE SILVA", CPF_A),
        ],
    )
    assert candidate_pairs(prepare(reordered)).count() == 0
    sorted_pairs = candidate_pairs(prepare(reordered, sort_tokens=True)).collect()
    assert [p["score"] for p in sorted_pairs] == [1.0]


def test_public_candidates_and_report_hold_no_names(spark: SparkSession) -> None:
    prepared = prepare(socios(spark))
    pairs = candidate_pairs(prepared)
    public = public_candidates(pairs, "2026-09")
    assert not any("nome" in c or c == "block_key" for c in public.columns)
    blob = json.dumps([r.asDict() for r in public.collect()])
    assert not any(name in blob for name in NAMES)

    rep = report("2026-09", prepared, pairs, Thresholds(), max_block_size=3)
    assert (rep.partners, rep.named, rep.blocks, rep.blocks_skipped_oversized) == (6, 6, 0, 1)
    rep = report("2026-09", prepared, pairs, Thresholds(), max_block_size=200)
    assert (rep.blocks, rep.rows_in_blocks, rep.largest_block, rep.candidate_pairs) == (1, 4, 4, 6)
    assert sum(rep.bands.values()) == 6 and sum(h["pairs"] for h in rep.histogram) == 6
    assert rep.histogram[-1]["upper"] == 1.0  # a 1.0 score falls in the last bin
    text = json.dumps(rep.to_dict()) + rep.to_markdown()
    assert not any(name in text for name in NAMES)
    assert "Candidate pairs | 6" in rep.to_markdown()


# --- review sample and precision ---------------------------------------------------------------


def test_review_sample_is_seeded_and_band_checked(spark: SparkSession) -> None:
    pairs = candidate_pairs(prepare(socios(spark)), Thresholds(match=0.99, review=0.5))
    first = review_sample(pairs, 2, seed=7)
    assert first == review_sample(pairs, 2, seed=7) and len(first) == 2
    assert all(0.5 <= r["score"] < 0.99 and r["label"] == "" for r in first)
    with pytest.raises(ValueError):
        review_sample(pairs, 1, band="maybe")


def test_ensure_private_path_refuses_other_locations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for bad in (
        tmp_path / "review.csv",
        tmp_path / "build" / "x.csv",
        tmp_path / "build" / "linkage-review",
    ):
        with pytest.raises(ValueError):
            ensure_private_path(bad, cwd=tmp_path)
    good = tmp_path / "build" / "linkage-review" / "r.csv"
    assert ensure_private_path(good, cwd=tmp_path) == good.resolve()

    def not_ignored(*a: Any, **k: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(a, 1)

    monkeypatch.setattr(linkage.subprocess, "run", not_ignored)
    with pytest.raises(ValueError, match="not ignored"):
        ensure_private_path(good, cwd=tmp_path)

    def no_git(*a: Any, **k: Any) -> Any:
        raise FileNotFoundError("git")

    monkeypatch.setattr(linkage.subprocess, "run", no_git)
    assert ensure_private_path(good, cwd=tmp_path) == good.resolve()


def test_repository_build_dir_is_git_ignored() -> None:
    root = Path(__file__).resolve().parents[1]
    target = root / "build" / "linkage-review" / "review.csv"
    assert ensure_private_path(target, cwd=root) == target.resolve()


def test_review_round_trip_and_precision(tmp_path: Path) -> None:
    rows = [
        {**dict.fromkeys(linkage.REVIEW_COLUMNS, ""), "pair": f"p{i}", "score": 0.9}
        for i in range(4)
    ]
    path = write_review_sample(rows, tmp_path / "build" / "linkage-review" / "r.csv", tmp_path)
    assert precision_from_review(path).labeled == 0
    text = path.read_text(encoding="utf-8").splitlines()
    labels = ["match", "no_match", " MATCH ", ""]
    path.write_text(
        "\n".join([text[0], *[line + label for line, label in zip(text[1:], labels, strict=True)]])
        + "\n",
        encoding="utf-8",
    )
    result = precision_from_review(path)
    assert (result.sampled, result.labeled, result.matches, result.precision) == (4, 3, 2, 0.6667)
    assert result.ci95_low is not None and result.ci95_low < 0.6667 < (result.ci95_high or 0)
    path.write_text(text[0] + "\n" + text[1] + "maybe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="line 2"):
        precision_from_review(path)


def test_wilson_interval_bounds() -> None:
    assert wilson_interval(10, 10)[1] == 1.0
    low, high = wilson_interval(45, 50)
    assert 0.78 < low < 0.9 < high < 0.97


# --- job and CLI -------------------------------------------------------------------------------


@needs_hadoop_fs
def test_run_end_to_end_on_synthetic_silver(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    socios(spark).write.parquet(str(silver / "socios"))
    rep = linkage.run(
        spark,
        "2026-09",
        str(silver),
        str(gold),
        thresholds=Thresholds(match=0.99, review=0.5),
        review_n=10,
        input_format="parquet",
        output_format="parquet",
    )
    assert rep.candidate_pairs == 6 and rep.review_sample == rep.bands["review"] > 0
    stored = spark.read.parquet(str(gold / linkage.CANDIDATES_TABLE))
    assert stored.count() == 6 and not [c for c in stored.columns if "nome" in c]
    sample = tmp_path / "build" / "linkage-review" / "review-2026-09.csv"
    assert "MARIA" in sample.read_text(encoding="utf-8")  # names only in the ignored sample
    with pytest.raises(ValueError):
        linkage.run(
            spark,
            "2026-09",
            str(silver),
            None,
            review_n=1,
            review_path=tmp_path / "x.csv",
            input_format="parquet",
        )


class _FakeSpark:
    stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_main_candidates_writes_aggregate_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake = _FakeSpark()
    seen: dict[str, Any] = {}

    def fake_run(spark: Any, competencia: str, silver: str, gold: str | None, **kw: Any) -> Any:
        seen.update(silver=silver, gold=gold, **kw)
        return linkage.LinkageReport(
            "2026-09",
            {"match": 0.92, "review": 0.85},
            200,
            6,
            6,
            1,
            0,
            4,
            4,
            6,
            {"match": 3, "review": 1, "non_match": 2},
            [{"lower": 0.95, "upper": 1.0, "pairs": 3}],
        )

    monkeypatch.setattr(linkage, "get_spark", lambda *a, **k: fake)
    monkeypatch.setattr(linkage, "run", fake_run)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "b")
    out = tmp_path / "out"
    args = ["candidates", "--competencia", "2026-09", "--out", str(out), "--sort-tokens"]
    assert linkage.main(args) == 0 and fake.stopped
    assert seen["silver"] == "s3a://b/silver" and seen["gold"] == "s3a://b/gold"
    assert seen["sort_tokens"] and seen["use_cpf"]
    assert json.loads((out / "linkage-2026-09.json").read_text())["candidate_pairs"] == 6
    assert (out / "linkage-2026-09.md").exists()
    assert linkage.main([*args, "--no-table", "--no-cpf-block"]) == 0
    assert seen["gold"] is None and not seen["use_cpf"]
    assert linkage.main([*args, "--match", "0.5", "--review", "0.9"]) == 1
    bad = [*args, "--review-sample", "5", "--review-file", str(tmp_path / "names.csv")]
    assert linkage.main(bad) == 1


def test_main_precision(tmp_path: Path) -> None:
    review = tmp_path / "build" / "linkage-review" / "r.csv"
    rows = [{**dict.fromkeys(linkage.REVIEW_COLUMNS, ""), "pair": "p", "label": "match"}]
    write_review_sample(rows, review, tmp_path)
    out = tmp_path / "agg"
    assert linkage.main(["precision", "--review", str(review), "--out", str(out)]) == 0
    assert json.loads((out / "precision.json").read_text())["precision"] == 1.0
    assert linkage.main(["precision", "--review", str(tmp_path / "missing.csv")]) == 1
