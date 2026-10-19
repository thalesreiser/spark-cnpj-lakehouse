"""RF-03 quality checks: each rule type passes and fails as expected, severities, report, exit code.

The checks run on tiny in-memory DataFrames shaped like silver tables (no file I/O, so also on
Windows). `run` reading Parquet tables is covered on Linux (CI); the Delta path on MinIO by the
CI ``bronze`` job (``make quality``).
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from cnpj_lakehouse import quality
from cnpj_lakehouse.bronze import PARTITION_COLUMN
from cnpj_lakehouse.quality import (
    RULES,
    UFS,
    Kind,
    QualityReport,
    Rule,
    RuleResult,
    Severity,
    check_table,
    check_tables,
    write_report,
)
from cnpj_lakehouse.schemas import TABLES
from cnpj_lakehouse.silver import RULES as SILVER_RULES

needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="local Spark writes on Windows need HADOOP_HOME/winutils",
)

COMPETENCIA = "2026-09"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-quality")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def frame(spark: SparkSession, table: str, rows: list[dict[str, Any]]) -> DataFrame:
    """Silver-shaped DataFrame of the columns the table's rules use (strings, flag boolean)."""
    names = list(dict.fromkeys(c for r in RULES[table] for c in r.columns))
    fields = [StructField(n, BooleanType() if n == "cnpj_valido" else StringType()) for n in names]
    fields.append(StructField(PARTITION_COLUMN, StringType()))
    data = [
        tuple({PARTITION_COLUMN: COMPETENCIA, **row}.get(f.name) for f in fields) for row in rows
    ]
    return spark.createDataFrame(data, StructType(fields))


def lookup(spark: SparkSession, table: str, *codes: str) -> DataFrame:
    return frame(spark, table, [{"codigo": c, "descricao": f"name {c}"} for c in codes])


def by_rule(results: list[RuleResult]) -> dict[str, RuleResult]:
    return {r.rule: r for r in results}


EMPRESA = {
    "cnpj_basico": "80300911",
    "razao_social": "EMPRESA FICTICIA LTDA",
    "natureza_juridica": "2062",
    "qualificacao_responsavel": "49",
    "porte_empresa": "01",
}


# --- rules -------------------------------------------------------------------------------------


def test_every_table_has_rules_with_the_silver_key_unique_and_mandatory() -> None:
    assert set(RULES) == set(TABLES)
    for name, rules in RULES.items():
        unique = [r for r in rules if r.kind is Kind.UNIQUE]
        assert [r.columns for r in unique] == [SILVER_RULES[name].key]
        assert unique[0].severity is Severity.ERROR
        mandatory = {
            r.columns[0] for r in rules if r.kind is Kind.NOT_NULL and r.severity == "error"
        }
        assert mandatory == {c.name for c in TABLES[name].columns if not c.nullable}
        assert len({r.name for r in rules}) == len(rules)  # names identify rules


def test_documented_domains_and_the_invalid_cnpj_is_only_a_warning() -> None:
    assert len(UFS) == 28 and {"SP", "DF", "EX"} <= UFS
    rules = {r.name: r for r in RULES["estabelecimentos"]}
    assert rules["estabelecimentos.situacao_cadastral:domain"].values == {
        "01",
        "02",
        "03",
        "04",
        "08",
    }
    assert rules["estabelecimentos.cnpj_valido:is_true"].severity is Severity.WARN  # ADR 010
    assert all(r.severity is Severity.WARN for r in rules.values() if r.kind is Kind.REFERENCE)
    porte = {r.name: r for r in RULES["empresas"]}["empresas.porte_empresa:domain"]
    assert porte.values == {"00", "01", "03", "05"} and porte.severity is Severity.ERROR


def test_rule_descriptions() -> None:
    texts = {r.kind: r.describe() for rules in RULES.values() for r in rules}
    assert texts[Kind.NOT_NULL] == "is filled"
    assert texts[Kind.UNIQUE] == "no key appears twice"
    assert texts[Kind.IS_TRUE] == "is true"
    assert texts[Kind.REFERENCE].startswith("exists in `")
    assert texts[Kind.PATTERN].startswith("matches `^")
    assert texts[Kind.DOMAIN].startswith("in {")


def test_unique_is_not_a_row_rule() -> None:
    with pytest.raises(ValueError, match="not a row-level rule"):
        quality.failing(RULES["cnaes"][-1])


# --- each rule type ----------------------------------------------------------------------------


def test_clean_empresas_pass_every_rule(spark: SparkSession) -> None:
    df = frame(spark, "empresas", [EMPRESA, {**EMPRESA, "cnpj_basico": "80300912"}])
    lookups = {
        "naturezas": lookup(spark, "naturezas", "2062"),
        "qualificacoes": lookup(spark, "qualificacoes", "49"),
    }
    results = check_table(df, "empresas", lookups)
    assert {r.status for r in results} == {"pass"}
    assert all(r.rows_checked == 2 and r.failures == 0 for r in results)


def test_not_null_domain_pattern_unique_and_reference_failures(spark: SparkSession) -> None:
    rows = [
        EMPRESA,
        {**EMPRESA, "porte_empresa": "02"},  # duplicated key, invalid size
        {**EMPRESA, "cnpj_basico": None, "porte_empresa": None},  # null key; null skips domain
        {**EMPRESA, "cnpj_basico": "1234", "razao_social": None, "natureza_juridica": "9999"},
    ]
    lookups = {"naturezas": lookup(spark, "naturezas", "2062")}
    results = by_rule(check_table(frame(spark, "empresas", rows), "empresas", lookups))
    failures = {name: r.failures for name, r in results.items()}
    assert failures == {
        "empresas.cnpj_basico:not_null": 1,
        "empresas.cnpj_basico:pattern": 1,
        "empresas.razao_social:not_null": 1,
        "empresas.natureza_juridica:not_null": 0,
        "empresas.porte_empresa:domain": 1,
        "empresas.natureza_juridica:reference->naturezas": 1,
        "empresas.qualificacao_responsavel:reference->qualificacoes": 0,
        "empresas.cnpj_basico:unique": 1,
    }
    assert results["empresas.qualificacao_responsavel:reference->qualificacoes"].status == (
        "skipped"  # no lookup rows that month
    )
    assert results["empresas.cnpj_basico:unique"].rows_checked == 4


def test_estabelecimentos_flag_and_domains(spark: SparkSession) -> None:
    base = {
        "cnpj_basico": "80300911",
        "cnpj_ordem": "0001",
        "cnpj_dv": "35",
        "identificador_matriz_filial": "1",
        "situacao_cadastral": "02",
        "uf": "SP",
        "municipio": "7107",
        "cnae_fiscal_principal": "4711302",
        "cep": "01001000",
        "cnpj_valido": True,
    }
    rows = [
        base,
        {**base, "cnpj_ordem": "0002", "uf": "XX", "situacao_cadastral": "2", "cnpj_valido": False},
        {**base, "cnpj_ordem": "0003", "cep": "1001-000", "identificador_matriz_filial": "3"},
        {**base, "cnpj_ordem": "0004", "uf": "EX", "cnpj_dv": "3A"},
    ]
    results = by_rule(check_table(frame(spark, "estabelecimentos", rows), "estabelecimentos"))
    assert results["estabelecimentos.uf:domain"].failures == 1  # EX is valid (abroad)
    assert results["estabelecimentos.situacao_cadastral:domain"].failures == 1
    assert results["estabelecimentos.identificador_matriz_filial:domain"].failures == 1
    assert results["estabelecimentos.cep:pattern"].failures == 1
    assert results["estabelecimentos.cnpj_dv:pattern"].failures == 1
    flag = results["estabelecimentos.cnpj_valido:is_true"]
    assert (flag.failures, flag.severity, flag.blocking) == (1, "warn", False)
    assert results["estabelecimentos.cnpj_basico+cnpj_ordem+cnpj_dv:unique"].failures == 0
    assert results["estabelecimentos.municipio:reference->municipios"].status == "skipped"


def test_missing_column_raises(spark: SparkSession) -> None:
    df = frame(spark, "cnaes", [{"codigo": "0111301", "descricao": "x"}]).drop("descricao")
    with pytest.raises(ValueError, match="descricao"):
        check_table(df, "cnaes")


def test_custom_rules_and_empty_table(spark: SparkSession) -> None:
    rule = Rule("simples", Kind.DOMAIN, ("opcao_mei",), Severity.ERROR, values=frozenset("SN"))
    [result] = check_table(frame(spark, "simples", []), "simples", rules=[rule])
    assert (result.rows_checked, result.failures, result.status) == (0, 0, "pass")


# --- severity, report and exit code ------------------------------------------------------------


def _report(spark: SparkSession) -> QualityReport:
    frames = {
        "cnaes": lookup(spark, "cnaes", "0111301", "0111301"),  # duplicated code: error
        "simples": frame(
            spark, "simples", [{"cnpj_basico": "80300911", "opcao_pelo_simples": "X"}]
        ),  # invalid S/N option: warn
    }
    return check_tables(frames, COMPETENCIA)


def test_only_error_failures_block(spark: SparkSession) -> None:
    report = _report(spark)
    assert not report.ok
    assert [r.rule for r in report.blocking] == ["cnaes.codigo:unique"]
    data = report.to_dict()
    assert data["blocking_failures"] == ["cnaes.codigo:unique"]
    assert data["warnings"] == ["simples.opcao_pelo_simples:domain"]
    assert data["fail"] == 2 and data["rules"] == len(RULES["cnaes"]) + len(RULES["simples"])
    assert report.tables() == ["cnaes", "simples"]
    assert QualityReport(COMPETENCIA, tuple(r for r in report.results if not r.blocking)).ok


def test_report_files_list_every_rule(spark: SparkSession, tmp_path: Path) -> None:
    report = _report(spark)
    json_path, md_path = write_report(report, tmp_path / "q")
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["competencia"] == COMPETENCIA and len(data["results"]) == len(report.results)
    markdown = md_path.read_text(encoding="utf-8")
    assert "FAILED (1 blocking)" in markdown
    assert markdown.count("\n| ") == len(report.results) + 1  # header + one line per rule
    assert (
        "| cnaes | unique | `codigo` | no key appears twice | error | 2 | 1 | 50.00 | **FAIL** |"
        in (markdown)
    )
    assert "80300911" not in markdown and "80300911" not in json_path.read_text()  # counts only
    empty = QualityReport(COMPETENCIA).to_markdown()
    assert ": OK" in empty


@pytest.mark.parametrize("blocking", [False, True])
def test_main_writes_the_report_and_exits_non_zero_on_error(
    spark: SparkSession, tmp_path: Path, blocking: bool, capsys: pytest.CaptureFixture[str]
) -> None:
    report = _report(spark)
    if not blocking:
        report = QualityReport(COMPETENCIA, tuple(r for r in report.results if not r.blocking))
    session = mock.MagicMock()
    with (
        mock.patch.object(quality, "get_spark", return_value=session),
        mock.patch.object(quality, "run", return_value=report) as run,
    ):
        code = quality.main(["--competencia", COMPETENCIA, "--report", str(tmp_path)])
    assert code == (1 if blocking else 0)
    assert run.call_args.args[2] == "s3a://lakehouse/silver"
    session.stop.assert_called_once()
    assert (tmp_path / "quality.md").exists() and (tmp_path / "quality.json").exists()
    assert ("FAILED" in capsys.readouterr().out) is blocking


def test_main_returns_1_when_there_is_nothing_to_check(tmp_path: Path) -> None:
    with (
        mock.patch.object(quality, "get_spark", return_value=mock.MagicMock()),
        mock.patch.object(quality, "run", side_effect=ValueError("no silver table")),
    ):
        assert quality.main(["--competencia", COMPETENCIA, "--report", str(tmp_path)]) == 1
    assert not (tmp_path / "quality.json").exists()


# --- run on files ------------------------------------------------------------------------------


@needs_hadoop_fs
def test_run_reads_the_month_and_the_lookups(spark: SparkSession, tmp_path: Path) -> None:
    root = tmp_path / "silver"
    lookup(spark, "naturezas", "2062").write.partitionBy(PARTITION_COLUMN).parquet(
        str(root / "naturezas")
    )
    frame(spark, "empresas", [EMPRESA]).write.partitionBy(PARTITION_COLUMN).parquet(
        str(root / "empresas")
    )
    report = quality.run(spark, COMPETENCIA, str(root), ["empresas"], fmt="parquet")
    assert report.tables() == ["empresas"] and report.ok
    results = by_rule(list(report.results))
    assert results["empresas.natureza_juridica:reference->naturezas"].status == "pass"
    assert quality.run(spark, COMPETENCIA, str(root), fmt="parquet").tables() == [
        "empresas",
        "naturezas",
    ]
    with pytest.raises(ValueError, match="socios"):
        quality.run(spark, COMPETENCIA, str(root), ["socios"], fmt="parquet")
    with pytest.raises(ValueError, match="no silver table"):
        quality.run(spark, "2026-08", str(root), fmt="parquet")
