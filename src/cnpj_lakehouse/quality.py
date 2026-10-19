"""RF-03 quality checks of the silver layer: nulls, domains, references and key uniqueness.

The rules are declarative (`Rule`, one list per table in `RULES`) and come from the data
dictionary (``docs/data-dictionary.md``, section "Quality rules"):

* ``not_null`` - mandatory columns: the business key columns (``nullable = False`` in
  `cnpj_lakehouse.schemas`) are ``error``; other columns that should always be filled are ``warn``;
* ``domain`` - coded columns with a closed set of values documented in the dictionary (UF,
  registration status, company size, head office/branch, partner type, age group, S/N options);
* ``pattern`` - format of identifiers (CNPJ parts, CEP), as a regular expression;
* ``reference`` - codes that must exist in a silver lookup table of the same month
  (``municipios``, ``cnaes``, ``naturezas``, ``qualificacoes``, ``paises``, ``motivos``); skipped
  when the lookup has no rows for the month;
* ``unique`` - the silver business key (`cnpj_lakehouse.silver.RULES`): no key twice;
* ``is_true`` - a boolean flag that should be true: ``cnpj_valido`` (ADR 010 keeps invalid CNPJs
  flagged, so this is a ``warn`` metric, never a blocker).

Null values are only counted by ``not_null``; the other row rules ignore them. Each rule has a
severity: an ``error`` failure blocks the pipeline (non-zero exit code), a ``warn`` failure is
reported only. The checks are pure DataFrame -> result functions (RNF-04); `run` and `main`
read the silver Delta tables and write the report (counts only, never data, RST-01).

Usage (inside the ``spark-client`` container, see ``make quality``)::

    python -m cnpj_lakehouse.quality --competencia 2026-09 [--tables empresas] \
        --report build/quality-2026-09
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse.bronze import PARTITION_COLUMN
from cnpj_lakehouse.download import parse_competencia
from cnpj_lakehouse.schemas import TABLES, get_table
from cnpj_lakehouse.silver import CNPJ_VALID_COLUMN, SILVER_LAYER
from cnpj_lakehouse.silver import RULES as SILVER_RULES
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger(__name__)


class Severity(StrEnum):
    ERROR = "error"  # blocks the pipeline
    WARN = "warn"  # reported only


class Kind(StrEnum):
    NOT_NULL = "not_null"
    DOMAIN = "domain"
    PATTERN = "pattern"
    REFERENCE = "reference"
    UNIQUE = "unique"
    IS_TRUE = "is_true"


class Status(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIPPED = "skipped"


# 26 states + Federal District, and "EX" for establishments abroad.
UFS = frozenset(
    [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
        "EX",
    ]
)
PORTE_EMPRESA = frozenset({"00", "01", "03", "05"})
SITUACAO_CADASTRAL = frozenset({"01", "02", "03", "04", "08"})
MATRIZ_FILIAL = frozenset({"1", "2"})
IDENTIFICADOR_SOCIO = frozenset({"1", "2", "3"})
FAIXA_ETARIA = frozenset(str(n) for n in range(10))
SIM_NAO = frozenset({"S", "N"})


@dataclass(frozen=True)
class Rule:
    """One quality rule of one table. ``values``/``pattern``/``reference`` depend on ``kind``."""

    table: str
    kind: Kind
    columns: tuple[str, ...]
    severity: Severity
    values: frozenset[str] | None = None
    pattern: str | None = None
    reference: str | None = None  # lookup table whose ``codigo`` must contain the value

    @property
    def name(self) -> str:
        target = f"->{self.reference}" if self.reference else ""
        return f"{self.table}.{'+'.join(self.columns)}:{self.kind.value}{target}"

    def describe(self) -> str:
        if self.kind is Kind.DOMAIN:
            assert self.values is not None
            return "in {" + ", ".join(sorted(self.values)) + "}"
        if self.kind is Kind.PATTERN:
            return f"matches `{self.pattern}`"
        if self.kind is Kind.REFERENCE:
            return f"exists in `{self.reference}.codigo`"
        if self.kind is Kind.UNIQUE:
            return "no key appears twice"
        if self.kind is Kind.IS_TRUE:
            return "is true"
        return "is filled"


@dataclass(frozen=True)
class RuleResult:
    """Outcome of one rule: rows checked and failing (for ``unique``: duplicated keys)."""

    rule: str
    table: str
    kind: str
    columns: tuple[str, ...]
    severity: str
    description: str
    rows_checked: int
    failures: int
    status: str

    @property
    def blocking(self) -> bool:
        return self.status == Status.FAIL and self.severity == Severity.ERROR


@dataclass(frozen=True)
class QualityReport:
    competencia: str
    results: tuple[RuleResult, ...] = field(default_factory=tuple)

    @property
    def blocking(self) -> tuple[RuleResult, ...]:
        return tuple(r for r in self.results if r.blocking)

    @property
    def ok(self) -> bool:
        return not self.blocking

    def tables(self) -> list[str]:
        return list(dict.fromkeys(r.table for r in self.results))

    def to_dict(self) -> dict[str, Any]:
        counts = {s.value: sum(r.status == s for r in self.results) for s in Status}
        return {
            "competencia": self.competencia,
            "ok": self.ok,
            "rules": len(self.results),
            **counts,
            "blocking_failures": [r.rule for r in self.blocking],
            "warnings": [
                r.rule for r in self.results if r.status == Status.FAIL and not r.blocking
            ],
            "results": [asdict(r) for r in self.results],
        }

    def to_markdown(self) -> str:
        verdict = "OK" if self.ok else f"FAILED ({len(self.blocking)} blocking)"
        lines = [
            f"### Silver quality - competencia {self.competencia}: {verdict}",
            "",
            "`error` failures block the pipeline; `warn` failures are reported only.",
            "",
            "| Table | Rule | Column(s) | Check | Severity | Rows checked | Failures | %"
            " | Status |",
            "|---|---|---|---|---|---:|---:|---:|---|",
        ]
        for r in self.results:
            pct = f"{100 * r.failures / r.rows_checked:.2f}" if r.rows_checked else "-"
            status = {"pass": "pass", "fail": "**FAIL**", "skipped": "skipped"}[r.status]
            lines.append(
                f"| {r.table} | {r.kind} | {', '.join(f'`{c}`' for c in r.columns)}"
                f" | {r.description} | {r.severity} | {r.rows_checked:,} | {r.failures:,}"
                f" | {pct} | {status} |"
            )
        return "\n".join(lines) + "\n"


def _not_null(table: str, *columns: str, severity: Severity = Severity.WARN) -> list[Rule]:
    return [Rule(table, Kind.NOT_NULL, (c,), severity) for c in columns]


def _ref(table: str, column: str, lookup: str) -> Rule:
    return Rule(table, Kind.REFERENCE, (column,), Severity.WARN, reference=lookup)


def _rules() -> dict[str, tuple[Rule, ...]]:
    error, warn = Severity.ERROR, Severity.WARN
    rules: dict[str, list[Rule]] = {name: [] for name in TABLES}
    for name, spec in TABLES.items():
        # Mandatory columns: the not-nullable (key) columns of the schema block the pipeline.
        rules[name] += _not_null(
            name, *(c.name for c in spec.columns if not c.nullable), severity=error
        )
    for name in ("empresas", "estabelecimentos", "socios", "simples"):
        rules[name].append(
            Rule(name, Kind.PATTERN, ("cnpj_basico",), error, pattern=r"^[0-9A-Z]{8}$")
        )
    rules["empresas"] += [
        *_not_null("empresas", "razao_social", "natureza_juridica"),
        Rule("empresas", Kind.DOMAIN, ("porte_empresa",), error, values=PORTE_EMPRESA),
        _ref("empresas", "natureza_juridica", "naturezas"),
        _ref("empresas", "qualificacao_responsavel", "qualificacoes"),
    ]
    rules["estabelecimentos"] += [
        *_not_null(
            "estabelecimentos",
            "identificador_matriz_filial",
            "situacao_cadastral",
            "uf",
            "municipio",
            "cnae_fiscal_principal",
        ),
        Rule("estabelecimentos", Kind.PATTERN, ("cnpj_ordem",), error, pattern=r"^[0-9A-Z]{4}$"),
        Rule("estabelecimentos", Kind.PATTERN, ("cnpj_dv",), error, pattern=r"^[0-9]{2}$"),
        Rule("estabelecimentos", Kind.DOMAIN, ("uf",), error, values=UFS),
        Rule(
            "estabelecimentos",
            Kind.DOMAIN,
            ("situacao_cadastral",),
            error,
            values=SITUACAO_CADASTRAL,
        ),
        Rule(
            "estabelecimentos",
            Kind.DOMAIN,
            ("identificador_matriz_filial",),
            error,
            values=MATRIZ_FILIAL,
        ),
        Rule("estabelecimentos", Kind.PATTERN, ("cep",), warn, pattern=r"^[0-9]{8}$"),
        _ref("estabelecimentos", "municipio", "municipios"),
        _ref("estabelecimentos", "cnae_fiscal_principal", "cnaes"),
        _ref("estabelecimentos", "motivo_situacao_cadastral", "motivos"),
        _ref("estabelecimentos", "pais", "paises"),
        # ADR 010: invalid CNPJs are kept, flagged; a metric, not a blocker.
        Rule("estabelecimentos", Kind.IS_TRUE, (CNPJ_VALID_COLUMN,), warn),
    ]
    rules["socios"] += [
        *_not_null("socios", "identificador_socio", "nome_socio_razao_social"),
        Rule("socios", Kind.DOMAIN, ("identificador_socio",), error, values=IDENTIFICADOR_SOCIO),
        Rule("socios", Kind.DOMAIN, ("faixa_etaria",), warn, values=FAIXA_ETARIA),
        _ref("socios", "qualificacao_socio", "qualificacoes"),
        _ref("socios", "qualificacao_representante_legal", "qualificacoes"),
        _ref("socios", "pais", "paises"),
    ]
    rules["simples"] += [
        Rule("simples", Kind.DOMAIN, ("opcao_pelo_simples",), warn, values=SIM_NAO),
        Rule("simples", Kind.DOMAIN, ("opcao_mei",), warn, values=SIM_NAO),
    ]
    for name, spec in TABLES.items():
        if {c.name for c in spec.columns} == {"codigo", "descricao"}:  # lookup tables
            rules[name] += _not_null(name, "descricao")
        # Uniqueness of the silver business key (same key as the deduplication).
        rules[name].append(Rule(name, Kind.UNIQUE, SILVER_RULES[name].key, error))
    return {name: tuple(r) for name, r in rules.items()}


RULES: Mapping[str, tuple[Rule, ...]] = _rules()


def failing(rule: Rule) -> Column:
    """Row-level predicate: true when the row breaks the rule (null never fails but not_null)."""
    if rule.kind in (Kind.UNIQUE, Kind.REFERENCE):
        raise ValueError(f"{rule.kind} is not a row-level rule")
    col = F.col(rule.columns[0])
    if rule.kind is Kind.NOT_NULL:
        return col.isNull()
    if rule.kind is Kind.DOMAIN:
        assert rule.values is not None
        return col.isNotNull() & ~col.isin(*sorted(rule.values))
    if rule.kind is Kind.PATTERN:
        assert rule.pattern is not None
        return col.isNotNull() & ~col.rlike(rule.pattern)
    return col.isNotNull() & ~col  # Kind.IS_TRUE


def _result(rule: Rule, rows: int, failures: int, status: Status | None = None) -> RuleResult:
    if status is None:
        status = Status.FAIL if failures else Status.PASS
    return RuleResult(
        rule=rule.name,
        table=rule.table,
        kind=rule.kind.value,
        columns=rule.columns,
        severity=rule.severity.value,
        description=rule.describe(),
        rows_checked=rows,
        failures=failures,
        status=status.value,
    )


def duplicated_keys(df: DataFrame, key: Sequence[str]) -> int:
    """Number of distinct keys that appear in more than one row."""
    return df.groupBy(*key).count().where(F.col("count") > 1).count()


def missing_references(df: DataFrame, column: str, lookup: DataFrame) -> int:
    """Rows whose non-null ``column`` has no ``codigo`` in the lookup table."""
    codes = lookup.select(F.col("codigo").alias("_code")).distinct()
    values = df.select(F.col(column).alias("_code")).where(F.col("_code").isNotNull())
    return values.join(F.broadcast(codes), "_code", "left_anti").count()


def check_table(
    df: DataFrame,
    table: str,
    lookups: Mapping[str, DataFrame] | None = None,
    rules: Sequence[Rule] | None = None,
) -> list[RuleResult]:
    """Runs the rules of a table on its silver DataFrame (one aggregation for the row rules).

    ``lookups`` maps a lookup table name to its silver DataFrame; a ``reference`` rule whose
    lookup is absent is ``skipped``. A rule on a column the DataFrame lacks raises `ValueError`.
    """
    lookups = lookups or {}
    rules = RULES[table] if rules is None else rules
    missing = sorted({c for r in rules for c in r.columns} - set(df.columns))
    if missing:
        raise ValueError(f"{table}: column(s) {', '.join(missing)} not in the silver table")
    row_rules = [r for r in rules if r.kind not in (Kind.UNIQUE, Kind.REFERENCE)]
    aggregates = [F.count(F.lit(1)).alias("_rows")] + [
        F.sum(F.when(failing(r), 1).otherwise(0)).alias(f"_r{i}") for i, r in enumerate(row_rules)
    ]
    counts = df.agg(*aggregates).first()
    assert counts is not None
    rows = int(counts["_rows"])
    by_rule = {r: int(counts[f"_r{i}"] or 0) for i, r in enumerate(row_rules)}
    results: list[RuleResult] = []
    for rule in rules:
        if rule in by_rule:
            results.append(_result(rule, rows, by_rule[rule]))
        elif rule.kind is Kind.UNIQUE:
            results.append(_result(rule, rows, duplicated_keys(df, rule.columns)))
        elif rule.reference is not None and rule.reference in lookups:
            failures = missing_references(df, rule.columns[0], lookups[rule.reference])
            results.append(_result(rule, rows, failures))
        else:
            results.append(_result(rule, rows, 0, Status.SKIPPED))
    return results


def check_tables(
    frames: Mapping[str, DataFrame],
    competencia: str,
    lookups: Mapping[str, DataFrame] | None = None,
) -> QualityReport:
    """Quality report of several silver tables of one month (lookups default to ``frames``)."""
    lookups = frames if lookups is None else lookups
    results: list[RuleResult] = []
    for table, df in frames.items():
        results += check_table(df, table, lookups)
    return QualityReport(competencia, tuple(results))


def write_report(report: QualityReport, out: Path) -> tuple[Path, Path]:
    """Writes ``quality.json`` and ``quality.md`` into ``out`` (counts only)."""
    out.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out / "quality.json", out / "quality.md"
    json_path.write_text(json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8")
    md_path.write_text(report.to_markdown(), encoding="utf-8")
    return json_path, md_path


def _read_month(spark: SparkSession, path: str, competencia: str, fmt: str) -> DataFrame | None:
    try:
        df = spark.read.format(fmt).load(path).where(F.col(PARTITION_COLUMN) == competencia)
    except AnalysisException:
        return None
    return df if df.limit(1).count() else None


def run(
    spark: SparkSession,
    competencia: str,
    silver_root: str,
    tables: Iterable[str] | None = None,
    fmt: str = "delta",
) -> QualityReport:
    """Checks the requested silver tables (default: every table with rows that month).

    Lookup tables of the month are read for the ``reference`` rules even when not requested. A
    table requested explicitly must have rows, otherwise `ValueError`.
    """
    competencia = parse_competencia(competencia)
    root = silver_root.rstrip("/")
    available = {
        name: df
        for name in TABLES
        if (df := _read_month(spark, f"{root}/{name}", competencia, fmt)) is not None
    }
    if tables is None:
        names = list(available)
    else:
        names = [get_table(n).name for n in tables]
        absent = [n for n in names if n not in available]
        if absent:
            raise ValueError(
                f"no silver rows for {competencia} in table(s) {', '.join(absent)};"
                " run `make silver` first"
            )
    if not names:
        raise ValueError(f"no silver table has rows for {competencia}; run `make silver` first")
    log.info("quality %s: %d table(s) %s", competencia, len(names), names)
    return check_tables({n: available[n] for n in names}, competencia, available)


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.quality",
        description="RF-03: quality checks (nulls, domains, uniqueness) of the silver layer.",
    )
    parser.add_argument("--competencia", required=True, help="reference month, YYYY-MM")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=f"tables ({', '.join(TABLES)}); default: every silver table with rows that month",
    )
    parser.add_argument("--silver", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/silver")
    parser.add_argument("--format", default="delta", help="table format (default: delta)")
    parser.add_argument(
        "--report", type=Path, default=None, help="report folder (default: build/quality-<month>)"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: 0 when no ``error`` rule fails, 1 otherwise (or on a usage error)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark(f"quality-{args.competencia}", cfg)
    try:
        report = run(
            spark, args.competencia, args.silver or cfg.path(SILVER_LAYER), args.tables, args.format
        )
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    out = args.report or Path(f"build/quality-{report.competencia}")
    write_report(report, out)
    for r in report.results:
        print(f"[quality] {r.status:7} {r.severity:5} {r.rule}: {r.failures}/{r.rows_checked}")
    blocking = report.blocking
    print(
        "[quality] OK"
        if not blocking
        else f"[quality] FAILED: {len(blocking)} blocking rule(s): "
        + ", ".join(r.rule for r in blocking)
    )
    return 0 if not blocking else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
