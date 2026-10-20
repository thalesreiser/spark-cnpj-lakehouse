"""RF-05: gold aggregations by CNAE/state/year, company size and registration status (Delta).

The gold tables are derived from the **current state** of the silver tables (ADR 011: each
silver table is read whole, one row per business key). Three tables are built:

* ``gold_empresas_cnae_uf_ano``: main CNAE x UF x opening year; counts establishments;
* ``gold_distribuicao_porte``: company size (``porte_empresa``); counts companies;
* ``gold_situacao_setor``: registration status x CNAE section/division; counts establishments.

Counting rules (ADR 012):

* the **establishment** is the unit of ``gold_empresas_cnae_uf_ano`` and ``gold_situacao_setor``
  (CNAE, UF, opening date and status are establishment attributes); the **company**
  (``cnpj_basico``) is the unit of ``gold_distribuicao_porte`` (size is a company attribute).
  ``empresas`` columns are distinct ``cnpj_basico`` counts within a row: they do not add up
  across rows (a company with branches in two states counts in both);
* establishments with an invalid CNPJ (``cnpj_valido = false``, ADR 010) are **counted** by
  default, so gold reconciles with silver and with the source; ``include_invalid_cnpj=False``
  (``--exclude-invalid-cnpj``) leaves them out, and the run summary reports how many there are;
* a null code (CNAE, UF, size, status) or a null opening date is **kept as its own group**
  (null key), never dropped; a description is null when the code is null or not in the lookup.

Lookups are joined with ``F.broadcast`` (ADR 003): the ``cnaes`` silver table for the CNAE
description, and small in-memory tables for the documented domains (size, status, CNAE
sections). The aggregations are pure ``DataFrame -> DataFrame`` functions (RNF-04); `run` reads
silver, writes every gold table as a whole-table Delta overwrite (gold is derived, re-runs give
the same tables) and checks that the gold sums reconcile with the eligible silver rows.

Usage (inside the ``spark-client`` container, see ``make gold``)::

    python -m cnpj_lakehouse.gold --competencia 2026-09 [--summary build/gold-2026-09.json]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from cnpj_lakehouse.download import parse_competencia
from cnpj_lakehouse.silver import CNPJ_VALID_COLUMN, SILVER_LAYER
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger("cnpj_lakehouse.gold")

GOLD_LAYER = "gold"
CNAE_UF_ANO = "gold_empresas_cnae_uf_ano"
DISTRIBUICAO_PORTE = "gold_distribuicao_porte"
SITUACAO_SETOR = "gold_situacao_setor"
GOLD_TABLES = (CNAE_UF_ANO, DISTRIBUICAO_PORTE, SITUACAO_SETOR)

ACTIVE = "02"  # situacao_cadastral of an active establishment

# Documented domains (data dictionary); the source has no lookup file for them.
PORTE_DESCRICAO: Mapping[str, str] = {
    "00": "NÃO INFORMADO",
    "01": "MICRO EMPRESA",
    "03": "EMPRESA DE PEQUENO PORTE",
    "05": "DEMAIS",
}
SITUACAO_DESCRICAO: Mapping[str, str] = {
    "01": "NULA",
    "02": "ATIVA",
    "03": "SUSPENSA",
    "04": "INAPTA",
    "08": "BAIXADA",
}
# CNAE 2.x sections (IBGE/CONCLA): letter, first and last division (2-digit prefix), name.
CNAE_SECOES: tuple[tuple[str, int, int, str], ...] = (
    ("A", 1, 3, "Agricultura, pecuária, produção florestal, pesca e aquicultura"),
    ("B", 5, 9, "Indústrias extrativas"),
    ("C", 10, 33, "Indústrias de transformação"),
    ("D", 35, 35, "Eletricidade e gás"),
    ("E", 36, 39, "Água, esgoto, atividades de gestão de resíduos e descontaminação"),
    ("F", 41, 43, "Construção"),
    ("G", 45, 47, "Comércio; reparação de veículos automotores e motocicletas"),
    ("H", 49, 53, "Transporte, armazenagem e correio"),
    ("I", 55, 56, "Alojamento e alimentação"),
    ("J", 58, 63, "Informação e comunicação"),
    ("K", 64, 66, "Atividades financeiras, de seguros e serviços relacionados"),
    ("L", 68, 68, "Atividades imobiliárias"),
    ("M", 69, 75, "Atividades profissionais, científicas e técnicas"),
    ("N", 77, 82, "Atividades administrativas e serviços complementares"),
    ("O", 84, 84, "Administração pública, defesa e seguridade social"),
    ("P", 85, 85, "Educação"),
    ("Q", 86, 88, "Saúde humana e serviços sociais"),
    ("R", 90, 93, "Artes, cultura, esporte e recreação"),
    ("S", 94, 96, "Outras atividades de serviços"),
    ("T", 97, 97, "Serviços domésticos"),
    ("U", 99, 99, "Organismos internacionais e outras instituições extraterritoriais"),
)

# Establishment columns the aggregations need (projection pushed down to the silver read).
ESTABLISHMENT_COLUMNS = (
    "cnpj_basico",
    "cnae_fiscal_principal",
    "uf",
    "data_inicio_atividade",
    "situacao_cadastral",
    CNPJ_VALID_COLUMN,
)


def _active() -> Column:
    return F.when(F.col("situacao_cadastral") == ACTIVE, 1).otherwise(0)


def domain_lookup(
    spark: SparkSession, mapping: Mapping[str, str], key: str, value: str
) -> DataFrame:
    """Small code -> description table from a documented domain (to be broadcast)."""
    schema = StructType([StructField(key, StringType(), False), StructField(value, StringType())])
    return spark.createDataFrame(sorted(mapping.items()), schema)


def cnae_divisions(spark: SparkSession) -> DataFrame:
    """One row per CNAE division (2-digit prefix) with its section letter and name."""
    rows = [
        (f"{division:02d}", letter, name)
        for letter, first, last, name in CNAE_SECOES
        for division in range(first, last + 1)
    ]
    schema = StructType(
        [
            StructField("cnae_divisao", StringType(), False),
            StructField("cnae_secao", StringType(), False),
            StructField("cnae_secao_descricao", StringType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def eligible_establishments(
    estabelecimentos: DataFrame, include_invalid_cnpj: bool = True
) -> DataFrame:
    """The silver establishments counted by gold, projected to the columns gold needs."""
    df = estabelecimentos.select(*ESTABLISHMENT_COLUMNS)
    if not include_invalid_cnpj:
        df = df.where(F.coalesce(F.col(CNPJ_VALID_COLUMN), F.lit(False)))
    return df.drop(CNPJ_VALID_COLUMN)


def empresas_cnae_uf_ano(establishments: DataFrame, cnaes: DataFrame) -> DataFrame:
    """Establishments by main CNAE, UF and opening year (``gold_empresas_cnae_uf_ano``).

    Measures: ``estabelecimentos`` (all), ``estabelecimentos_ativos`` (status 02) and
    ``empresas`` (distinct ``cnpj_basico`` in the row; not additive across rows).
    """
    lookup = F.broadcast(
        cnaes.select(
            F.col("codigo").alias("cnae_fiscal_principal"),
            F.col("descricao").alias("cnae_descricao"),
        )
    )
    return (
        establishments.withColumn("ano_inicio_atividade", F.year("data_inicio_atividade"))
        .join(lookup, "cnae_fiscal_principal", "left")
        .groupBy("cnae_fiscal_principal", "cnae_descricao", "uf", "ano_inicio_atividade")
        .agg(
            F.count(F.lit(1)).alias("estabelecimentos"),
            F.sum(_active()).cast(LongType()).alias("estabelecimentos_ativos"),
            F.countDistinct("cnpj_basico").alias("empresas"),
        )
    )


def distribuicao_porte(empresas: DataFrame, establishments: DataFrame | None) -> DataFrame:
    """Companies by size (``gold_distribuicao_porte``), with their share of all companies.

    Measures: ``empresas``, ``participacao`` (share, sums to 1) and, when establishments are
    available, ``estabelecimentos``, ``estabelecimentos_ativos`` and ``empresas_ativas``
    (companies with at least one active establishment), joined by ``cnpj_basico``; null
    otherwise. Establishments whose company is not in silver are not counted here (the run
    summary reports them as ``establishments_without_company``).
    """
    spark = empresas.sparkSession
    companies = empresas.select("cnpj_basico", "porte_empresa")
    if establishments is not None:
        per_company = establishments.groupBy("cnpj_basico").agg(
            F.count(F.lit(1)).alias("_estabelecimentos"),
            F.sum(_active()).cast(LongType()).alias("_ativos"),
        )
        companies = companies.join(per_company, "cnpj_basico", "left")
        measures = [
            F.coalesce(F.sum("_estabelecimentos"), F.lit(0)).cast(LongType()),
            F.coalesce(F.sum("_ativos"), F.lit(0)).cast(LongType()),
            F.sum(F.when(F.col("_ativos") > 0, 1).otherwise(0)).cast(LongType()),
        ]
    else:
        measures = [F.lit(None).cast(LongType())] * 3
    names = ("estabelecimentos", "estabelecimentos_ativos", "empresas_ativas")
    aggregated = companies.groupBy("porte_empresa").agg(
        F.count(F.lit(1)).alias("empresas"),
        *(m.alias(n) for m, n in zip(measures, names, strict=True)),
    )
    lookup = F.broadcast(domain_lookup(spark, PORTE_DESCRICAO, "porte_empresa", "porte_descricao"))
    total = Window.partitionBy(F.lit(1))  # a handful of rows: one partition is fine
    return (
        aggregated.join(lookup, "porte_empresa", "left")
        .withColumn("participacao", F.col("empresas") / F.sum("empresas").over(total))
        .select("porte_empresa", "porte_descricao", "empresas", "participacao", *names)
    )


def situacao_setor(establishments: DataFrame) -> DataFrame:
    """Establishments by registration status and CNAE section/division (``gold_situacao_setor``).

    The division is the 2-digit prefix of the main CNAE; its section comes from the CNAE
    structure. Measures: ``estabelecimentos`` and ``empresas`` (distinct, not additive).
    """
    spark = establishments.sparkSession
    divisions = F.broadcast(cnae_divisions(spark))
    status = F.broadcast(
        domain_lookup(spark, SITUACAO_DESCRICAO, "situacao_cadastral", "situacao_descricao")
    )
    return (
        establishments.withColumn("cnae_divisao", F.substring("cnae_fiscal_principal", 1, 2))
        .join(divisions, "cnae_divisao", "left")
        .join(status, "situacao_cadastral", "left")
        .groupBy(
            "situacao_cadastral",
            "situacao_descricao",
            "cnae_secao",
            "cnae_secao_descricao",
            "cnae_divisao",
        )
        .agg(
            F.count(F.lit(1)).alias("estabelecimentos"),
            F.countDistinct("cnpj_basico").alias("empresas"),
        )
    )


# --- reconciliation ----------------------------------------------------------------------------


@dataclass(frozen=True)
class Check:
    """One sum check: a gold total against the count of eligible silver rows."""

    name: str
    expected: float
    actual: float

    @property
    def passed(self) -> bool:
        return abs(self.expected - self.actual) < 1e-9

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "passed": self.passed}


def _total(df: DataFrame, column: str) -> float:
    value = df.agg(F.sum(column)).first()
    return float(value[0] or 0) if value else 0.0


@dataclass(frozen=True)
class SilverCounts:
    """Silver rows behind the gold tables (counts only, RST-01)."""

    empresas: int | None = None
    estabelecimentos: int | None = None
    invalid_cnpj: int | None = None
    eligible: int | None = None
    eligible_active: int | None = None
    without_company: int | None = None


def reconcile(gold: Mapping[str, DataFrame], counts: SilverCounts) -> list[Check]:
    """Sum checks: every gold total equals the number of eligible silver rows it counts."""
    checks: list[Check] = []
    for table in (CNAE_UF_ANO, SITUACAO_SETOR):
        if table in gold and counts.eligible is not None:
            checks.append(
                Check(
                    f"{table}.estabelecimentos",
                    counts.eligible,
                    _total(gold[table], "estabelecimentos"),
                )
            )
    if CNAE_UF_ANO in gold and counts.eligible_active is not None:
        checks.append(
            Check(
                f"{CNAE_UF_ANO}.estabelecimentos_ativos",
                counts.eligible_active,
                _total(gold[CNAE_UF_ANO], "estabelecimentos_ativos"),
            )
        )
    if DISTRIBUICAO_PORTE in gold and counts.empresas is not None:
        porte = gold[DISTRIBUICAO_PORTE]
        checks.append(
            Check(f"{DISTRIBUICAO_PORTE}.empresas", counts.empresas, _total(porte, "empresas"))
        )
        checks.append(
            Check(
                f"{DISTRIBUICAO_PORTE}.participacao",
                1.0 if counts.empresas else 0.0,
                _total(porte, "participacao"),
            )
        )
        if counts.eligible is not None and counts.without_company is not None:
            checks.append(
                Check(
                    f"{DISTRIBUICAO_PORTE}.estabelecimentos",
                    counts.eligible - counts.without_company,
                    _total(porte, "estabelecimentos"),
                )
            )
    return checks


# --- I/O ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldReport:
    """Result of one gold run: row counts per table, silver counts and sum checks."""

    competencia: str
    include_invalid_cnpj: bool
    tables: dict[str, int]
    paths: dict[str, str]
    skipped: dict[str, str]
    silver: SilverCounts
    checks: tuple[Check, ...] = field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "competencia": self.competencia,
            "include_invalid_cnpj": self.include_invalid_cnpj,
            "tables": [
                {"table": t, "rows": n, "path": self.paths[t]} for t, n in self.tables.items()
            ],
            "skipped": self.skipped,
            "silver": asdict(self.silver),
            "checks": [c.to_dict() for c in self.checks],
            "passed": self.passed,
        }

    def to_markdown(self) -> str:
        verdict = "all sum checks pass" if self.passed else "**SUM CHECKS FAILED**"
        lines = [
            f"### Gold (RF-05) after {self.competencia}: {verdict}",
            "",
            "| Table | Rows |",
            "|---|---:|",
            *(f"| `{t}` | {n:,} |" for t, n in self.tables.items()),
            *(f"| `{t}` | skipped: {why} |" for t, why in self.skipped.items()),
            "",
            "| Check | Expected (silver) | Actual (gold) | Status |",
            "|---|---:|---:|---|",
            *(
                f"| {c.name} | {_number(c.expected)} | {_number(c.actual)}"
                f" | {'pass' if c.passed else '**FAIL**'} |"
                for c in self.checks
            ),
        ]
        return "\n".join(lines) + "\n"


def _number(value: float) -> str:
    return f"{int(value):,}" if float(value).is_integer() else f"{value:.6f}"


def _read(spark: SparkSession, path: str) -> DataFrame | None:
    """The whole silver table (current state, ADR 011), or None when it does not exist."""
    try:
        return spark.read.format("delta").load(path)
    except AnalysisException:
        return None


def write_gold(df: DataFrame, path: str) -> None:
    """Whole-table Delta overwrite: gold is derived from silver, so a re-run replaces it."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def _empty_cnaes(spark: SparkSession) -> DataFrame:
    return domain_lookup(spark, {}, "codigo", "descricao")


def run(
    spark: SparkSession,
    competencia: str,
    silver_root: str,
    gold_root: str,
    include_invalid_cnpj: bool = True,
) -> GoldReport:
    """Builds and writes every gold table the available silver tables allow.

    ``gold_distribuicao_porte`` needs silver ``empresas``; the other two need
    ``estabelecimentos`` (``cnaes`` only adds descriptions). A table whose input is missing is
    skipped with the reason; if no table can be built, `ValueError`.
    """
    competencia = parse_competencia(competencia)
    silver_root, gold_root = silver_root.rstrip("/"), gold_root.rstrip("/")
    empresas = _read(spark, f"{silver_root}/empresas")
    estabelecimentos = _read(spark, f"{silver_root}/estabelecimentos")
    if empresas is None and estabelecimentos is None:
        raise ValueError("no silver empresas nor estabelecimentos; run `make silver` first")

    frames: dict[str, DataFrame] = {}
    skipped: dict[str, str] = {}
    counts = SilverCounts()
    eligible: DataFrame | None = None
    if estabelecimentos is not None:
        eligible = eligible_establishments(estabelecimentos, include_invalid_cnpj).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        cnaes = _read(spark, f"{silver_root}/cnaes")
        if cnaes is None:
            cnaes = _empty_cnaes(spark)
        frames[CNAE_UF_ANO] = empresas_cnae_uf_ano(eligible, cnaes)
        frames[SITUACAO_SETOR] = situacao_setor(eligible)
        flags = estabelecimentos.agg(
            F.count(F.lit(1)), F.sum(F.when(~F.col(CNPJ_VALID_COLUMN), 1).otherwise(0))
        ).first()
        active = eligible.agg(F.count(F.lit(1)), F.sum(_active())).first()
        counts = SilverCounts(
            estabelecimentos=int(flags[0]) if flags else 0,
            invalid_cnpj=int(flags[1] or 0) if flags else 0,
            eligible=int(active[0]) if active else 0,
            eligible_active=int(active[1] or 0) if active else 0,
        )
    else:
        reason = "no silver estabelecimentos"
        skipped = {CNAE_UF_ANO: reason, SITUACAO_SETOR: reason}
    if empresas is not None:
        frames[DISTRIBUICAO_PORTE] = distribuicao_porte(empresas, eligible)
        without_company = (
            eligible.join(empresas.select("cnpj_basico"), "cnpj_basico", "left_anti").count()
            if eligible is not None
            else None
        )
        counts = SilverCounts(
            **{**asdict(counts), "empresas": empresas.count(), "without_company": without_company}
        )
    else:
        skipped[DISTRIBUICAO_PORTE] = "no silver empresas"

    paths = {name: f"{gold_root}/{name}" for name in GOLD_TABLES if name in frames}
    written: dict[str, DataFrame] = {}
    for name in GOLD_TABLES:
        if name in frames:
            write_gold(frames[name], paths[name])
            written[name] = spark.read.format("delta").load(paths[name])
    if eligible is not None:
        eligible.unpersist()
    tables = {name: df.count() for name, df in written.items()}
    checks = tuple(reconcile(written, counts))
    log.info("gold %s: tables %s, skipped %s", competencia, tables, skipped)
    return GoldReport(competencia, include_invalid_cnpj, tables, paths, skipped, counts, checks)


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.gold",
        description="RF-05: gold aggregations (CNAE/UF/year, size, status) from silver.",
    )
    parser.add_argument(
        "--competencia", required=True, help="reference month of the last silver load, YYYY-MM"
    )
    parser.add_argument("--silver", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/silver")
    parser.add_argument("--output", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/gold")
    parser.add_argument(
        "--exclude-invalid-cnpj",
        action="store_true",
        help="leave establishments with an invalid CNPJ out (default: counted, ADR 012)",
    )
    parser.add_argument(
        "--summary", type=Path, default=None, help="JSON run summary (a .md is written next to it)"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: 0 when every sum check passes, 1 otherwise (or on a usage error)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark(f"gold-{args.competencia}", cfg)
    try:
        report = run(
            spark,
            args.competencia,
            args.silver or cfg.path(SILVER_LAYER),
            args.output or cfg.path(GOLD_LAYER),
            include_invalid_cnpj=not args.exclude_invalid_cnpj,
        )
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8")
        args.summary.with_suffix(".md").write_text(report.to_markdown(), encoding="utf-8")
    for name, rows in report.tables.items():
        print(f"[gold] {name}: {rows} rows")
    for name, why in report.skipped.items():
        print(f"[gold] {name}: skipped ({why})")
    for c in report.checks:
        print(f"[gold] check {c.name}: expected {c.expected}, actual {c.actual}, passed {c.passed}")
    return 0 if report.passed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
