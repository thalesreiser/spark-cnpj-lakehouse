"""RF-05 / task 5.2: silver x gold reconciliation, recomputed independently from silver.

`gold.run` already checks its own sums (ADR 012, decision 4), but with its own code. This module
is the independent proof: it recomputes every gold measure **straight from the silver tables**,
with its own expressions (it never calls the aggregation functions of `cnpj_lakehouse.gold`),
and compares it with what the gold Delta tables hold, at several levels:

* **grand total** of every additive measure (``estabelecimentos``, ``estabelecimentos_ativos``,
  company ``empresas``, ``participacao`` = 1, ...);
* **by UF** (``gold_empresas_cnae_uf_ano``), **by registration status** (``gold_situacao_setor``)
  and **by size** (``gold_distribuicao_porte``): the gold rows summed per group against the silver
  rows counted per group;
* **at the gold grain** (one gold row per group), every measure, including the non-additive
  ``empresas`` of the establishment tables, which is compared with a ``countDistinct`` of
  ``cnpj_basico`` on silver at the same grain and never summed across rows.

The counting rules are those of ADR 012: establishments with an invalid CNPJ are counted unless
``include_invalid_cnpj=False`` (it must match the gold run), a null code or date is a group of
its own (groups are matched null-safely), and establishments whose company is not in silver are
left out of the size table.

A comparison unions the silver and gold values per group and sums each side, so a group missing
on either side, a duplicated gold row or a changed value all count as a mismatching group. The
report (``reconcile.json`` / ``reconcile.md``) holds counts per group only (codes such as UF,
size or status, never rows, RST-01); the CLI exits with code 1 on any mismatch.

Usage (inside the ``spark-client`` container, see ``make reconcile``)::

    python -m cnpj_lakehouse.reconcile --competencia 2026-09 [--out build/reconcile]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from cnpj_lakehouse.download import parse_competencia
from cnpj_lakehouse.gold import CNAE_UF_ANO, DISTRIBUICAO_PORTE, GOLD_LAYER, SITUACAO_SETOR
from cnpj_lakehouse.silver import CNPJ_VALID_COLUMN, SILVER_LAYER
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger("cnpj_lakehouse.reconcile")

TOLERANCE = 1e-9  # counts are exact; only the size shares are fractions
MAX_LISTED = 50  # groups listed per comparison in the report
MAX_MISMATCHES_LISTED = 10  # mismatching groups listed for a comparison at the gold grain
TOTAL = "total"
GRAIN = "gold grain"
ACTIVE_STATUS = "02"

Key = tuple[Any, ...]


@dataclass(frozen=True)
class Group:
    """One group of a comparison: its key values and the silver and gold numbers."""

    key: Key
    expected: float
    actual: float

    @property
    def passed(self) -> bool:
        return abs(self.expected - self.actual) <= TOLERANCE

    def to_dict(self) -> dict[str, Any]:
        return {"key": list(self.key), "expected": self.expected, "actual": self.actual}


@dataclass(frozen=True)
class Comparison:
    """One measure of one gold table at one level, silver against gold."""

    table: str
    level: str
    keys: tuple[str, ...]
    measure: str
    groups: int
    mismatches: int
    expected_total: float | None  # None for a non-additive measure (distinct counts)
    actual_total: float | None
    listed: tuple[Group, ...] = ()  # every group of a small level, or the first mismatches

    @property
    def passed(self) -> bool:
        return self.mismatches == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "level": self.level,
            "keys": list(self.keys),
            "measure": self.measure,
            "groups": self.groups,
            "mismatches": self.mismatches,
            "expected_total": self.expected_total,
            "actual_total": self.actual_total,
            "listed": [g.to_dict() for g in self.listed],
            "passed": self.passed,
        }


def _side(df: DataFrame, keys: Sequence[str], measure: str, own: str, other: str) -> DataFrame:
    return df.select(
        *keys,
        F.col(measure).cast(DoubleType()).alias(own),
        F.lit(None).cast(DoubleType()).alias(other),
    )


def compare(
    table: str,
    level: str,
    keys: Sequence[str],
    measure: str,
    expected: DataFrame,
    actual: DataFrame,
    additive: bool = True,
    list_all: bool = True,
) -> Comparison:
    """Sums `measure` per `keys` on each side and counts the groups whose sums differ.

    `expected` (silver) and `actual` (gold) may be at a finer grain than `keys`: their rows are
    summed per group, so this is the check "gold rows summed by UF == silver rows counted by UF".
    Null keys form one group on both sides (``groupBy`` is null-safe). A group present on one
    side only counts as 0 on the other. With `list_all` every group is listed (up to
    `MAX_LISTED`); otherwise only the first mismatching ones.
    """
    keys = tuple(keys)
    both = _side(expected, keys, measure, "_e", "_a").unionByName(
        _side(actual, keys, measure, "_a", "_e")
    )
    groups = (
        both.groupBy(*keys)
        .agg(
            F.coalesce(F.sum("_e"), F.lit(0.0)).alias("_expected"),
            F.coalesce(F.sum("_a"), F.lit(0.0)).alias("_actual"),
        )
        .withColumn("_bad", F.abs(F.col("_expected") - F.col("_actual")) > TOLERANCE)
    )
    stats = groups.agg(
        F.count(F.lit(1)),
        F.coalesce(F.sum(F.col("_bad").cast("int")), F.lit(0)),
        F.sum("_expected"),
        F.sum("_actual"),
    ).first()
    n_groups, mismatches = (int(stats[0]), int(stats[1])) if stats else (0, 0)
    shown = groups if list_all else groups.where("_bad")
    limit = MAX_LISTED if list_all else MAX_MISMATCHES_LISTED
    listed = tuple(
        Group(tuple(r[k] for k in keys), float(r["_expected"]), float(r["_actual"]))
        for r in (shown.orderBy(*keys) if keys else shown).limit(limit).collect()
    )
    expected_total = float(stats[2] or 0) if stats and additive else None
    actual_total = float(stats[3] or 0) if stats and additive else None
    return Comparison(
        table, level, keys, measure, n_groups, mismatches, expected_total, actual_total, listed
    )


# --- independent silver computations -----------------------------------------------------------


def _active(column: str = "situacao_cadastral") -> Column:
    return F.coalesce((F.col(column) == ACTIVE_STATUS).cast("int"), F.lit(0))


def silver_establishments(estabelecimentos: DataFrame, include_invalid_cnpj: bool) -> DataFrame:
    """Silver establishments counted by gold (ADR 012), with the derived grouping columns."""
    df = estabelecimentos
    if not include_invalid_cnpj:
        df = df.where(F.col(CNPJ_VALID_COLUMN).eqNullSafe(F.lit(True)))
    return df.select(
        "cnpj_basico",
        "cnae_fiscal_principal",
        "uf",
        "situacao_cadastral",
        F.year("data_inicio_atividade").alias("ano_inicio_atividade"),
        F.expr("left(cnae_fiscal_principal, 2)").alias("cnae_divisao"),
        _active().alias("_ativo"),
    )


def _per_group(establishments: DataFrame, grain: Sequence[str]) -> DataFrame:
    return establishments.groupBy(*grain).agg(
        F.count(F.lit(1)).alias("estabelecimentos"),
        F.sum("_ativo").alias("estabelecimentos_ativos"),
        F.countDistinct("cnpj_basico").alias("empresas"),
    )


@dataclass(frozen=True)
class Plan:
    """What to compare for one gold table: the silver numbers at the gold grain and the levels."""

    table: str
    grain: tuple[str, ...]
    expected: Mapping[str, DataFrame]  # measure -> silver numbers at the gold grain
    levels: tuple[tuple[str, tuple[str, ...]], ...]  # (name, keys) of the additive roll-ups
    non_additive: tuple[str, ...] = ()  # measures compared at the gold grain only
    small_grain: bool = False  # list every group of the grain level (a handful of rows)


def establishment_plans(establishments: DataFrame) -> list[Plan]:
    """Plans for ``gold_empresas_cnae_uf_ano`` (by UF) and ``gold_situacao_setor`` (by status)."""
    plans = []
    for table, grain, level, measures in (
        (
            CNAE_UF_ANO,
            ("cnae_fiscal_principal", "uf", "ano_inicio_atividade"),
            ("uf", ("uf",)),
            ("estabelecimentos", "estabelecimentos_ativos"),
        ),
        (
            SITUACAO_SETOR,
            ("situacao_cadastral", "cnae_divisao"),
            ("situacao_cadastral", ("situacao_cadastral",)),
            ("estabelecimentos",),
        ),
    ):
        expected = _per_group(establishments, grain)
        plans.append(
            Plan(
                table,
                grain,
                {m: expected for m in (*measures, "empresas")},
                ((TOTAL, ()), level),
                non_additive=("empresas",),
            )
        )
    return plans


def porte_plan(empresas: DataFrame, establishments: DataFrame | None) -> Plan:
    """Plan for ``gold_distribuicao_porte``: companies (and their establishments) by size."""
    companies = empresas.select("cnpj_basico", "porte_empresa")
    total = companies.count()
    by_size = companies.groupBy("porte_empresa").agg(F.count(F.lit(1)).alias("empresas"))
    expected = {
        "empresas": by_size,
        "participacao": by_size.select(
            "porte_empresa", (F.col("empresas") / F.lit(max(total, 1))).alias("participacao")
        ),
    }
    if establishments is not None:
        # inner join: an establishment whose company is not in silver is not counted (ADR 012)
        joined = establishments.join(companies, "cnpj_basico")
        per_size = joined.groupBy("porte_empresa").agg(
            F.count(F.lit(1)).alias("estabelecimentos"),
            F.sum("_ativo").alias("estabelecimentos_ativos"),
            F.countDistinct(F.when(F.col("_ativo") == 1, F.col("cnpj_basico"))).alias(
                "empresas_ativas"
            ),
        )
        for measure in ("estabelecimentos", "estabelecimentos_ativos", "empresas_ativas"):
            expected[measure] = per_size
    return Plan(DISTRIBUICAO_PORTE, ("porte_empresa",), expected, ((TOTAL, ()),), small_grain=True)


def run_plan(plan: Plan, gold: DataFrame) -> list[Comparison]:
    """Every additive measure at every level and at the grain; non-additive ones at the grain."""
    comparisons = []
    grain_name = plan.grain[0] if plan.small_grain else GRAIN
    for measure, expected in plan.expected.items():
        additive = measure not in plan.non_additive
        levels = (*plan.levels, (grain_name, plan.grain)) if additive else ((GRAIN, plan.grain),)
        for name, keys in levels:
            comparisons.append(
                compare(
                    plan.table,
                    name,
                    keys,
                    measure,
                    expected,
                    gold,
                    additive=additive,
                    list_all=name != GRAIN,
                )
            )
    return comparisons


def reconcile_frames(
    gold: Mapping[str, DataFrame],
    empresas: DataFrame | None,
    estabelecimentos: DataFrame | None,
    include_invalid_cnpj: bool = True,
) -> tuple[list[Comparison], dict[str, str], list[str]]:
    """Compares every gold table with silver; returns (comparisons, skipped, errors).

    A gold table is skipped when neither it nor its silver input exists; it is an error when
    only one of the two exists (a gold table without silver, or silver without its gold table).
    """
    comparisons: list[Comparison] = []
    skipped: dict[str, str] = {}
    errors: list[str] = []
    establishments = (
        silver_establishments(estabelecimentos, include_invalid_cnpj)
        if estabelecimentos is not None
        else None
    )
    plans: dict[str, Plan | None] = {CNAE_UF_ANO: None, SITUACAO_SETOR: None}
    if establishments is not None:
        plans.update({p.table: p for p in establishment_plans(establishments)})
    plans[DISTRIBUICAO_PORTE] = (
        porte_plan(empresas, establishments) if empresas is not None else None
    )
    inputs = {
        CNAE_UF_ANO: "estabelecimentos",
        SITUACAO_SETOR: "estabelecimentos",
        DISTRIBUICAO_PORTE: "empresas",
    }
    for table, plan in plans.items():
        frame = gold.get(table)
        if plan is None and frame is None:
            skipped[table] = f"no silver {inputs[table]} nor gold table"
        elif plan is None:
            errors.append(f"{table} exists but silver {inputs[table]} does not")
        elif frame is None:
            errors.append(f"{table} is missing although silver {inputs[table]} exists")
        else:
            comparisons.extend(run_plan(plan, frame))
    return comparisons, skipped, errors


# --- report ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReconcileReport:
    """Result of one reconciliation: every comparison, skipped tables and errors."""

    competencia: str
    include_invalid_cnpj: bool
    comparisons: tuple[Comparison, ...]
    skipped: dict[str, str] = field(default_factory=dict)
    errors: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return (
            bool(self.comparisons) and not self.errors and all(c.passed for c in self.comparisons)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "competencia": self.competencia,
            "include_invalid_cnpj": self.include_invalid_cnpj,
            "tables": sorted({c.table for c in self.comparisons}),
            "skipped": self.skipped,
            "errors": list(self.errors),
            "comparisons": [c.to_dict() for c in self.comparisons],
            "passed": self.passed,
        }

    def to_markdown(self) -> str:
        failed = sum(not c.passed for c in self.comparisons)
        verdict = (
            f"all {len(self.comparisons)} comparisons match"
            if self.passed
            else f"**RECONCILIATION FAILED** ({failed} comparisons, {len(self.errors)} errors)"
        )
        lines = [
            f"### Silver x gold reconciliation (RF-05) after {self.competencia}: {verdict}",
            "",
            f"Invalid CNPJs counted: {'yes' if self.include_invalid_cnpj else 'no'}."
            " Silver recomputed independently of the gold code; counts only.",
            "",
            *(f"- **error:** {e}" for e in self.errors),
            *(f"- `{t}` skipped: {why}" for t, why in self.skipped.items()),
            "",
            "| Table | Level | Measure | Groups | Mismatching | Silver | Gold | Status |",
            "|---|---|---|---:|---:|---:|---:|---|",
        ]
        for c in self.comparisons:
            totals = (
                (_number(c.expected_total), _number(c.actual_total))
                if c.expected_total is not None and c.actual_total is not None
                else ("non-additive", "non-additive")
            )
            lines.append(
                f"| `{c.table}` | {c.level} | {c.measure} | {c.groups:,} | {c.mismatches:,}"
                f" | {totals[0]} | {totals[1]} | {'pass' if c.passed else '**FAIL**'} |"
            )
        lines += _group_tables(self.comparisons)
        return "\n".join(lines) + "\n"


def _key(group: Group) -> str:
    return ", ".join("null" if v is None else str(v) for v in group.key) or "-"


def _group_tables(comparisons: Sequence[Comparison]) -> list[str]:
    """Per-group numbers of the breakdowns (by UF, status, size) and the mismatching groups."""
    lines: list[str] = []
    for c in comparisons:
        if c.level in (TOTAL, GRAIN) and c.passed:
            continue
        rows = c.listed if c.level != GRAIN else c.listed[:MAX_MISMATCHES_LISTED]
        if not rows:
            continue
        lines += [
            "",
            f"`{c.table}` {c.measure} by {', '.join(c.keys) or 'total'}"
            + (" (mismatching groups)" if c.level == GRAIN else ""),
            "",
            "| Group | Silver | Gold | Status |",
            "|---|---:|---:|---|",
            *(
                f"| {_key(g)} | {_number(g.expected)} | {_number(g.actual)}"
                f" | {'pass' if g.passed else '**FAIL**'} |"
                for g in rows
            ),
        ]
    return lines


def _number(value: float) -> str:
    return f"{int(value):,}" if float(value).is_integer() else f"{value:.6f}"


# --- I/O ---------------------------------------------------------------------------------------


def _read(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.format("delta").load(path)
    except AnalysisException:
        return None


def run(
    spark: SparkSession,
    competencia: str,
    silver_root: str,
    gold_root: str,
    include_invalid_cnpj: bool = True,
) -> ReconcileReport:
    """Reads the silver and gold Delta tables and reconciles every gold table with silver."""
    competencia = parse_competencia(competencia)
    silver_root, gold_root = silver_root.rstrip("/"), gold_root.rstrip("/")
    gold = {
        name: df
        for name in (CNAE_UF_ANO, DISTRIBUICAO_PORTE, SITUACAO_SETOR)
        if (df := _read(spark, f"{gold_root}/{name}")) is not None
    }
    comparisons, skipped, errors = reconcile_frames(
        gold,
        _read(spark, f"{silver_root}/empresas"),
        _read(spark, f"{silver_root}/estabelecimentos"),
        include_invalid_cnpj,
    )
    if not comparisons and not errors:
        errors.append("nothing to reconcile: no gold table; run `make gold` first")
    log.info(
        "reconcile %s: %d comparisons, %d failed, errors %s",
        competencia,
        len(comparisons),
        sum(not c.passed for c in comparisons),
        errors,
    )
    return ReconcileReport(
        competencia, include_invalid_cnpj, tuple(comparisons), skipped, tuple(errors)
    )


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.reconcile",
        description="RF-05: reconciles the gold tables with an independent silver computation.",
    )
    parser.add_argument("--competencia", required=True, help="label of the report, YYYY-MM")
    parser.add_argument("--silver", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/silver")
    parser.add_argument("--gold", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/gold")
    parser.add_argument(
        "--exclude-invalid-cnpj",
        action="store_true",
        help="the gold run left invalid CNPJs out (must match `make gold`)",
    )
    parser.add_argument(
        "--out", type=Path, default=Path("build/reconcile"), help="reconcile.json / .md"
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: 0 when silver and gold match everywhere, 1 otherwise."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark(f"reconcile-{args.competencia}", cfg)
    try:
        report = run(
            spark,
            args.competencia,
            args.silver or cfg.path(SILVER_LAYER),
            args.gold or cfg.path(GOLD_LAYER),
            include_invalid_cnpj=not args.exclude_invalid_cnpj,
        )
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "reconcile.json").write_text(
        json.dumps(report.to_dict(), indent=2) + "\n", encoding="utf-8"
    )
    (args.out / "reconcile.md").write_text(report.to_markdown(), encoding="utf-8")
    for c in report.comparisons:
        print(
            f"[reconcile] {c.table} {c.level} {c.measure}: {c.groups} groups,"
            f" {c.mismatches} mismatching"
        )
    for error in report.errors:
        print(f"[reconcile] error: {error}")
    return 0 if report.passed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
