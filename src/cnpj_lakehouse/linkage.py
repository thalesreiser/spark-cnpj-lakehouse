"""RF-10: simplified record linkage of partners (``socios``) with similar names (ADR 014).

A **technical** exercise (RST-01, ``USAGE-POLICY.md``): "do two partner rows with similar names
and the same masked CPF digits look like the same record?", never "who is this person". Nothing
here unmasks a CPF or joins with another dataset.

Method:

1. **Normalization** (`normalize_name`): upper case, accents stripped, anything but letters
   becomes a space, particles (``DA DE DO DAS DOS E D``) dropped, spaces collapsed; optionally
   the tokens are sorted (``sort_tokens``) so word order does not matter.
2. **Blocking** (`block_key`): first token + last token of the normalized name + the six middle
   digits of the masked CPF (``***123456**``) that the source already publishes. Pairs are only
   formed inside a block (self-join on the key, never a cartesian product); blocks larger than
   ``max_block_size`` are skipped and counted.
3. **Score**: Jaro-Winkler on the normalized names (`jaro_winkler`, pure Python, applied as a
   UDF: no new dependency) plus the Levenshtein ratio from Spark's ``levenshtein``.
4. **Bands** (`Thresholds`): ``match`` (score >= 0.92), ``review`` (0.85 <= score < 0.92),
   ``non_match`` below; both thresholds are configurable.

Outputs (privacy rules in ADR 014 and ``docs/linkage.md``):

* the Delta table ``gold/linkage_candidates`` in the local lakehouse: row keys (hashes),
  ``cnpj_basico`` of each side, block id (hash), block size, scores and band; **no names**;
* ``build/linkage/linkage-<competencia>.json|md``: aggregates only (blocks, pairs, bands, score
  histogram), safe to publish;
* ``--review-sample N``: N random pairs of a band **with names**, written only below the
  git-ignored ``build/linkage-review/`` (`write_review_sample` refuses any other path), for
  Thales to label locally; ``precision`` turns the labels into an aggregate precision.

Usage (inside the ``spark-client`` container, see ``make linkage``)::

    python -m cnpj_lakehouse.linkage candidates --competencia 2026-09 [--review-sample 50]
    python -m cnpj_lakehouse.linkage precision --review build/linkage-review/review-2026-09.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from cnpj_lakehouse.gold import GOLD_LAYER
from cnpj_lakehouse.normalize import strip_accents
from cnpj_lakehouse.silver import SILVER_LAYER, SOCIOS_KEY
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

log = logging.getLogger("cnpj_lakehouse.linkage")

CANDIDATES_TABLE = "linkage_candidates"
INDIVIDUAL = "2"  # identificador_socio of a natural person
PARTICLES = ("DA", "DE", "DO", "DAS", "DOS", "E", "D")
# Masked CPF as published by the source: three asterisks, six digits, two asterisks.
MASKED_CPF = r"^\*{3}(\d{6})\*{2}$"
DEFAULT_MAX_BLOCK_SIZE = 200
HISTOGRAM_WIDTH = 0.05
BANDS = ("match", "review", "non_match")
# The only place a review sample with names may be written (git-ignored through `build/`).
REVIEW_DIR = Path("build") / "linkage-review"
LABELS = {"match": True, "no_match": False}


# --- similarity (pure Python) ------------------------------------------------------------------


def jaro(a: str, b: str) -> float:
    """Jaro similarity in [0, 1]: matching characters within a window, minus transpositions."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    window = max(max(len(a), len(b)) // 2 - 1, 0)
    a_hits = [False] * len(a)
    b_hits = [False] * len(b)
    matches = 0
    for i, char in enumerate(a):
        for j in range(max(0, i - window), min(len(b), i + window + 1)):
            if not b_hits[j] and b[j] == char:
                a_hits[i] = b_hits[j] = True
                matches += 1
                break
    if matches == 0:
        return 0.0
    b_matched = [c for c, hit in zip(b, b_hits, strict=True) if hit]
    a_matched = [c for c, hit in zip(a, a_hits, strict=True) if hit]
    transpositions = sum(x != y for x, y in zip(a_matched, b_matched, strict=True)) / 2
    return (matches / len(a) + matches / len(b) + (matches - transpositions) / matches) / 3


def jaro_winkler(a: str | None, b: str | None, prefix_scale: float = 0.1) -> float | None:
    """Jaro-Winkler similarity: Jaro boosted by the common prefix (up to 4 characters).

    ``None`` when either side is ``None`` (a null name never matches).
    """
    if a is None or b is None:
        return None
    score = jaro(a, b)
    prefix = 0
    for x, y in zip(a[:4], b[:4], strict=False):
        if x != y:
            break
        prefix += 1
    return score + prefix * prefix_scale * (1 - score)


@dataclass(frozen=True)
class Thresholds:
    """Score bands: ``match`` >= match; ``review`` in [review, match); ``non_match`` below."""

    match: float = 0.92
    review: float = 0.85

    def __post_init__(self) -> None:
        if not 0.0 <= self.review <= self.match <= 1.0:
            raise ValueError(
                "thresholds must satisfy 0 <= review <= match <= 1,"
                f" got {self.review}, {self.match}"
            )

    def band(self, score: float | None) -> str:
        """Band of one score (the Spark column in `band_column` follows the same rule)."""
        if score is not None and score >= self.match:
            return "match"
        if score is not None and score >= self.review:
            return "review"
        return "non_match"

    def band_column(self, score: Column) -> Column:
        return (
            F.when(score >= self.match, "match")
            .when(score >= self.review, "review")
            .otherwise("non_match")
        )


DEFAULT_THRESHOLDS = Thresholds()


# --- normalization and blocking (Spark columns) ------------------------------------------------


def normalize_name(column: str | Column, sort_tokens: bool = False) -> Column:
    """Comparison form of a name; ``null`` when nothing is left.

    ``" Maria  da Silva-Teste "`` -> ``"MARIA SILVA TESTE"``; with ``sort_tokens``,
    ``"MARIA SILVA TESTE"`` (tokens in alphabetical order).
    """
    col = F.col(column) if isinstance(column, str) else column
    name = F.upper(strip_accents(col))
    name = F.regexp_replace(name, r"[^A-Z]", " ")
    name = F.regexp_replace(name, r"\b(" + "|".join(PARTICLES) + r")\b", " ")
    name = F.trim(F.regexp_replace(name, r"\s+", " "))
    if sort_tokens:
        name = F.array_join(F.array_sort(F.split(name, " ")), " ")
    return F.when(name != "", name)


def masked_cpf_digits(column: str | Column) -> Column:
    """The six digits the source leaves visible in ``***123456**``; ``""`` for anything else."""
    col = F.col(column) if isinstance(column, str) else column
    return F.coalesce(F.regexp_extract(col, MASKED_CPF, 1), F.lit(""))


def block_key(normalized: Column, cpf: Column | None) -> Column:
    """First token | last token [| masked CPF digits]; ``null`` when the name is ``null``."""
    tokens = F.split(normalized, " ")
    parts = [F.element_at(tokens, 1), F.element_at(tokens, -1)]
    if cpf is not None:
        parts.append(masked_cpf_digits(cpf))
    return F.when(normalized.isNotNull(), F.concat_ws("|", *parts))


def row_id(df: DataFrame) -> Column:
    """Stable row key: SHA-256 of the silver business key of ``socios`` (no name in clear)."""
    return F.sha2(
        F.concat_ws("\u001f", *[F.coalesce(F.col(c), F.lit("")) for c in SOCIOS_KEY]), 256
    )


def prepare(
    socios: DataFrame,
    *,
    sort_tokens: bool = False,
    use_cpf: bool = True,
    partner_types: Sequence[str] = (INDIVIDUAL,),
) -> DataFrame:
    """Individual partners with ``row_id``, normalized name, ``block_key`` and ``block_size``."""
    rows = socios.where(F.col("identificador_socio").isin(*partner_types))
    rows = rows.select(
        row_id(rows).alias("row_id"),
        "cnpj_basico",
        F.col("nome_socio_razao_social").alias("nome"),
        normalize_name("nome_socio_razao_social", sort_tokens).alias("nome_normalizado"),
        F.col("cpf_cnpj_socio"),
    ).dropDuplicates(["row_id"])
    rows = rows.withColumn(
        "block_key",
        block_key(F.col("nome_normalizado"), F.col("cpf_cnpj_socio") if use_cpf else None),
    )
    return rows.withColumn("block_size", F.count("*").over(Window.partitionBy("block_key")))


_jaro_winkler_udf = F.udf(jaro_winkler, DoubleType())


def candidate_pairs(
    prepared: DataFrame,
    thresholds: Thresholds = DEFAULT_THRESHOLDS,
    max_block_size: int = DEFAULT_MAX_BLOCK_SIZE,
) -> DataFrame:
    """Every pair of distinct rows inside a block of 2..``max_block_size`` rows, scored.

    Each unordered pair appears once (``left.row_id < right.row_id``), so a row is never paired
    with itself. Names stay in the result (``nome_*``) for the local review sample only;
    `public_candidates` drops them before anything is stored.
    """
    eligible = prepared.where(
        F.col("block_key").isNotNull() & F.col("block_size").between(2, max_block_size)
    )
    side = ("row_id", "cnpj_basico", "nome", "nome_normalizado")
    left = eligible.select("block_key", "block_size", *[F.col(c).alias(f"left_{c}") for c in side])
    right = eligible.select(
        F.col("block_key").alias("_bk"), *[F.col(c).alias(f"right_{c}") for c in side]
    )
    pairs = left.join(
        right, (F.col("block_key") == F.col("_bk")) & (F.col("left_row_id") < F.col("right_row_id"))
    ).drop("_bk")
    a, b = F.col("left_nome_normalizado"), F.col("right_nome_normalizado")
    score = _jaro_winkler_udf(a, b)
    lev_ratio = 1 - F.levenshtein(a, b) / F.greatest(F.length(a), F.length(b))
    return (
        pairs.withColumn("score", F.round(score, 4))
        .withColumn("levenshtein_ratio", F.round(lev_ratio, 4))
        .withColumn("band", thresholds.band_column(F.col("score")))
    )


def public_candidates(pairs: DataFrame, competencia: str) -> DataFrame:
    """Columns stored in ``gold/linkage_candidates``: keys, scores and band, never a name."""
    return pairs.select(
        F.lit(competencia).alias("competencia"),
        F.sha2(F.col("block_key"), 256).alias("block_id"),
        "block_size",
        "left_row_id",
        "right_row_id",
        "left_cnpj_basico",
        "right_cnpj_basico",
        "score",
        "levenshtein_ratio",
        "band",
    )


# --- aggregate report (publishable) ------------------------------------------------------------


@dataclass(frozen=True)
class LinkageReport:
    """Counts and a score histogram only (RST-01): no names, CPF digits, keys or rows."""

    competencia: str
    thresholds: dict[str, float]
    max_block_size: int
    partners: int
    named: int
    blocks: int
    blocks_skipped_oversized: int
    rows_in_blocks: int
    largest_block: int
    candidate_pairs: int
    bands: dict[str, int]
    histogram: list[dict[str, Any]]
    review_sample: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_markdown(self) -> str:
        lines = [
            f"# Partner record linkage (RF-10) - {self.competencia}",
            "",
            "Aggregates only (RST-01): no names, CPF digits or row keys.",
            "",
            "| Measure | Value |",
            "|---|---|",
            f"| Individual partners | {self.partners:,} |",
            f"| With a normalized name | {self.named:,} |",
            f"| Blocks (2..{self.max_block_size} rows) | {self.blocks:,} |",
            f"| Blocks skipped (> {self.max_block_size} rows)"
            f" | {self.blocks_skipped_oversized:,} |",
            f"| Rows in blocks | {self.rows_in_blocks:,} |",
            f"| Largest block | {self.largest_block:,} |",
            f"| Candidate pairs | {self.candidate_pairs:,} |",
            *[f"| Band `{b}` | {self.bands.get(b, 0):,} |" for b in BANDS],
            f"| Review sample written locally | {self.review_sample:,} |",
            "",
            f"Thresholds: match >= {self.thresholds['match']},"
            f" review >= {self.thresholds['review']}.",
            "",
            "| Score | Pairs |",
            "|---|---|",
            *[f"| [{h['lower']:.2f}, {h['upper']:.2f}) | {h['pairs']:,} |" for h in self.histogram],
            "",
        ]
        return "\n".join(lines)


def histogram(pairs: DataFrame, width: float = HISTOGRAM_WIDTH) -> list[dict[str, Any]]:
    """Pairs per score bin of ``width`` (a score of 1.0 falls in the last bin)."""
    bins = int(round(1 / width))
    index = F.least(F.floor(F.col("score") / width), F.lit(bins - 1)).cast("int")
    rows = pairs.where(F.col("score").isNotNull()).groupBy(index.alias("bin")).count().collect()
    return [
        {
            "lower": round(r["bin"] * width, 4),
            "upper": round((r["bin"] + 1) * width, 4),
            "pairs": int(r["count"]),
        }
        for r in sorted(rows, key=lambda r: int(r["bin"]))
    ]


def report(
    competencia: str,
    prepared: DataFrame,
    pairs: DataFrame,
    thresholds: Thresholds,
    max_block_size: int,
) -> LinkageReport:
    blocks = (
        prepared.where(F.col("block_key").isNotNull())
        .groupBy("block_key")
        .agg(F.count("*").alias("n"))
        .agg(
            F.sum(F.when(F.col("n").between(2, max_block_size), 1).otherwise(0)).alias("blocks"),
            F.sum(F.when(F.col("n") > max_block_size, 1).otherwise(0)).alias("oversized"),
            F.sum(F.when(F.col("n").between(2, max_block_size), F.col("n")).otherwise(0)).alias(
                "rows"
            ),
            F.max("n").alias("largest"),
        )
        .first()
    )
    totals = prepared.agg(
        F.count("*").alias("partners"), F.count("nome_normalizado").alias("named")
    ).first()
    bands = {b: 0 for b in BANDS}
    for r in pairs.groupBy("band").count().collect():
        bands[r["band"]] = int(r["count"])
    assert blocks is not None and totals is not None
    return LinkageReport(
        competencia=competencia,
        thresholds={"match": thresholds.match, "review": thresholds.review},
        max_block_size=max_block_size,
        partners=int(totals["partners"]),
        named=int(totals["named"]),
        blocks=int(blocks["blocks"] or 0),
        blocks_skipped_oversized=int(blocks["oversized"] or 0),
        rows_in_blocks=int(blocks["rows"] or 0),
        largest_block=int(blocks["largest"] or 0),
        candidate_pairs=sum(bands.values()),
        bands=bands,
        histogram=histogram(pairs),
    )


# --- local review sample (names; never published) ----------------------------------------------

REVIEW_COLUMNS = (
    "pair",
    "score",
    "levenshtein_ratio",
    "left_nome",
    "right_nome",
    "left_nome_normalizado",
    "right_nome_normalizado",
    "label",
)


def ensure_private_path(path: Path, cwd: Path | None = None) -> Path:
    """Refuse to write names anywhere but below ``<cwd>/build/linkage-review/``.

    The directory is git-ignored (``build/``); when Git is available, ``git check-ignore`` must
    also confirm it, so a change to ``.gitignore`` cannot silently expose the sample.
    """
    base = ((cwd or Path.cwd()) / REVIEW_DIR).resolve()
    target = path.resolve()
    if target == base or base not in target.parents:
        raise ValueError(f"review samples hold names: write them only below {REVIEW_DIR}/")
    try:
        result = subprocess.run(
            ["git", "check-ignore", "-q", str(target)],
            cwd=cwd or Path.cwd(),
            capture_output=True,
            check=False,
        )
    except OSError:  # no Git in the container: the path rule above still applies
        return target
    if result.returncode == 1:
        raise ValueError(f"{path} is not ignored by Git: refusing to write names there")
    return target


def review_sample(
    pairs: DataFrame, n: int, band: str = "review", seed: int = 42
) -> list[dict[str, Any]]:
    """``n`` random pairs of ``band`` (seeded), with names, for the local manual review."""
    if band not in BANDS:
        raise ValueError(f"unknown band {band!r}; expected one of {BANDS}")
    rows = (
        pairs.where(F.col("band") == band)
        .orderBy(F.rand(seed), "left_row_id", "right_row_id")
        .limit(n)
        .collect()
    )
    return [
        {
            "pair": f"{r['left_row_id'][:12]}-{r['right_row_id'][:12]}",
            "score": r["score"],
            "levenshtein_ratio": r["levenshtein_ratio"],
            "left_nome": r["left_nome"],
            "right_nome": r["right_nome"],
            "left_nome_normalizado": r["left_nome_normalizado"],
            "right_nome_normalizado": r["right_nome_normalizado"],
            "label": "",
        }
        for r in rows
    ]


def write_review_sample(
    rows: Sequence[dict[str, Any]], path: Path, cwd: Path | None = None
) -> Path:
    """CSV (UTF-8, ``;``) with an empty ``label`` column to fill with ``match``/``no_match``."""
    target = ensure_private_path(path, cwd)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_COLUMNS, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    return target


@dataclass(frozen=True)
class Precision:
    """Aggregate result of a labeled review sample (safe to publish)."""

    sampled: int
    labeled: int
    matches: int
    precision: float | None
    ci95_low: float | None
    ci95_high: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def wilson_interval(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval of a proportion (stable for small samples)."""
    p = successes / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return round(max(0.0, centre - margin), 4), round(min(1.0, centre + margin), 4)


def precision_from_review(path: Path) -> Precision:
    """Share of labeled pairs marked ``match``; blank labels are ignored, others are an error."""
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter=";"))
    labels: list[bool] = []
    for line, row in enumerate(rows, start=2):
        label = (row.get("label") or "").strip().lower()
        if not label:
            continue
        if label not in LABELS:
            raise ValueError(f"line {line}: label must be match or no_match, got {label!r}")
        labels.append(LABELS[label])
    if not labels:
        return Precision(len(rows), 0, 0, None, None, None)
    matches = sum(labels)
    low, high = wilson_interval(matches, len(labels))
    return Precision(len(rows), len(labels), matches, round(matches / len(labels), 4), low, high)


# --- job -----------------------------------------------------------------------------------------


def run(
    spark: SparkSession,
    competencia: str,
    silver_root: str,
    gold_root: str | None,
    *,
    thresholds: Thresholds = DEFAULT_THRESHOLDS,
    max_block_size: int = DEFAULT_MAX_BLOCK_SIZE,
    sort_tokens: bool = False,
    use_cpf: bool = True,
    review_n: int = 0,
    review_band: str = "review",
    review_path: Path | None = None,
    seed: int = 42,
    input_format: str = "delta",
    output_format: str = "delta",
) -> LinkageReport:
    """Silver ``socios`` -> scored candidate pairs (gold table without names) + aggregate report."""
    target = review_path or REVIEW_DIR / f"review-{competencia}.csv"
    if review_n > 0:
        ensure_private_path(target)  # refuse before reading or collecting any name
    socios = spark.read.format(input_format).load(f"{silver_root}/socios")
    prepared = prepare(socios, sort_tokens=sort_tokens, use_cpf=use_cpf).persist()
    try:
        pairs = candidate_pairs(prepared, thresholds, max_block_size).persist()
        result = report(competencia, prepared, pairs, thresholds, max_block_size)
        if gold_root is not None:
            public_candidates(pairs, competencia).write.format(output_format).mode(
                "overwrite"
            ).option("overwriteSchema", "true").save(f"{gold_root}/{CANDIDATES_TABLE}")
        written = 0
        if review_n > 0:
            sample = review_sample(pairs, review_n, review_band, seed)
            write_review_sample(sample, target)
            written = len(sample)
            log.info("review sample of %d pairs written locally (git-ignored)", written)
        pairs.unpersist()
    finally:
        prepared.unpersist()
    return LinkageReport(**{**result.to_dict(), "review_sample": written})


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.linkage",
        description="RF-10: candidate pairs of partners with similar names (ADR 014).",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    cand = sub.add_parser("candidates", help="build gold/linkage_candidates and the report")
    cand.add_argument("--competencia", required=True, help="label of the report, YYYY-MM")
    cand.add_argument("--silver", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/silver")
    cand.add_argument("--gold", default=None, help="default: s3a://<LAKEHOUSE_BUCKET>/gold")
    cand.add_argument("--no-table", action="store_true", help="do not write the gold table")
    cand.add_argument("--match", type=float, default=Thresholds.match)
    cand.add_argument("--review", type=float, default=Thresholds.review)
    cand.add_argument("--max-block-size", type=int, default=DEFAULT_MAX_BLOCK_SIZE)
    cand.add_argument("--sort-tokens", action="store_true", help="ignore word order")
    cand.add_argument(
        "--no-cpf-block", action="store_true", help="block on the name only (more pairs)"
    )
    cand.add_argument("--review-sample", type=int, default=0, metavar="N")
    cand.add_argument("--review-band", choices=BANDS, default="review")
    cand.add_argument("--review-file", type=Path, default=None, help=f"below {REVIEW_DIR}/")
    cand.add_argument("--seed", type=int, default=42)
    cand.add_argument("--input-format", default="delta", help=argparse.SUPPRESS)
    cand.add_argument("--out", type=Path, default=Path("build/linkage"))
    prec = sub.add_parser("precision", help="aggregate precision of a labeled review sample")
    prec.add_argument("--review", type=Path, required=True, dest="review_file")
    prec.add_argument("--out", type=Path, default=Path("build/linkage"))
    return parser.parse_args(argv)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: 0 on success, 1 on invalid input."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    if args.command == "precision":
        try:
            result = precision_from_review(args.review_file)
        except (OSError, ValueError) as exc:
            log.error("%s", exc)
            return 1
        _write_json(args.out / "precision.json", result.to_dict())
        print(
            f"[linkage] precision: {result.matches}/{result.labeled} labeled pairs are matches"
            f" ({result.precision}, 95% CI {result.ci95_low}-{result.ci95_high})"
        )
        return 0
    try:
        thresholds = Thresholds(args.match, args.review)
        if args.review_sample > 0:
            ensure_private_path(args.review_file or REVIEW_DIR / "x.csv")
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark(f"linkage-{args.competencia}", cfg)
    try:
        result_report = run(
            spark,
            args.competencia,
            args.silver or cfg.path(SILVER_LAYER),
            None if args.no_table else (args.gold or cfg.path(GOLD_LAYER)),
            thresholds=thresholds,
            max_block_size=args.max_block_size,
            sort_tokens=args.sort_tokens,
            use_cpf=not args.no_cpf_block,
            review_n=args.review_sample,
            review_band=args.review_band,
            review_path=args.review_file,
            seed=args.seed,
            input_format=args.input_format,
        )
    finally:
        spark.stop()
    _write_json(args.out / f"linkage-{args.competencia}.json", result_report.to_dict())
    (args.out / f"linkage-{args.competencia}.md").write_text(
        result_report.to_markdown(), encoding="utf-8"
    )
    print(
        f"[linkage] {result_report.blocks} blocks, {result_report.candidate_pairs} candidate"
        f" pairs: {result_report.bands}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
