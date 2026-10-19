"""RF-09: deduplication edge cases of the silver current record (RF-03, RN-01, ADR 010).

In-memory bronze-shaped DataFrames only (no file I/O, no dataset), compared with chispa. The
cases complement ``test_silver.py``: null ordering columns, ties, null and malformed keys,
several competencias, masked/unpadded and alphanumeric CNPJ keys, and the invalid / all-zero
CNPJ outcome under every combination of the two ADR 010 switches.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from cnpj_lakehouse.bronze import INGEST_TS, PARTITION_COLUMN, SOURCE_FILE
from cnpj_lakehouse.schemas import get_table
from cnpj_lakehouse.silver import (
    CNPJ_COLUMN,
    CNPJ_VALID_COLUMN,
    InvalidCnpjPolicy,
    SilverOptions,
    build_silver,
    deduplicate,
    duplicate_keys,
    get_rule,
    quality_counts,
    select_rows,
    type_and_flag,
)

TS1 = datetime(2026, 10, 1, 8, 0)
TS2 = datetime(2026, 10, 2, 8, 0)
BASE = "80300911"


def dv(root: str) -> str:
    """Module 11 check digits of a 12-character root (numeric or alphanumeric: ``ascii - 48``),
    computed here independently of ``normalize``."""
    values = [ord(c) - 48 for c in root.upper()]
    first = _mod11(values, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    second = _mod11([*values, first], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return f"{first}{second}"


def _mod11(values: list[int], weights: list[int]) -> int:
    rest = sum(v * w for v, w in zip(values, weights, strict=True)) % 11
    return 0 if rest < 2 else 11 - rest


def bronze_df(
    spark: SparkSession, table: str, rows: list[dict[str, Any]], partitions: int = 2
) -> DataFrame:
    """Bronze-shaped DataFrame: every source column a string, plus the metadata columns."""
    schema = StructType(
        [StructField(name, StringType()) for name in get_table(table).column_names]
        + [
            StructField(INGEST_TS, TimestampType()),
            StructField(SOURCE_FILE, StringType()),
            StructField(PARTITION_COLUMN, StringType()),
        ]
    )
    defaults = {INGEST_TS: TS1, SOURCE_FILE: "file:/raw/a.csv", PARTITION_COLUMN: "2026-09"}
    data = [tuple({**defaults, **row}.get(f.name) for f in schema.fields) for row in rows]
    return spark.createDataFrame(data, schema).repartition(partitions)


def establishment(order: str = "0001", base: str = BASE, **values: Any) -> dict[str, Any]:
    return {"cnpj_basico": base, "cnpj_ordem": order, "cnpj_dv": dv(base + order), **values}


def expected(spark: SparkSession, ddl: str, rows: list[tuple[Any, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, ddl)


def assert_rows(actual: DataFrame, wanted: DataFrame) -> None:
    """Same rows on the expected columns, whatever the row order (Spark gives no order)."""
    assert_df_equality(
        actual.select(*wanted.columns), wanted, ignore_row_order=True, ignore_nullable=True
    )


# --- the deduplicate step ----------------------------------------------------------------------


def test_deduplicate_keeps_the_first_row_of_the_ordering_and_the_helper_hash(
    spark: SparkSession,
) -> None:
    rows = [
        establishment(nome_fantasia="OLD", data_situacao_cadastral="20200101"),
        establishment(nome_fantasia="NEW", data_situacao_cadastral="20240101"),
        establishment("0002", nome_fantasia="ONLY"),
    ]
    rule = get_rule("estabelecimentos")
    flagged = select_rows(type_and_flag(bronze_df(spark, "estabelecimentos", rows), rule.table))
    out = deduplicate(flagged, rule)
    assert "_row_hash" in out.columns and "_row_number" not in out.columns
    assert_rows(
        out,
        expected(
            spark,
            "cnpj_ordem string, nome_fantasia string",
            [("0001", "NEW"), ("0002", "ONLY")],
        ),
    )


# --- null ordering columns ---------------------------------------------------------------------


@pytest.mark.parametrize("missing", [None, "", "0", "00000000", "20251340"])
def test_a_row_without_status_date_loses_to_a_dated_one(
    spark: SparkSession, missing: str | None
) -> None:
    """Ordering is ``desc nulls last``: empty, zero and unparseable dates never win."""
    rows = [
        establishment(nome_fantasia="NO DATE", data_situacao_cadastral=missing),
        establishment(nome_fantasia="DATED", data_situacao_cadastral="19990101"),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows), "estabelecimentos")
    assert_rows(
        out,
        expected(
            spark,
            "nome_fantasia string, data_situacao_cadastral date",
            [("DATED", date(1999, 1, 1))],
        ),
    )


def test_when_every_status_date_is_null_the_start_date_decides(spark: SparkSession) -> None:
    rows = [
        establishment(nome_fantasia="A", data_inicio_atividade="20100101"),
        establishment(nome_fantasia="B", data_inicio_atividade="20150101"),
        establishment(nome_fantasia="C"),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows), "estabelecimentos")
    assert [row["nome_fantasia"] for row in out.collect()] == ["B"]


def test_simples_without_any_event_date_loses_to_a_row_with_one(spark: SparkSession) -> None:
    rows = [
        {"cnpj_basico": BASE, "opcao_pelo_simples": "N"},
        {"cnpj_basico": BASE, "opcao_pelo_simples": "S", "data_opcao_mei": "20010101"},
    ]
    out = build_silver(bronze_df(spark, "simples", rows), "simples")
    assert [row["opcao_pelo_simples"] for row in out.collect()] == ["S"]


# --- ties --------------------------------------------------------------------------------------


def test_date_tie_falls_back_to_ingest_ts_then_source_file(spark: SparkSession) -> None:
    same_date = {"data_situacao_cadastral": "20240101"}
    rows = [
        establishment(nome_fantasia="EARLY", **same_date, **{INGEST_TS: TS1}),
        establishment(nome_fantasia="LATE", **same_date, **{INGEST_TS: TS2}),
        establishment("0002", nome_fantasia="FILE A", **{SOURCE_FILE: "file:/raw/a.csv"}),
        establishment("0002", nome_fantasia="FILE B", **{SOURCE_FILE: "file:/raw/b.csv"}),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows), "estabelecimentos")
    assert_rows(
        out,
        expected(
            spark,
            "cnpj_ordem string, nome_fantasia string",
            [("0001", "LATE"), ("0002", "FILE B")],
        ),
    )


def test_identical_rows_collapse_to_one(spark: SparkSession) -> None:
    rows = [establishment(nome_fantasia="SAME")] * 5
    df = bronze_df(spark, "estabelecimentos", rows, partitions=4)
    out = build_silver(df, "estabelecimentos")
    assert out.count() == 1 and duplicate_keys(out, "estabelecimentos") == 0
    assert quality_counts(type_and_flag(df, "estabelecimentos"), "estabelecimentos").rows_in == 5


def test_a_full_tie_is_broken_by_the_row_hash_whatever_the_partitioning(
    spark: SparkSession,
) -> None:
    rows = [establishment(nome_fantasia=f"NAME {i}") for i in range(6)]
    picked = [
        build_silver(
            bronze_df(spark, "estabelecimentos", order, parts), "estabelecimentos"
        ).first()["nome_fantasia"]
        for order, parts in ((rows, 1), (rows[::-1], 4), (rows[2:] + rows[:2], 3))
    ]
    assert len(set(picked)) == 1


# --- null and malformed keys -------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    [
        {"cnpj_basico": None, "cnpj_ordem": "0001", "cnpj_dv": "00"},
        {"cnpj_basico": BASE, "cnpj_ordem": "", "cnpj_dv": "00"},
        {"cnpj_basico": BASE, "cnpj_ordem": "0001", "cnpj_dv": "AB"},  # DV is numeric only
        {"cnpj_basico": "123456789", "cnpj_ordem": "0001", "cnpj_dv": "00"},  # too long
    ],
    ids=["null-base", "empty-order", "letter-dv", "long-base"],
)
def test_rows_with_a_null_or_malformed_key_part_are_discarded_and_counted(
    spark: SparkSession, key: dict[str, Any]
) -> None:
    rows = [key, key, establishment()]  # two keyless rows must not form a "null" group
    df = bronze_df(spark, "estabelecimentos", rows)
    counts = quality_counts(type_and_flag(df, "estabelecimentos"), "estabelecimentos")
    out = build_silver(df, "estabelecimentos")
    assert [row[CNPJ_COLUMN] for row in out.collect()] == [BASE + "0001" + dv(BASE + "0001")]
    assert (counts.rows_without_key, counts.rows_deduplicated_in) == (2, 1)


def test_empresas_without_base_is_discarded(spark: SparkSession) -> None:
    rows = [{"cnpj_basico": " ", "razao_social": "X"}, {"cnpj_basico": BASE, "razao_social": "Y"}]
    out = build_silver(bronze_df(spark, "empresas", rows), "empresas")
    assert_rows(out, expected(spark, "cnpj_basico string", [(BASE,)]))


# --- several competencias ----------------------------------------------------------------------


def test_each_key_keeps_its_latest_competencia(spark: SparkSession) -> None:
    rows = [
        # A: three months, the latest wins although its date is null
        establishment(
            nome_fantasia="A-07", data_situacao_cadastral="20250101", competencia="2026-07"
        ),
        establishment(
            nome_fantasia="A-08", data_situacao_cadastral="20260101", competencia="2026-08"
        ),
        establishment(nome_fantasia="A-09", competencia="2026-09"),
        # B: only in an older month, still kept
        establishment("0002", nome_fantasia="B-07", competencia="2026-07"),
        # C: two rows in the latest month, the date decides inside it
        establishment(
            "0003", nome_fantasia="C-08", data_situacao_cadastral="20990101", competencia="2026-08"
        ),
        establishment("0003", nome_fantasia="C-09a", data_situacao_cadastral="20200101"),
        establishment("0003", nome_fantasia="C-09b", data_situacao_cadastral="20210101"),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows, 3), "estabelecimentos")
    assert_rows(
        out,
        expected(
            spark,
            "cnpj_ordem string, nome_fantasia string, competencia string",
            [
                ("0001", "A-09", "2026-09"),
                ("0002", "B-07", "2026-07"),
                ("0003", "C-09b", "2026-09"),
            ],
        ),
    )
    assert duplicate_keys(out, "estabelecimentos") == 0


# --- key normalization before deduplication ----------------------------------------------------


def test_masked_and_unpadded_bases_are_the_same_key(spark: SparkSession) -> None:
    rows = [
        {"cnpj_basico": "803009", "razao_social": "SHORT", INGEST_TS: TS1},
        {"cnpj_basico": "00.803.009", "razao_social": "MASKED", INGEST_TS: TS2},
        {"cnpj_basico": "00803009", "razao_social": "PADDED", INGEST_TS: TS1},
    ]
    out = build_silver(bronze_df(spark, "empresas", rows), "empresas")
    assert_rows(
        out,
        expected(spark, "cnpj_basico string, razao_social string", [("00803009", "MASKED")]),
    )


def test_alphanumeric_cnpj_keys(spark: SparkSession) -> None:
    """Alphanumeric CNPJ (from July 2026, ADR 009): case and mask do not split a key."""
    base, order = "12ABC345", "01DE"
    good = dv(base + order)
    rows = [
        {"cnpj_basico": base.lower(), "cnpj_ordem": order.lower(), "cnpj_dv": good, INGEST_TS: TS1},
        {"cnpj_basico": "12.ABC.345", "cnpj_ordem": order, "cnpj_dv": good, INGEST_TS: TS2},
        # another establishment of the same alphanumeric base
        {"cnpj_basico": base, "cnpj_ordem": "0001", "cnpj_dv": dv(base + "0001")},
        # wrong check digits: kept and flagged (default policy)
        {
            "cnpj_basico": base,
            "cnpj_ordem": "ZZ99",
            "cnpj_dv": "00" if dv(base + "ZZ99") != "00" else "11",
        },
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows), "estabelecimentos")
    bad = "00" if dv(base + "ZZ99") != "00" else "11"
    assert_rows(
        out,
        expected(
            spark,
            f"{CNPJ_COLUMN} string, {CNPJ_VALID_COLUMN} boolean, {INGEST_TS} timestamp",
            [
                (base + order + good, True, TS2),
                (base + "0001" + dv(base + "0001"), True, TS1),
                (base + "ZZ99" + bad, False, TS1),
            ],
        ),
    )


# --- invalid and all-zero CNPJ under the ADR 010 switches --------------------------------------

VALID = BASE + "0001" + dv(BASE + "0001")
BAD_DV = BASE + "0002" + ("00" if dv(BASE + "0002") != "00" else "11")
ZERO = "0" * 14


def _split(cnpj: str) -> dict[str, Any]:
    return {"cnpj_basico": cnpj[:8], "cnpj_ordem": cnpj[8:12], "cnpj_dv": cnpj[12:]}


@pytest.mark.parametrize(
    ("policy", "zero_is_invalid", "kept", "invalid"),
    [
        (InvalidCnpjPolicy.FLAG, True, [(VALID, True), (BAD_DV, False), (ZERO, False)], 4),
        (InvalidCnpjPolicy.FLAG, False, [(VALID, True), (BAD_DV, False), (ZERO, True)], 2),
        (InvalidCnpjPolicy.DROP, True, [(VALID, True)], 4),
        (InvalidCnpjPolicy.DROP, False, [(VALID, True), (ZERO, True)], 2),
    ],
    ids=["flag-zero-invalid", "flag-zero-valid", "drop-zero-invalid", "drop-zero-valid"],
)
def test_invalid_and_zero_cnpj_under_both_switches(
    spark: SparkSession,
    policy: InvalidCnpjPolicy,
    zero_is_invalid: bool,
    kept: list[tuple[str, bool]],
    invalid: int,
) -> None:
    options = SilverOptions(invalid_cnpj_policy=policy, zero_cnpj_is_invalid=zero_is_invalid)
    # every CNPJ twice: duplicates of invalid CNPJs collapse like any other key
    rows = [_split(cnpj) for cnpj in (VALID, BAD_DV, ZERO) for _ in range(2)]
    df = bronze_df(spark, "estabelecimentos", rows)
    counts = quality_counts(
        type_and_flag(df, "estabelecimentos", options), "estabelecimentos", options
    )
    out = build_silver(df, "estabelecimentos", options)
    assert_rows(out, expected(spark, f"{CNPJ_COLUMN} string, {CNPJ_VALID_COLUMN} boolean", kept))
    dropped = invalid if policy is InvalidCnpjPolicy.DROP else 0
    assert (counts.invalid_cnpj, counts.zero_cnpj, counts.invalid_cnpj_dropped) == (
        invalid,
        2,
        dropped,
    )
    assert counts.rows_deduplicated_in == 6 - dropped
