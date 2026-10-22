"""RF-03 / RN-01 / RNF-04: unit tests of the pure normalization functions (local Spark, tiny
in-memory DataFrames, no file I/O, so they also run on Windows without winutils)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from chispa import assert_df_equality
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, DecimalType, StringType

from cnpj_lakehouse import normalize as N
from cnpj_lakehouse.schemas import EMPRESAS, ESTABELECIMENTOS


@pytest.fixture(params=["false", "true"], ids=["ansi-off", "ansi-on"])
def ansi(spark: SparkSession, request: pytest.FixtureRequest) -> Iterator[SparkSession]:
    """Runs a test with ``spark.sql.ansi.enabled`` off and on: the functions must never raise."""
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield spark
    spark.conf.set("spark.sql.ansi.enabled", previous or "false")


def _apply(spark: SparkSession, fn: Callable[[Column], Column], values: list[Any]) -> list[Any]:
    df = spark.createDataFrame([(v,) for v in values], "v string")
    return [row[0] for row in df.select(fn(F.col("v"))).collect()]


# --- strings -----------------------------------------------------------------------------------


def test_clean_string(ansi: SparkSession) -> None:
    values = ["  BANCO   DO\tBRASIL  ", "", "   ", None, "São Paulo"]
    assert _apply(ansi, N.clean_string, values) == [
        "BANCO DO BRASIL",
        None,
        None,
        None,
        "São Paulo",
    ]


def test_normalize_key_strips_accents_and_upper_cases(ansi: SparkSession) -> None:
    values = [
        " São  Paulo ",
        "AÇÚCAR & CIA",
        "áàâãä éèêë íìîï óòôõö úùûü çñ",
        "ÁÀÂÃÄ ÉÊ ÍÓÔÕ ÚÜ ÇÑ",
        "",
        None,
    ]
    assert _apply(ansi, N.normalize_key, values) == [
        "SAO PAULO",
        "ACUCAR & CIA",
        "AAAAA EEEE IIII OOOOO UUUU CN",
        "AAAAA EE IOOO UU CN",
        None,
        None,
    ]


def test_accent_tables_are_aligned() -> None:
    assert len(N.ACCENTED) == len(N.UNACCENTED)
    assert N.ACCENTED.upper() == N.ACCENTED[: len(N.ACCENTED) // 2] * 2


def test_strip_accents_keeps_the_case(spark: SparkSession) -> None:
    assert _apply(spark, N.strip_accents, ["Ação", None]) == ["Acao", None]


# --- CNPJ --------------------------------------------------------------------------------------


def test_strip_mask(spark: SparkSession) -> None:
    values = ["12.ABC.345/01de-35", " 00.000.000/0001-91 ", "./-", None]
    assert _apply(spark, N.strip_mask, values) == ["12ABC34501DE35", "00000000000191", None, None]


def test_cnpj_parts_are_padded_and_validated(ansi: SparkSession) -> None:
    basico = ["191", "00000000", "12.ABC.345", "12abc345", "ABC", "123456789", "", None]
    assert _apply(ansi, N.normalize_cnpj_basico, basico) == [
        "00000191",
        "00000000",
        "12ABC345",
        "12ABC345",
        None,  # alphanumeric values are never padded
        None,  # too long
        None,
        None,
    ]
    ordem = ["1", "0001", "01DE", "1DE", "12345", None]
    assert _apply(ansi, N.normalize_cnpj_ordem, ordem) == ["0001", "0001", "01DE", None, None, None]
    dv = ["1", "91", "-35", "3A", "123", None]
    assert _apply(ansi, N.normalize_cnpj_dv, dv) == ["01", "91", "35", None, None, None]


def test_cnpj_check_digits(ansi: SparkSession) -> None:
    values = [
        "000000000001",  # Banco do Brasil head office: 00.000.000/0001-91
        "12ABC34501DE",  # official example of the Receita Federal: 12.ABC.345/01DE-35
        "12.abc.345/01de",
        "11222333000181"[:12],
        "1234",
        "12ABC34501D!",
        None,
    ]
    assert _apply(ansi, N.cnpj_check_digits, values) == ["91", "35", "35", "81", None, None, None]


def test_cnpj_root_and_root_check_digits(ansi: SparkSession) -> None:
    roots = ["00.000.000/0001", "12abc34501de", "1234", None]
    assert _apply(ansi, N.cnpj_root, roots) == ["000000000001", "12ABC34501DE", None, None]
    df = ansi.createDataFrame([("000000000001",), ("12ABC34501DE",), (None,)], "root string")
    out = [row[0] for row in df.select(N.root_check_digits("root")).collect()]
    assert out == ["91", "35", None]


def test_check_digit_weights() -> None:
    assert len(N.DV1_WEIGHTS) == 12
    assert len(N.DV2_WEIGHTS) == 13
    assert N.DV2_WEIGHTS[1:] == N.DV1_WEIGHTS


def test_normalize_cnpj(ansi: SparkSession) -> None:
    values = [
        "00.000.000/0001-91",
        "191",  # numeric value that lost its leading zeros
        "12.abc.345/01de-35",
        "12ABC34501DE35",
        "12ABC34501DE36",  # wrong check digit
        "00.000.000/0001-92",  # wrong check digit
        "12ABC34501DEAB",  # check digits must be numeric
        "ABC",
        "123456789012345",
        "",
        None,
    ]
    assert _apply(ansi, N.normalize_cnpj, values) == [
        "00000000000191",
        "00000000000191",
        "12ABC34501DE35",
        "12ABC34501DE35",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    assert _apply(ansi, lambda c: N.normalize_cnpj(c, validate=False), values) == [
        "00000000000191",
        "00000000000191",
        "12ABC34501DE35",
        "12ABC34501DE35",
        "12ABC34501DE36",
        "00000000000192",
        None,
        None,
        None,
        None,
        None,
    ]


def test_is_valid_cnpj_is_a_quality_flag(ansi: SparkSession) -> None:
    values = ["11.222.333/0001-81", "12ABC34501DE35", "12ABC34501DE36", "XYZ", "", None]
    assert _apply(ansi, N.is_valid_cnpj, values) == [True, True, False, False, False, None]


def test_build_and_split_cnpj(ansi: SparkSession) -> None:
    rows = [
        ("00000000", "0001", "91"),
        ("12abc345", "01DE", "35"),
        ("191", "1", "1"),
        ("12ABC", "0001", "35"),  # short alphanumeric base: invalid
        (None, "0001", "91"),
    ]
    df = ansi.createDataFrame(rows, "b string, o string, d string")
    built = [r[0] for r in df.select(N.build_cnpj("b", "o", "d")).collect()]
    assert built == ["00000000000191", "12ABC34501DE35", "00000191000101", None, None]

    full = ansi.createDataFrame([("12.ABC.345/01DE-35",), ("00000000000192",), (None,)], "c string")
    parts = full.select(*N.split_cnpj("c")).collect()
    assert [tuple(r) for r in parts] == [("12ABC345", "01DE", "35"), (None,) * 3, (None,) * 3]
    unvalidated = full.select(*N.split_cnpj(F.col("c"), validate=False)).collect()
    assert tuple(unvalidated[1]) == ("00000000", "0001", "92")


# --- CNAE --------------------------------------------------------------------------------------


def test_normalize_cnae(ansi: SparkSession) -> None:
    values = [
        "0111301",
        "111301",
        "0111-3/01",
        " 4711-3/02 ",
        "8888888",
        "12345678",
        "47A1302",
        "",
        None,
    ]
    assert _apply(ansi, N.normalize_cnae, values) == [
        "0111301",
        "0111301",
        "0111301",
        "4711302",
        "8888888",
        None,
        None,
        None,
        None,
    ]


def test_normalize_cnae_list(ansi: SparkSession) -> None:
    values = ["4711302,111301, 0111-3/01,,XX,5611201", "", "  ", None]
    df = ansi.createDataFrame([(v,) for v in values], "v string")
    result = df.select(N.normalize_cnae_list("v").alias("cnaes"))
    assert [r.cnaes for r in result.collect()] == [
        ["4711302", "0111301", "5611201"],
        [],
        [],
        [],
    ]
    data_type = result.schema["cnaes"].dataType
    assert isinstance(data_type, ArrayType) and data_type.elementType == StringType()


# --- dates and money ---------------------------------------------------------------------------


def test_parse_date(ansi: SparkSession) -> None:
    values = [
        "20250115",
        " 20240229 ",  # leap day
        "20000229",  # leap year (divisible by 400)
        "19000229",  # not a leap year (divisible by 100)
        "20250229",
        "20251340",
        "20250230",
        "20250431",
        "20251300",
        "20250100",
        "00000000",
        "0",
        "",
        "2025011",
        "202501155",
        "2025-01-15",
        "abcdefgh",
        "00010101",
        None,
    ]
    assert _apply(ansi, N.parse_date, values) == [
        date(2025, 1, 15),
        date(2024, 2, 29),
        date(2000, 2, 29),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        date(1, 1, 1),
        None,
    ]


def test_parse_date_is_safe_with_the_legacy_parser_policy(spark: SparkSession) -> None:
    """`to_date` raises SparkUpgradeException on ``2025011`` with the default policy; ours not."""
    key = "spark.sql.legacy.timeParserPolicy"
    previous = spark.conf.get(key)
    try:
        for policy in ("EXCEPTION", "CORRECTED", "LEGACY"):
            spark.conf.set(key, policy)
            assert _apply(spark, N.parse_date, ["2025011", "20251340", "20250115"]) == [
                None,
                None,
                date(2025, 1, 15),
            ]
    finally:  # the session is shared by the whole run
        spark.conf.set(key, previous or "EXCEPTION")


def test_parse_decimal(ansi: SparkSession) -> None:
    values = [
        "120000000000,00",
        "0,00",
        "1000,5",
        "1000",
        "-10,25",
        " 7,10 ",
        "999999999999999999,99",
        "1000000000000000000,00",  # 19 integer digits: does not fit decimal(20,2)
        "1.000,50",  # thousand separator
        "1000.50",
        "10,123",
        "abc",
        "",
        None,
    ]
    assert _apply(ansi, N.parse_decimal, values) == [
        Decimal("120000000000.00"),
        Decimal("0.00"),
        Decimal("1000.50"),
        Decimal("1000.00"),
        Decimal("-10.25"),
        Decimal("7.10"),
        Decimal("999999999999999999.99"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]


# --- DataFrame helper --------------------------------------------------------------------------


def test_apply_silver_types_empresas(spark: SparkSession) -> None:
    bronze = spark.createDataFrame(
        [
            ("191", "  BANCO  DO BRASIL SA ", "2038", "10", "120000000000,00", "05", "UNIAO", "x"),
            ("12abc345", "", "2062", "49", "1.0,0", "01", None, "y"),
        ],
        EMPRESAS.bronze_schema().add("_source_file", "string"),
    )
    silver = N.apply_silver_types(bronze, "empresas")
    expected = spark.createDataFrame(
        [
            (
                "00000191",
                "BANCO DO BRASIL SA",
                "2038",
                "10",
                Decimal("120000000000.00"),
                "05",
                "UNIAO",
                "x",
            ),
            ("12ABC345", None, "2062", "49", None, "01", None, "y"),
        ],
        "cnpj_basico string, razao_social string, natureza_juridica string,"
        " qualificacao_responsavel string, capital_social decimal(20,2), porte_empresa string,"
        " ente_federativo_responsavel string, _source_file string",
    )
    assert_df_equality(silver, expected, ignore_nullable=True)


def test_apply_silver_types_estabelecimentos(spark: SparkSession) -> None:
    values = {name: None for name in ESTABELECIMENTOS.column_names}
    values.update(
        cnpj_basico="12.ABC.345",
        cnpj_ordem="1DE".rjust(4, "0"),
        cnpj_dv="35",
        nome_fantasia="  Café  Açúcar ",
        data_situacao_cadastral="20251340",
        data_inicio_atividade="20200101",
        cnae_fiscal_principal="4711-3/02",
        cnae_fiscal_secundaria="5611201,4711302",
        data_situacao_especial="00000000",
    )
    bronze = spark.createDataFrame([tuple(values.values())], ESTABELECIMENTOS.bronze_schema())
    row = N.apply_silver_types(bronze, "estabelecimentos").collect()[0]
    assert (row.cnpj_basico, row.cnpj_ordem, row.cnpj_dv) == ("12ABC345", "01DE", "35")
    assert row.nome_fantasia == "Café Açúcar"
    assert row.data_situacao_cadastral is None
    assert row.data_inicio_atividade == date(2020, 1, 1)
    assert row.data_situacao_especial is None
    assert row.cnae_fiscal_principal == "4711302"
    assert row.cnae_fiscal_secundaria == ["5611201", "4711302"]

    schema = N.apply_silver_types(bronze, "estabelecimentos").schema
    assert schema.names == list(ESTABELECIMENTOS.column_names)
    for name, target in ESTABELECIMENTOS.silver_types().items():
        actual = schema[name].dataType
        if isinstance(target, ArrayType):
            assert isinstance(actual, ArrayType) and actual.elementType == target.elementType
        else:
            assert actual == target, name
    assert isinstance(schema["data_inicio_atividade"].dataType, DateType)
    assert not isinstance(schema["cnpj_basico"].dataType, DecimalType)


def test_functions_are_pure_expressions(spark: SparkSession) -> None:
    """RNF-04: the functions only build expressions; nothing runs until an action."""
    expression = N.normalize_cnpj("cnpj")
    assert isinstance(expression, Column)
    df = spark.createDataFrame([("00000000000191",)], "cnpj string")
    assert df.select(expression.alias("c")).collect()[0].c == "00000000000191"
    assert df.collect()[0].cnpj == "00000000000191"  # the input DataFrame is unchanged
