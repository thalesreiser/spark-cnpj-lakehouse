"""Tests of the explicit table schemas (task 2.2, ADR 008).

Most tests are pure (building `StructType` objects needs no JVM); the last ones read a small
synthetic CSV with a local SparkSession, using the same options as the real release.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, DataType, DateType, DecimalType, StringType

from cnpj_lakehouse import schemas
from cnpj_lakehouse.download import ALL_FILES, FILE_GROUPS, LOOKUP_FILES
from cnpj_lakehouse.schemas import (
    CSV_READ_OPTIONS,
    TABLES,
    TABLES_BY_PREFIX,
    bronze_schema,
    get_table,
    is_snake_case,
    silver_types,
    table_for_file,
)

# Column order of each table, as in the official layout (cnpj-metadados.pdf) and in the files.
EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "empresas": (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ),
    "estabelecimentos": (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ),
    "socios": (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_do_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ),
    "simples": (
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ),
    **{
        name: ("codigo", "descricao")
        for name in ("cnaes", "motivos", "municipios", "naturezas", "paises", "qualificacoes")
    },
}
# Fields per row counted on the first rows of the 2026-09 release (docs/data-dictionary.md).
FIELDS_IN_REAL_FILES = {
    "empresas": 7,
    "estabelecimentos": 30,
    "socios": 11,
    "simples": 7,
    "cnaes": 2,
    "motivos": 2,
    "municipios": 2,
    "naturezas": 2,
    "paises": 2,
    "qualificacoes": 2,
}
ALLOWED_TARGET_TYPES: tuple[DataType, ...] = (
    StringType(),
    DateType(),
    DecimalType(20, 2),
    ArrayType(StringType(), containsNull=False),
)


# --------------------------------------------------------------------------------------------
# Pure tests
# --------------------------------------------------------------------------------------------


def test_every_table_is_described() -> None:
    assert set(TABLES) == set(EXPECTED_COLUMNS)


@pytest.mark.parametrize("table", sorted(EXPECTED_COLUMNS))
def test_columns_follow_the_official_layout(table: str) -> None:
    spec = get_table(table)
    assert spec.column_names == EXPECTED_COLUMNS[table]
    assert len(spec.columns) == FIELDS_IN_REAL_FILES[table]
    assert bronze_schema(table).fieldNames() == list(EXPECTED_COLUMNS[table])


@pytest.mark.parametrize("spec", TABLES.values(), ids=list(TABLES))
def test_names_are_unique_snake_case(spec: schemas.TableSpec) -> None:
    assert is_snake_case(spec.name)
    assert len(set(spec.column_names)) == len(spec.column_names)
    assert all(is_snake_case(name) for name in spec.column_names), spec.column_names


@pytest.mark.parametrize("spec", TABLES.values(), ids=list(TABLES))
def test_bronze_fields_are_nullable_strings_with_metadata(spec: schemas.TableSpec) -> None:
    for field, column in zip(spec.bronze_schema().fields, spec.columns, strict=True):
        assert field.dataType == StringType()
        assert field.nullable is True
        assert field.metadata == {
            "description": column.description,
            "target_type": column.target_type.simpleString(),
        }


@pytest.mark.parametrize("spec", TABLES.values(), ids=list(TABLES))
def test_columns_are_documented_with_valid_target_types(spec: schemas.TableSpec) -> None:
    assert spec.description
    types = silver_types(spec.name)
    assert list(types) == list(spec.column_names)
    for column in spec.columns:
        assert column.description.strip(), column.name
        assert isinstance(column.target_type, DataType)
        assert column.target_type in ALLOWED_TARGET_TYPES, column.name


def test_target_types_of_typed_columns() -> None:
    assert silver_types("empresas")["capital_social"] == DecimalType(20, 2)
    assert silver_types("estabelecimentos")["cnae_fiscal_secundaria"] == ArrayType(
        StringType(), containsNull=False
    )
    dates = {
        table: sorted(name for name, kind in silver_types(table).items() if kind == DateType())
        for table in TABLES
    }
    assert dates["estabelecimentos"] == [
        "data_inicio_atividade",
        "data_situacao_cadastral",
        "data_situacao_especial",
    ]
    assert dates["socios"] == ["data_entrada_sociedade"]
    assert dates["simples"] == [
        "data_exclusao_mei",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_opcao_simples",
    ]
    assert dates["empresas"] == []
    # Codes keep their leading zeros: they stay strings.
    assert silver_types("estabelecimentos")["cnae_fiscal_principal"] == StringType()
    assert silver_types("cnaes")["codigo"] == StringType()


def test_business_keys_are_not_nullable() -> None:
    for spec in TABLES.values():
        key = spec.columns[0]
        assert key.name in {"cnpj_basico", "codigo"}
        assert key.nullable is False
    keys = [c.name for c in get_table("estabelecimentos").columns if not c.nullable]
    assert keys == ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]


def test_file_prefixes_cover_every_downloaded_file() -> None:
    tables = {name: table_for_file(name).name for name in ALL_FILES}
    assert len(tables) == 37
    for group in ("empresas", "estabelecimentos", "socios", "simples"):
        assert {tables[name] for name in FILE_GROUPS[group]} == {group}
    assert {tables[name] for name in LOOKUP_FILES} == {
        "cnaes",
        "motivos",
        "municipios",
        "naturezas",
        "paises",
        "qualificacoes",
    }
    # every table has at least one file, and every prefix maps back to its table
    assert set(tables.values()) == set(TABLES)
    assert all(TABLES_BY_PREFIX[spec.file_prefix] is spec for spec in TABLES.values())


@pytest.mark.parametrize(
    ("file_name", "table"),
    [("Empresas0.zip", "empresas"), ("Socios9", "socios"), (" Simples.zip ", "simples")],
)
def test_table_for_file_accepts_names_without_suffix(file_name: str, table: str) -> None:
    assert table_for_file(file_name).name == table


@pytest.mark.parametrize("file_name", ["empresas0.zip", "Unknown.zip", "", "K3241.EMPRECSV"])
def test_table_for_file_rejects_unknown_names(file_name: str) -> None:
    with pytest.raises(ValueError, match="no table for file"):
        table_for_file(file_name)


def test_get_table_rejects_unknown_names() -> None:
    with pytest.raises(KeyError, match="unknown table"):
        get_table("companies")


def test_csv_read_options_match_the_source_format() -> None:
    assert dict(CSV_READ_OPTIONS) == {
        "header": "false",
        "sep": ";",
        "encoding": "ISO-8859-1",
        "quote": '"',
        "escape": '"',
        "mode": "PERMISSIVE",
        "multiLine": "false",
    }
    with pytest.raises(TypeError):
        CSV_READ_OPTIONS["sep"] = ","  # type: ignore[index]


def test_layout_facts_are_recorded() -> None:
    assert schemas.LAYOUT_SOURCE.endswith("cnpj-metadados.pdf")
    assert schemas.SCHEMA_VERSION
    assert schemas.DATE_FORMAT == "yyyyMMdd"
    assert "00000000" in schemas.EMPTY_DATE_VALUES
    assert schemas.DECIMAL_SEPARATOR == ","


@pytest.mark.parametrize(
    ("name", "expected"),
    [("cnpj_basico", True), ("ddd_1", True), ("Cnpj", False), ("a__b", False), ("_x", False)],
)
def test_is_snake_case(name: str, expected: bool) -> None:
    assert is_snake_case(name) is expected


# --------------------------------------------------------------------------------------------
# Reading a synthetic CSV with local Spark
# --------------------------------------------------------------------------------------------

# Synthetic rows in the format of the real files: every field quoted, `;` separator, quotes
# inside values doubled, a `;` inside a quoted value, accents, no header row.
EMPRESAS_ROWS = [
    '"00000000";"BANCO EXEMPLO SA";"2038";"10";"120000000000,00";"05";""',
    '"00000001";"NIGHT CLUB ""TUTU"" LTDA";"2062";"49";"0,00";"01";""',
    '"00000002";"CAFÉ; AÇÚCAR & CIA";"1031";"49";"1000,50";"03";"SÃO PAULO - SP"',
]


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-schemas")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_latin1(path: Path, rows: list[str]) -> Path:
    path.write_bytes(("\n".join(rows) + "\n").encode("latin-1"))
    return path


def test_spark_reads_a_latin1_csv_with_the_bronze_schema(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = _write_latin1(tmp_path / "K3241.K03200Y0.D60912.EMPRECSV", EMPRESAS_ROWS)

    df = spark.read.options(**CSV_READ_OPTIONS).schema(bronze_schema("empresas")).csv(str(path))

    assert df.schema == bronze_schema("empresas")
    assert df.schema["capital_social"].metadata["target_type"] == "decimal(20,2)"
    rows = [tuple(row) for row in df.orderBy("cnpj_basico").collect()]
    assert rows == [
        ("00000000", "BANCO EXEMPLO SA", "2038", "10", "120000000000,00", "05", None),
        ("00000001", 'NIGHT CLUB "TUTU" LTDA', "2062", "49", "0,00", "01", None),
        ("00000002", "CAFÉ; AÇÚCAR & CIA", "1031", "49", "1000,50", "03", "SÃO PAULO - SP"),
    ]


def test_spark_reads_a_lookup_table_keeping_leading_zeros(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = _write_latin1(
        tmp_path / "F.K03200$Z.D60912.CNAECSV",
        ['"0111301";"Cultivo de arroz"', '"0113000";"Cultivo de cana-de-açúcar"'],
    )

    df = spark.read.options(**CSV_READ_OPTIONS).schema(bronze_schema("cnaes")).csv(str(path))

    assert [tuple(row) for row in df.collect()] == [
        ("0111301", "Cultivo de arroz"),
        ("0113000", "Cultivo de cana-de-açúcar"),
    ]
