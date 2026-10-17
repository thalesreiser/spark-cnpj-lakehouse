"""Explicit, versioned schemas of the Receita Federal CNPJ open data tables.

Every table of a monthly release is described once, column by column, by a `ColumnSpec`
(name, documented target type, description, whether the silver layer may hold nulls). Two
views are derived from that single description (ADR 008):

* `TableSpec.bronze_schema` - a `StructType` where **every column is a nullable string**. The
  bronze layer keeps the source as received: no row is lost or silently nulled by a failed cast.
* `TableSpec.silver_types` - the documented target type of each column, applied by the silver
  layer (RF-03), where invalid values can be counted and reported instead of disappearing.

Column order follows the official layout (``cnpj-metadados.pdf``, "Novo Layout para os DADOS
ABERTOS do CNPJ", 2021-12-02) and was checked against the first rows of the 2026-09 release;
see ``docs/data-dictionary.md``. The files have no header row, so the order is the contract.

`CSV_READ_OPTIONS` holds the Spark CSV reader options for every file of the release, and
`table_for_file` maps a release file (``Empresas3.zip``, ``Cnaes.zip``...) to its table, using the
same names as `cnpj_lakehouse.download`.

Building `StructType` objects does not start a JVM, so this module is pure and cheap to import.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from pyspark.sql.types import (
    ArrayType,
    DataType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# Official layout the schemas follow. Bump `SCHEMA_VERSION` whenever a table changes.
LAYOUT_SOURCE = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"
LAYOUT_DATE = "2021-12-02"
SCHEMA_VERSION = "1"

# Facts about the CSV files, checked on the 2026-09 release (docs/data-dictionary.md).
SOURCE_ENCODING = "ISO-8859-1"
SEPARATOR = ";"
QUOTE = '"'
# Dates are written as yyyyMMdd; "0" and "00000000" (and empty) mean "no date".
DATE_FORMAT = "yyyyMMdd"
EMPTY_DATE_VALUES: tuple[str, ...] = ("", "0", "00000000")
# capital_social uses a comma as the decimal separator ("120000000000,00").
DECIMAL_SEPARATOR = ","
# cnae_fiscal_secundaria holds several CNAE codes separated by commas.
LIST_SEPARATOR = ","

# Spark CSV reader options shared by every file of the release.
CSV_READ_OPTIONS: Mapping[str, str] = MappingProxyType(
    {
        "header": "false",  # no header row: column names come from the schema
        "sep": SEPARATOR,
        "encoding": SOURCE_ENCODING,
        "quote": QUOTE,
        # Quotes inside values are doubled ("NIGHT CLUB ""TUTU"" LTDA"), as in RFC 4180;
        # Spark's default escape character is a backslash, so it must be set explicitly.
        "escape": QUOTE,
        "mode": "PERMISSIVE",
        "multiLine": "false",
    }
)

# Target types used in the layout. Codes and identifiers stay strings: they carry leading zeros
# ("0111301", "05") and are never used in arithmetic.
STRING = StringType()
DATE = DateType()
MONEY = DecimalType(20, 2)
STRING_LIST = ArrayType(StringType(), containsNull=False)

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_FILE_PREFIX = re.compile(r"^([A-Za-z]+)\d*(?:\.zip)?$")


@dataclass(frozen=True)
class ColumnSpec:
    """One column of a source table.

    ``nullable`` is the contract of the typed (silver) column; in bronze every column is a
    nullable string regardless of it.
    """

    name: str
    target_type: DataType
    description: str
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    """One table of the release: its source files and its columns in file order."""

    name: str
    file_prefix: str
    description: str
    columns: tuple[ColumnSpec, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def bronze_schema(self) -> StructType:
        """Schema used to read the CSV into bronze: every column a nullable string."""
        return StructType(
            [
                StructField(column.name, StringType(), nullable=True, metadata=_metadata(column))
                for column in self.columns
            ]
        )

    def silver_types(self) -> dict[str, DataType]:
        """Documented target type of each column, in file order (applied by RF-03)."""
        return {column.name: column.target_type for column in self.columns}


def _metadata(column: ColumnSpec) -> dict[str, str]:
    """Field metadata carried by the bronze schema (visible in Delta and in `df.schema`)."""
    return {
        "description": column.description,
        "target_type": column.target_type.simpleString(),
    }


def _col(
    name: str, description: str, target_type: DataType = STRING, nullable: bool = True
) -> ColumnSpec:
    return ColumnSpec(name, target_type, description, nullable)


def _lookup(name: str, file_prefix: str, description: str, code: str) -> TableSpec:
    """Lookup tables share the same two columns: a code and its description."""
    return TableSpec(
        name=name,
        file_prefix=file_prefix,
        description=description,
        columns=(
            _col("codigo", f"Code of the {code}.", nullable=False),
            _col("descricao", f"Name or description of the {code}."),
        ),
    )


EMPRESAS = TableSpec(
    name="empresas",
    file_prefix="Empresas",
    description="Companies: one row per CNPJ root (first 8 digits), shared by all branches.",
    columns=(
        _col("cnpj_basico", "CNPJ root: first 8 digits of the CNPJ.", nullable=False),
        _col("razao_social", "Legal name of the company."),
        _col("natureza_juridica", "Legal nature code (lookup `naturezas`)."),
        _col(
            "qualificacao_responsavel",
            "Qualification code of the person responsible for the"
            " company (lookup `qualificacoes`).",
        ),
        _col(
            "capital_social",
            "Share capital in BRL; comma as decimal separator in the source.",
            MONEY,
        ),
        _col(
            "porte_empresa",
            "Company size code: 00 not informed, 01 micro, 03 small (EPP), 05 other.",
        ),
        _col(
            "ente_federativo_responsavel",
            "Responsible federative entity, filled only for"
            " legal natures 1xxx (public bodies); e.g. UNIAO, BAHIA, SAO PAULO - SP.",
        ),
    ),
)

ESTABELECIMENTOS = TableSpec(
    name="estabelecimentos",
    file_prefix="Estabelecimentos",
    description="Establishments: one row per full CNPJ (head office or branch).",
    columns=(
        _col("cnpj_basico", "CNPJ root: first 8 digits of the CNPJ.", nullable=False),
        _col("cnpj_ordem", "Establishment number: 9th to 12th digits of the CNPJ.", nullable=False),
        _col("cnpj_dv", "Check digits: last 2 digits of the CNPJ.", nullable=False),
        _col("identificador_matriz_filial", "1 head office (matriz), 2 branch (filial)."),
        _col("nome_fantasia", "Trade name."),
        _col(
            "situacao_cadastral",
            "Registration status code: 01 annulled (nula), 02 active (ativa),"
            " 03 suspended (suspensa), 04 unfit (inapta), 08 closed (baixada).",
        ),
        _col("data_situacao_cadastral", "Date of the registration status event.", DATE),
        _col(
            "motivo_situacao_cadastral",
            "Reason code of the registration status (lookup `motivos`).",
        ),
        _col("nome_cidade_exterior", "City name abroad."),
        _col("pais", "Country code (lookup `paises`)."),
        _col("data_inicio_atividade", "Date the activity started.", DATE),
        _col("cnae_fiscal_principal", "Main economic activity code, CNAE (lookup `cnaes`)."),
        _col(
            "cnae_fiscal_secundaria",
            "Secondary CNAE codes; comma-separated in the source.",
            STRING_LIST,
        ),
        _col("tipo_logradouro", "Street type (RUA, AVENIDA...)."),
        _col("logradouro", "Street name of the establishment."),
        _col("numero", "Street number; 'S/N' when there is none."),
        _col("complemento", "Address complement."),
        _col("bairro", "Neighbourhood."),
        _col("cep", "Postal code (CEP), 8 digits."),
        _col("uf", "State (UF) abbreviation."),
        _col(
            "municipio",
            "Municipality code of the Receita Federal (lookup `municipios`; not the IBGE code).",
        ),
        _col("ddd_1", "Area code (DDD) of phone 1."),
        _col("telefone_1", "Phone number 1."),
        _col("ddd_2", "Area code (DDD) of phone 2."),
        _col("telefone_2", "Phone number 2."),
        _col("ddd_fax", "Area code (DDD) of the fax."),
        _col("fax", "Fax number."),
        _col("correio_eletronico", "E-mail of the taxpayer."),
        _col("situacao_especial", "Special status of the company."),
        _col("data_situacao_especial", "Date the company entered the special status.", DATE),
    ),
)

SOCIOS = TableSpec(
    name="socios",
    file_prefix="Socios",
    description="Partners (QSA): one row per partner of a company.",
    columns=(
        _col("cnpj_basico", "CNPJ root: first 8 digits of the CNPJ.", nullable=False),
        _col("identificador_socio", "Partner type: 1 legal entity, 2 individual, 3 foreigner."),
        _col(
            "nome_socio_razao_social",
            "Name of the individual partner or legal name of the partner company.",
        ),
        _col(
            "cpf_cnpj_socio",
            "CPF (masked by the source: ***999999**) or CNPJ of the"
            " partner; empty for foreigners.",
        ),
        _col("qualificacao_socio", "Partner qualification code (lookup `qualificacoes`)."),
        _col("data_entrada_sociedade", "Date the partner joined the company.", DATE),
        _col("pais", "Country code of a foreign partner (lookup `paises`)."),
        _col("representante_legal", "CPF of the legal representative (masked by the source)."),
        _col("nome_do_representante", "Name of the legal representative."),
        _col(
            "qualificacao_representante_legal",
            "Qualification code of the legal representative (lookup `qualificacoes`).",
        ),
        _col(
            "faixa_etaria",
            "Age group code: 1 = 0-12 years, 2 = 13-20, 3 = 21-30, ...,"
            " 8 = 71-80, 9 = over 80, 0 = not applicable.",
        ),
    ),
)

SIMPLES = TableSpec(
    name="simples",
    file_prefix="Simples",
    description="Simples Nacional and MEI tax regime options, one row per CNPJ root.",
    columns=(
        _col("cnpj_basico", "CNPJ root: first 8 digits of the CNPJ.", nullable=False),
        _col("opcao_pelo_simples", "Opted for Simples Nacional: S yes, N no, empty other."),
        _col("data_opcao_simples", "Date of the Simples Nacional option.", DATE),
        _col("data_exclusao_simples", "Date of the exclusion from Simples Nacional.", DATE),
        _col(
            "opcao_mei",
            "Opted for MEI (individual micro-entrepreneur): S yes, N no, empty other.",
        ),
        _col("data_opcao_mei", "Date of the MEI option.", DATE),
        _col("data_exclusao_mei", "Date of the exclusion from MEI.", DATE),
    ),
)

CNAES = _lookup("cnaes", "Cnaes", "Economic activities (CNAE).", "economic activity (CNAE)")
MOTIVOS = _lookup(
    "motivos", "Motivos", "Reasons of the registration status.", "registration status reason"
)
MUNICIPIOS = _lookup(
    "municipios", "Municipios", "Municipalities (Receita Federal codes).", "municipality"
)
NATUREZAS = _lookup("naturezas", "Naturezas", "Legal natures.", "legal nature")
PAISES = _lookup("paises", "Paises", "Countries.", "country")
QUALIFICACOES = _lookup(
    "qualificacoes",
    "Qualificacoes",
    "Qualifications of partners and responsible persons.",
    "qualification",
)

# Every table of a release, keyed by table name.
TABLES: Mapping[str, TableSpec] = MappingProxyType(
    {
        table.name: table
        for table in (
            EMPRESAS,
            ESTABELECIMENTOS,
            SOCIOS,
            SIMPLES,
            CNAES,
            MOTIVOS,
            MUNICIPIOS,
            NATUREZAS,
            PAISES,
            QUALIFICACOES,
        )
    }
)
# Source file prefix (``Empresas`` in ``Empresas3.zip``) -> table.
TABLES_BY_PREFIX: Mapping[str, TableSpec] = MappingProxyType(
    {table.file_prefix: table for table in TABLES.values()}
)


def get_table(name: str) -> TableSpec:
    """Table spec by table name (``empresas``, ``cnaes``...)."""
    try:
        return TABLES[name]
    except KeyError:
        raise KeyError(f"unknown table {name!r} (tables: {', '.join(TABLES)})") from None


def table_for_file(file_name: str) -> TableSpec:
    """Table spec of a release file: ``Empresas3.zip``, ``Simples.zip``, ``Cnaes``...

    The numeric suffix and the ``.zip`` extension are optional; names follow
    `cnpj_lakehouse.download.ALL_FILES`.
    """
    match = _FILE_PREFIX.match(file_name.strip())
    table = TABLES_BY_PREFIX.get(match.group(1)) if match else None
    if table is None:
        raise ValueError(
            f"no table for file {file_name!r} (prefixes: {', '.join(TABLES_BY_PREFIX)})"
        )
    return table


def bronze_schema(table: str) -> StructType:
    """Bronze `StructType` (all nullable strings) of a table, by name."""
    return get_table(table).bronze_schema()


def silver_types(table: str) -> dict[str, DataType]:
    """Documented target types of a table's columns, by name."""
    return get_table(table).silver_types()


def is_snake_case(name: str) -> bool:
    """True for lower-case snake_case identifiers (``cnpj_basico``, ``ddd_1``)."""
    return bool(_SNAKE_CASE.match(name))
