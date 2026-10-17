"""RF-03 / RN-01 / RNF-04: pure normalization functions for the silver layer.

Every function here takes and returns Spark `Column` expressions (or a DataFrame in and a
DataFrame out for `apply_silver_types`): no I/O, no SparkSession, no side effects. The silver job
(task 3.2) composes them; the unit tests exercise them on tiny in-memory DataFrames.

Rules (documented in ``docs/data-dictionary.md``, "Silver normalization rules"):

* **Strings** - `clean_string`: trim, collapse inner whitespace to one space, empty -> ``null``.
  `normalize_key` additionally strips accents and upper-cases (comparison/join keys); the
  display value is kept by `clean_string`.
* **CNPJ** (RN-01, alphanumeric CNPJ of the Receita Federal, ADR 009) - masks (``.``, ``/``,
  ``-``, spaces) are removed and letters upper-cased, never dropped. The first 12 characters
  accept ``0-9`` and ``A-Z``; the 2 check digits are always numeric and are computed with the
  module 11 rule over ``ascii(char) - 48`` (so digits keep their value and ``A`` = 17). Numeric
  values are left-padded with zeros to their width (sources that lost leading zeros); an
  alphanumeric value must already have its full width, since padding it would be a guess.
  Anything else becomes ``null``.
* **CNAE** - digits only after removing masks (``0111-3/01``), left-padded to 7; else ``null``.
  The secondary list is split on ``,``, each code normalized, invalid and duplicate codes
  dropped; a missing or empty list becomes an **empty array** (the source cannot tell "none"
  from "not informed").
* **Dates** - ``yyyyMMdd``; ``""``, ``0``, ``00000000``, malformed and impossible dates
  (``20251340``, ``20250230``) become ``null``. The date is built with arithmetic checks and
  `make_date` on validated parts instead of `to_date`: in Spark 3.5 `to_date` and even
  `try_to_timestamp` raise on such values under ANSI mode or with the default
  ``spark.sql.legacy.timeParserPolicy=EXCEPTION``, so these functions are safe in every mode.
* **Money** - ``capital_social`` uses a comma decimal (``120000000000,00``), no thousand
  separators; up to 18 integer and 2 decimal digits -> ``decimal(20,2)``; malformed -> ``null``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType, DecimalType

from cnpj_lakehouse.schemas import (
    DECIMAL_SEPARATOR,
    EMPTY_DATE_VALUES,
    LIST_SEPARATOR,
    MONEY,
    get_table,
)

ColumnOrName = Column | str
Normalizer = Callable[[Column], Column]

# CNPJ layout (RN-01): base (8) + order (4) + check digits (2) = 14 characters.
CNPJ_BASICO_LENGTH = 8
CNPJ_ORDEM_LENGTH = 4
CNPJ_DV_LENGTH = 2
CNPJ_LENGTH = CNPJ_BASICO_LENGTH + CNPJ_ORDEM_LENGTH + CNPJ_DV_LENGTH
CNAE_LENGTH = 7

# Characters removed from identifiers: the usual CNPJ/CNAE masks and whitespace.
MASK_PATTERN = r"[\s./\-]"
# Module 11 weights of the first and second check digits (left to right).
DV1_WEIGHTS: tuple[int, ...] = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
DV2_WEIGHTS: tuple[int, ...] = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)

# Accented letters found in Portuguese text and their unaccented forms (same length).
ACCENTED = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑáàâãäéèêëíìîïóòôõöúùûüçñ"
UNACCENTED = "AAAAAEEEEIIIIOOOOOUUUUCNaaaaaeeeeiiiiooooouuuucn"

_MONEY_PATTERN = r"^-?\d{1,18}(" + DECIMAL_SEPARATOR + r"\d{1,2})?$"


def _col(column: ColumnOrName) -> Column:
    return F.col(column) if isinstance(column, str) else column


# --- strings -----------------------------------------------------------------------------------


def clean_string(column: ColumnOrName) -> Column:
    """Trim, collapse inner whitespace to a single space; empty or blank -> ``null``."""
    cleaned = F.trim(F.regexp_replace(_col(column), r"\s+", " "))
    return F.when(cleaned != "", cleaned)


def strip_accents(column: ColumnOrName) -> Column:
    """Replace accented letters (``á à â ã ä é ê ë í ó ô õ ú ü ç ñ``, both cases) by plain ones."""
    return F.translate(_col(column), ACCENTED, UNACCENTED)


def normalize_key(column: ColumnOrName) -> Column:
    """Comparison key: `clean_string`, accents stripped, upper case.

    ``" São  Paulo "`` -> ``"SAO PAULO"``.
    """
    return F.upper(strip_accents(clean_string(column)))


# --- identifiers -------------------------------------------------------------------------------


def strip_mask(column: ColumnOrName) -> Column:
    """Remove masks and whitespace (``.``, ``/``, ``-``) and upper-case; empty -> ``null``."""
    stripped = F.upper(F.regexp_replace(_col(column), MASK_PATTERN, ""))
    return F.when(stripped != "", stripped)


def _pad_identifier(column: ColumnOrName, width: int, alphanumeric: bool) -> Column:
    """Numeric values up to `width` are left-padded with zeros; alphanumeric ones (when allowed)
    must have exactly `width` characters; anything else -> ``null``."""
    value = strip_mask(column)
    padded = F.when(value.rlike(rf"^[0-9]{{1,{width}}}$"), F.lpad(value, width, "0"))
    if alphanumeric:
        padded = padded.when(value.rlike(rf"^[0-9A-Z]{{{width}}}$"), value)
    return padded


def normalize_cnpj_basico(column: ColumnOrName) -> Column:
    """CNPJ base (8 characters, alphanumeric allowed)."""
    return _pad_identifier(column, CNPJ_BASICO_LENGTH, alphanumeric=True)


def normalize_cnpj_ordem(column: ColumnOrName) -> Column:
    """CNPJ order / establishment number (4 characters, alphanumeric allowed)."""
    return _pad_identifier(column, CNPJ_ORDEM_LENGTH, alphanumeric=True)


def normalize_cnpj_dv(column: ColumnOrName) -> Column:
    """CNPJ check digits (2 digits, always numeric)."""
    return _pad_identifier(column, CNPJ_DV_LENGTH, alphanumeric=False)


def _mod11(total: Column) -> Column:
    """Module 11 check digit: remainder 0 or 1 -> 0, otherwise 11 - remainder."""
    return ((F.lit(11) - total % 11) % 11) % 10


def _root_check_digits(root: Column) -> Column:
    """Check digits of a validated 12-character root + order (one reference per character)."""
    codes = [F.ascii(F.substring(root, position, 1)) - 48 for position in range(1, 13)]
    total1 = F.lit(0)
    total2 = F.lit(0)
    for code, weight1, weight2 in zip(codes, DV1_WEIGHTS, DV2_WEIGHTS, strict=False):
        total1 = total1 + code * weight1
        total2 = total2 + code * weight2
    dv1 = _mod11(total1)
    dv2 = _mod11(total2 + dv1 * DV2_WEIGHTS[-1])
    return F.concat(dv1.cast("string"), dv2.cast("string"))


def cnpj_check_digits(column: ColumnOrName) -> Column:
    """Two check digits (string ``"35"``) of a 12-character CNPJ root + order; else ``null``.

    Works for the numeric and the alphanumeric CNPJ (Receita Federal, from July 2026).
    """
    value = strip_mask(column)
    return _root_check_digits(F.when(value.rlike(r"^[0-9A-Z]{12}$"), value))


def _cnpj_format(column: ColumnOrName) -> Column:
    """14-character CNPJ without mask (numeric left-padded), check digits not validated."""
    value = strip_mask(column)
    return F.when(
        value.rlike(rf"^[0-9]{{1,{CNPJ_LENGTH}}}$"), F.lpad(value, CNPJ_LENGTH, "0")
    ).when(value.rlike(r"^[0-9A-Z]{12}[0-9]{2}$"), value)


def is_valid_cnpj(column: ColumnOrName) -> Column:
    """True when the value is a well-formed CNPJ with correct check digits; ``null`` input ->
    ``null``; any other value -> false (usable as a quality flag)."""
    value = _cnpj_format(column)
    valid = F.coalesce(
        F.substring(value, 13, 2) == _root_check_digits(F.substring(value, 1, 12)), F.lit(False)
    )
    return F.when(_col(column).isNotNull(), valid)


def normalize_cnpj(column: ColumnOrName, validate: bool = True) -> Column:
    """Full CNPJ (masked or not) -> 14 characters without mask, upper case.

    With ``validate=True`` (default) a CNPJ whose check digits do not match becomes ``null``.
    """
    value = _cnpj_format(column)
    return F.when(is_valid_cnpj(column), value) if validate else value


def build_cnpj(basico: ColumnOrName, ordem: ColumnOrName, dv: ColumnOrName) -> Column:
    """RN-01 business key: base + order + check digits (each normalized); ``null`` if any part
    is invalid. The check digits are not validated here (see `is_valid_cnpj`)."""
    return F.concat(
        normalize_cnpj_basico(basico), normalize_cnpj_ordem(ordem), normalize_cnpj_dv(dv)
    )


def split_cnpj(column: ColumnOrName, validate: bool = True) -> tuple[Column, Column, Column]:
    """Full CNPJ -> (base, order, check digits) columns, as in the bronze layout."""
    value = normalize_cnpj(column, validate=validate)
    return (
        F.substring(value, 1, CNPJ_BASICO_LENGTH),
        F.substring(value, CNPJ_BASICO_LENGTH + 1, CNPJ_ORDEM_LENGTH),
        F.substring(value, CNPJ_BASICO_LENGTH + CNPJ_ORDEM_LENGTH + 1, CNPJ_DV_LENGTH),
    )


# --- CNAE --------------------------------------------------------------------------------------


def normalize_cnae(column: ColumnOrName) -> Column:
    """CNAE code: masks removed (``0111-3/01``), digits only, left-padded to 7; else ``null``."""
    return _pad_identifier(column, CNAE_LENGTH, alphanumeric=False)


def normalize_cnae_list(column: ColumnOrName) -> Column:
    """Comma-separated CNAE codes -> ``array<string>`` of normalized, distinct codes in source
    order; invalid codes are dropped; ``null``/empty -> empty array."""
    codes = F.transform(
        F.split(F.coalesce(_col(column), F.lit("")), LIST_SEPARATOR), normalize_cnae
    )
    return F.array_distinct(F.filter(codes, lambda code: code.isNotNull()))


# --- dates and numbers -------------------------------------------------------------------------


def parse_date(column: ColumnOrName) -> Column:
    """``yyyyMMdd`` -> ``date``; missing (``""``, ``0``, ``00000000``), malformed or impossible
    dates -> ``null``. Never raises, whatever ``spark.sql.ansi.enabled`` or the time parser
    policy (no datetime parser is used)."""
    value = F.trim(_col(column))
    digits = F.when(value.rlike(r"^[0-9]{8}$") & ~value.isin(*EMPTY_DATE_VALUES), value)
    year = F.substring(digits, 1, 4).cast("int")
    month = F.substring(digits, 5, 2).cast("int")
    day = F.substring(digits, 7, 2).cast("int")
    leap = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
    days_in_month = (
        F.when(month == 2, F.when(leap, 29).otherwise(28))
        .when(month.isin(4, 6, 9, 11), 30)
        .otherwise(31)
    )
    valid = (year >= 1) & month.between(1, 12) & day.between(1, days_in_month)
    return F.when(valid, F.make_date(year, month, day))


def parse_decimal(column: ColumnOrName, data_type: DecimalType = MONEY) -> Column:
    """Comma-decimal amount (``1000,50``) -> ``decimal(20,2)``; malformed -> ``null``."""
    value = F.trim(_col(column))
    return F.when(
        value.rlike(_MONEY_PATTERN), F.regexp_replace(value, DECIMAL_SEPARATOR, ".").cast(data_type)
    )


# --- DataFrame helper --------------------------------------------------------------------------

# Identifier columns with a dedicated normalizer; other string columns get `clean_string`.
COLUMN_NORMALIZERS: Mapping[str, Normalizer] = MappingProxyType(
    {
        "cnpj_basico": normalize_cnpj_basico,
        "cnpj_ordem": normalize_cnpj_ordem,
        "cnpj_dv": normalize_cnpj_dv,
        "cnae_fiscal_principal": normalize_cnae,
    }
)


def apply_silver_types(df: DataFrame, table: str) -> DataFrame:
    """Bronze DataFrame (all strings) -> the documented target types of `table` (ADR 008).

    ``date`` columns use `parse_date`, ``decimal`` `parse_decimal`, ``array`` (the secondary
    CNAEs) `normalize_cnae_list`, identifier columns their normalizer and every other string
    `clean_string`. Columns that are not in the table spec (bronze metadata) are kept as is,
    and the column order does not change. Deduplication is not done here (task 3.2).
    """
    types = get_table(table).silver_types()
    columns: list[Column] = []
    for name in df.columns:
        target = types.get(name)
        source = F.col(name)
        if target is None:
            converted = source
        elif isinstance(target, DateType):
            converted = parse_date(source)
        elif isinstance(target, DecimalType):
            converted = parse_decimal(source, target)
        elif isinstance(target, ArrayType):
            converted = normalize_cnae_list(source)
        else:
            converted = COLUMN_NORMALIZERS.get(name, clean_string)(source)
        columns.append(converted.alias(name))
    return df.select(*columns)
