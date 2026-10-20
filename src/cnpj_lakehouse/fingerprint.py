"""RF-04 idempotency proof: an order-independent fingerprint of a table's content.

`table_fingerprint` reduces a DataFrame to its row count and a SHA-256 digest of its rows that
does not depend on row order, partitioning or column order, so "a re-run gives the same table"
becomes one comparison (tests and the CI `bronze` job, task 4.2). Only the digest and the count
are ever printed, never data (RST-01).

How the digest is built: every row is hashed with `cnpj_lakehouse.silver.row_hash` (SHA-256 of
the JSON of the columns, nulls included), the hash is split into four 64-bit words and each word
is **summed** over the rows (a multiset hash: duplicated rows count twice, unlike XOR). The four
sums and the count are hashed once more on the driver. One aggregation, no collect of rows.

Volatile metadata (``_ingest_ts``, ``_source_file``) is left out by default: it records when and
from which file a row was loaded, not what the row says. ``competencia`` (the month of the last
change of a key in silver, ADR 011) and ``_row_hash`` are part of the state and are included.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cnpj_lakehouse.bronze import INGEST_TS, SOURCE_FILE
from cnpj_lakehouse.silver import row_hash

VOLATILE_COLUMNS: tuple[str, ...] = (INGEST_TS, SOURCE_FILE)
_WORDS = 4  # 64-bit words of the SHA-256 row hash (64 hex digits)
_WORD_HEX = 16


@dataclass(frozen=True)
class Fingerprint:
    rows: int
    digest: str

    def as_dict(self) -> dict[str, int | str]:
        return asdict(self)


def fingerprint_columns(df: DataFrame, exclude: Iterable[str] = VOLATILE_COLUMNS) -> list[str]:
    """Columns that enter the fingerprint, sorted by name (column order does not matter)."""
    excluded = set(exclude)
    return sorted(c for c in df.columns if c not in excluded)


def table_fingerprint(df: DataFrame, exclude: Iterable[str] = VOLATILE_COLUMNS) -> Fingerprint:
    """Row count and order-independent content digest of `df` (see the module docstring)."""
    columns = fingerprint_columns(df, exclude)
    if not columns:
        raise ValueError("no column left to fingerprint")
    hashed = df.select(row_hash(columns).alias("h"))
    sums = [
        F.sum(
            F.conv(F.substring("h", 1 + i * _WORD_HEX, _WORD_HEX), 16, 10).cast("decimal(38,0)")
        ).alias(f"w{i}")
        for i in range(_WORDS)
    ]
    result = hashed.agg(F.count(F.lit(1)).alias("rows"), *sums).first()
    assert result is not None  # a global aggregation always returns one row
    words = [str(result[f"w{i}"] or 0) for i in range(_WORDS)]
    payload = ":".join([",".join(columns), str(result["rows"]), *words])
    return Fingerprint(int(result["rows"]), hashlib.sha256(payload.encode()).hexdigest())
