"""RF-04 idempotency helper: `table_fingerprint` (pure, in-memory DataFrames, no Delta)."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from cnpj_lakehouse.fingerprint import fingerprint_columns, table_fingerprint

COLUMNS = ["cnpj_basico", "razao_social", "competencia", "_ingest_ts", "_source_file", "_row_hash"]
TS = datetime(2026, 10, 1, 8, 0)


def frame(spark: SparkSession, rows: list[tuple[object, ...]], partitions: int = 1) -> DataFrame:
    schema = (
        "cnpj_basico string, razao_social string, competencia string, _ingest_ts timestamp,"
        " _source_file string, _row_hash string"
    )
    return spark.createDataFrame(rows, schema).repartition(partitions)


ROWS = [
    ("10000001", "A", "2026-09", TS, "file:/a.csv", "h1"),
    ("10000002", "B", "2026-09", TS, "file:/a.csv", "h2"),
    ("10000003", None, "2026-10", TS, "file:/b.csv", "h3"),
]


def test_same_rows_in_any_order_partitioning_or_column_order_match(spark: SparkSession) -> None:
    reference = table_fingerprint(frame(spark, ROWS))
    assert reference.rows == 3 and len(reference.digest) == 64
    assert table_fingerprint(frame(spark, ROWS[::-1], partitions=3)) == reference
    assert table_fingerprint(frame(spark, ROWS).select(*COLUMNS[::-1])) == reference
    assert reference.as_dict() == {"rows": 3, "digest": reference.digest}


def test_volatile_metadata_is_ignored_but_state_columns_are_not(spark: SparkSession) -> None:
    reference = table_fingerprint(frame(spark, ROWS))
    reloaded = [(*r[:3], datetime(2026, 11, 1), "file:/other.csv", r[5]) for r in ROWS]
    assert table_fingerprint(frame(spark, reloaded)) == reference
    for index in (1, 2, 5):  # business column, competencia, _row_hash
        changed = [tuple("X" if i == index else v for i, v in enumerate(ROWS[0])), *ROWS[1:]]
        assert table_fingerprint(frame(spark, changed)).digest != reference.digest
    # null and empty string differ
    empty = [*ROWS[:2], ("10000003", "", "2026-10", TS, "file:/b.csv", "h3")]
    assert table_fingerprint(frame(spark, empty)).digest != reference.digest
    # nothing excluded: the load metadata now counts
    everything = table_fingerprint(frame(spark, reloaded), exclude=())
    assert everything.digest != table_fingerprint(frame(spark, ROWS), exclude=()).digest


def test_duplicated_rows_and_missing_rows_change_the_fingerprint(spark: SparkSession) -> None:
    reference = table_fingerprint(frame(spark, ROWS))
    doubled = table_fingerprint(frame(spark, [*ROWS, ROWS[0]]))
    assert doubled.rows == 4 and doubled.digest != reference.digest
    # two copies of A + no B is not mistaken for A + B (a sum, not an XOR, of row hashes)
    swapped = table_fingerprint(frame(spark, [ROWS[0], ROWS[0], ROWS[2]]))
    assert swapped.rows == 3 and swapped.digest != reference.digest


def test_empty_table_and_columns(spark: SparkSession) -> None:
    empty = table_fingerprint(frame(spark, []))
    assert empty.rows == 0
    assert empty != table_fingerprint(frame(spark, []).drop("_row_hash"))  # schema counts
    assert fingerprint_columns(frame(spark, [])) == [
        "_row_hash",
        "cnpj_basico",
        "competencia",
        "razao_social",
    ]
    with pytest.raises(ValueError, match="no column"):
        table_fingerprint(frame(spark, []).select("_ingest_ts"))
