"""RF-02 source x target row counts on a versioned synthetic release (``tests/fixtures/release``).

The fixture is laid out like a ``make download`` output: ``2026-09/`` holds the extracted CSVs
and a ``manifest.json`` in the `cnpj_lakehouse.download` format, so the test goes through the
real entry path (`bronze.run` reading the manifest). Every record is synthetic (RST-01): CNPJ
roots from a block proven absent from the real registry and invented company names (see
``tests/fixtures/README.md``). The files keep the real format: Latin-1, ``;``, every field quoted,
doubled inner quotes, a quoted ``;``, empty ``""`` fields and the three forms of a missing date
(``00000000``, ``0``, empty). Unlike `cnpj_lakehouse.download`, where they describe the zips, the
manifest ``size_bytes``/``sha256`` here describe the extracted CSVs (the fixture has no zips).
Parquet is written here; the Delta path is proven by the CI ``bronze`` job.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cnpj_lakehouse.bronze import (
    INGEST_TS,
    PARTITION_COLUMN,
    SOURCE_FILE,
    count_source_lines,
    run,
    source_files_by_table,
)
from cnpj_lakehouse.download import MANIFEST_NAME, load_manifest
from cnpj_lakehouse.schemas import get_table

# Writing files with local Spark on Windows needs winutils (HADOOP_HOME); CI runs on Linux.
needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="local Spark writes on Windows need HADOOP_HOME/winutils",
)

FIXTURE_RAW = Path(__file__).parent / "fixtures" / "release"
COMPETENCIA = "2026-09"
# Lines per table in the fixture (the test also counts them from the files).
EXPECTED_LINES = {"empresas": 10, "simples": 5, "cnaes": 4}


def _source_files() -> dict[str, list[Path]]:
    month = FIXTURE_RAW / COMPETENCIA
    return source_files_by_table(load_manifest(month / MANIFEST_NAME), month)


def _month(spark: SparkSession, out: Path, table: str) -> DataFrame:
    df = spark.read.parquet(str(out / table))
    return df.where(F.col(PARTITION_COLUMN) == COMPETENCIA)


def test_fixture_is_a_small_latin1_release_with_the_real_edge_cases() -> None:
    files = _source_files()
    assert list(files) == ["empresas", "simples", "cnaes"]
    manifest = json.loads((FIXTURE_RAW / COMPETENCIA / MANIFEST_NAME).read_text("utf-8"))
    assert manifest["competencia"] == COMPETENCIA
    raw = b"".join(path.read_bytes() for paths in files.values() for path in paths)
    assert len(raw) < 4096
    assert b"\r" not in raw  # Git must not rewrite the line endings (.gitattributes: -text)
    with pytest.raises(UnicodeDecodeError):
        raw.decode("utf-8")  # Latin-1 bytes, as in the real release
    text = raw.decode("latin-1")
    for edge_case in ['""BOA VIAGEM""', '"COMÉRCIO; SERVIÇOS', ';"";', '"00000000"', ';"0";']:
        assert edge_case in text
    for table, paths in files.items():
        assert count_source_lines(paths) == (EXPECTED_LINES[table], 0)


@needs_hadoop_fs
def test_bronze_row_counts_equal_the_source_lines(spark: SparkSession, tmp_path: Path) -> None:
    out = tmp_path / "bronze"
    reports = run(spark, COMPETENCIA, FIXTURE_RAW, str(out), fmt="parquet")
    files = _source_files()

    assert [r.table for r in reports] == list(EXPECTED_LINES)
    ingest_ts = {r.ingest_ts for r in reports}
    assert len(ingest_ts) == 1  # one timestamp for the whole run
    for report in reports:
        source_lines, odd_quote_lines = count_source_lines(files[report.table])
        assert odd_quote_lines == 0
        assert source_lines == EXPECTED_LINES[report.table]
        assert report.rows_read == report.rows_written == source_lines
        assert not report.sampled

        df = _month(spark, out, report.table)
        assert df.count() == source_lines
        assert df.columns[: len(get_table(report.table).columns)] == list(
            get_table(report.table).column_names
        )


@needs_hadoop_fs
def test_bronze_metadata_columns_are_filled(spark: SparkSession, tmp_path: Path) -> None:
    out = tmp_path / "bronze"
    ts = datetime(2026, 10, 17, 9, 0, tzinfo=UTC)
    run(spark, COMPETENCIA, FIXTURE_RAW, str(out), fmt="parquet", ingest_ts=ts)

    for table, paths in _source_files().items():
        df = _month(spark, out, table)
        stats = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.count(INGEST_TS).alias("ingest_ts_filled"),
            F.count(SOURCE_FILE).alias("source_file_filled"),
            F.countDistinct(INGEST_TS).alias("ingest_ts_distinct"),
        ).first()
        assert stats is not None
        assert stats["rows"] == stats["ingest_ts_filled"] == stats["source_file_filled"] > 0
        assert stats["ingest_ts_distinct"] == 1
        stamps = {r[0] for r in df.select(F.col(INGEST_TS).cast("string")).collect()}
        assert stamps == {"2026-10-17 09:00:00"}  # the session runs in UTC

        # `_source_file` is the URI of a versioned fixture CSV listed in the manifest.
        sources = {r[0].replace("%24", "$") for r in df.select(SOURCE_FILE).distinct().collect()}
        assert {s.rsplit("/", 1)[-1] for s in sources} == {p.name for p in paths}
        assert all(f"/fixtures/release/{COMPETENCIA}/" in s for s in sources)


@needs_hadoop_fs
def test_bronze_rerun_replaces_the_month_without_duplicates(
    spark: SparkSession, tmp_path: Path
) -> None:
    out = tmp_path / "bronze"
    first = datetime(2026, 10, 17, 9, 0, tzinfo=UTC)
    second = datetime(2026, 10, 17, 10, 0, tzinfo=UTC)
    run(spark, COMPETENCIA, FIXTURE_RAW, str(out), fmt="parquet", ingest_ts=first)
    reports = run(spark, COMPETENCIA, FIXTURE_RAW, str(out), fmt="parquet", ingest_ts=second)

    for report in reports:
        df = _month(spark, out, report.table)
        assert report.rows_written == df.count() == EXPECTED_LINES[report.table]
        stamps = {r[0] for r in df.select(F.col(INGEST_TS).cast("string")).collect()}
        assert stamps == {"2026-10-17 10:00:00"}


@needs_hadoop_fs
def test_bronze_keeps_quoted_values_and_latin1_text(spark: SparkSession, tmp_path: Path) -> None:
    out = tmp_path / "bronze"
    run(spark, COMPETENCIA, FIXTURE_RAW, str(out), fmt="parquet", tables=["empresas", "cnaes"])

    names = {r[0] for r in _month(spark, out, "empresas").select("razao_social").collect()}
    assert 'OFICINA "BOA VIAGEM" ME' in names  # doubled inner quotes are unescaped
    assert "COMÉRCIO; SERVIÇOS & CIA EXEMPLO" in names  # a quoted ";" stays in the value
    assert 'LOJA "O BARATÃO"; FILIAL ÚNICA' in names
    assert "AÇAÍ & PÃO DE QUEIJO FICTÍCIOS EIRELI" in names  # Latin-1 decoded, not mojibake
    assert not any("�" in n or "Ã" in n for n in names if n)
    cnaes = {r[0] for r in _month(spark, out, "cnaes").select("descricao").collect()}
    assert 'Restaurantes e similares "à la carte"' in cnaes
