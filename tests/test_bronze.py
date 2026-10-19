"""RF-02 bronze ingestion tests: local Spark on tiny synthetic Latin-1 CSVs (never real data).

The unit tests write Parquet instead of Delta: Delta needs the `delta-spark` jars, which a plain
local session resolves through Ivy at JVM start-up (network, and only once per JVM). The Delta
path (``replaceWhere``) is proven by the CI ``bronze`` job on the Docker stack with MinIO.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from cnpj_lakehouse import bronze
from cnpj_lakehouse.bronze import (
    INGEST_TS,
    PARTITION_COLUMN,
    SOURCE_FILE,
    LayoutError,
    add_metadata,
    apply_sample,
    build_bronze,
    check_layout,
    ingest_table,
    parse_sample_fraction,
    read_raw_lines,
    read_source,
    run,
    source_files_by_table,
)
from cnpj_lakehouse.schemas import CNAES, EMPRESAS

# Writing files with local Spark on Windows needs winutils (HADOOP_HOME); CI runs on Linux.
needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="local Spark writes on Windows need HADOOP_HOME/winutils",
)

TS = datetime(2026, 10, 17, 12, 30, tzinfo=UTC)
TS_TEXT = "2026-10-17 12:30:00"

EMPRESAS_ROWS = [
    '"00000000";"BANCO DO BRASIL SA";"2038";"10";"120000000000,00";"05";"UNIAO"',
    '"00000001";"NIGHT CLUB ""TUTU"" LTDA";"2062";"49";"0,00";"01";""',
    '"00000002";"CAFÉ; AÇÚCAR & CIA";"1031";"49";"1000,50";"03";"SÃO PAULO - SP"',
]
CNAES_ROWS = ['"0111301";"Cultivo de arroz"', '"8888888";"Atividade não informada"']


def _write_latin1(path: Path, rows: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(("\n".join(rows) + "\n").encode("latin-1"))
    return path


def _release(raw: Path, competencia: str, files: dict[str, tuple[str, list[str]]]) -> Path:
    """Writes a fake release folder: CSVs plus a manifest like the one of `make download`."""
    month = raw / competencia
    entries = []
    for zip_name, (csv_name, rows) in files.items():
        _write_latin1(month / csv_name, rows)
        entries.append({"name": zip_name, "extracted": [csv_name]})
    (month / "manifest.json").write_text(
        json.dumps({"competencia": competencia, "files": entries}), encoding="utf-8"
    )
    return month


# --- pure functions -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"), [(None, 1.0), ("", 1.0), (" 0.1 ", 0.1), ("1", 1.0)]
)
def test_parse_sample_fraction(value: str | None, expected: float) -> None:
    assert parse_sample_fraction(value) == expected


@pytest.mark.parametrize("value", ["0", "-0.5", "1.5", "abc"])
def test_parse_sample_fraction_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError, match="SPARK_SAMPLE_FRACTION"):
        parse_sample_fraction(value)


def test_source_files_by_table_follows_the_manifest(tmp_path: Path) -> None:
    manifest = {
        "files": [
            {"name": "Empresas1.zip", "extracted": ["B.EMPRECSV"]},
            {"name": "Cnaes.zip", "extracted": ["F.CNAECSV"]},
            {"name": "Empresas0.zip", "extracted": ["A.EMPRECSV"]},
        ]
    }
    files = source_files_by_table(manifest, tmp_path)
    assert list(files) == ["empresas", "cnaes"]  # TABLES order
    assert files["empresas"] == [tmp_path / "A.EMPRECSV", tmp_path / "B.EMPRECSV"]
    assert source_files_by_table(manifest, tmp_path, ["cnaes"]) == {
        "cnaes": [tmp_path / "F.CNAECSV"]
    }


def test_source_files_by_table_rejects_tables_not_downloaded(tmp_path: Path) -> None:
    manifest = {"files": [{"name": "Cnaes.zip", "extracted": ["F.CNAECSV"]}]}
    with pytest.raises(ValueError, match="socios not in"):
        source_files_by_table(manifest, tmp_path, ["socios"])
    with pytest.raises(KeyError, match="unknown table"):
        source_files_by_table(manifest, tmp_path, ["nope"])


# --- layout check ---------------------------------------------------------------------------


def test_layout_check_accepts_quoted_separators_and_doubled_quotes(
    spark: SparkSession, tmp_path: Path
) -> None:
    csv = _write_latin1(tmp_path / "K3241.K03200$Y1.D60912.EMPRECSV", [*EMPRESAS_ROWS, ""])
    result = check_layout(read_raw_lines(spark, [csv]), EMPRESAS)
    assert result.ok
    assert result.lines == 3  # the empty line is not a record
    assert result.field_counts == {7: 3}


def test_layout_check_reports_extra_and_missing_fields(spark: SparkSession, tmp_path: Path) -> None:
    rows = [
        *EMPRESAS_ROWS,
        '"00000003";"EXTRA FIELD";"2062";"49";"0,00";"01";"";"new"',
        '"00000004";"MISSING FIELD";"2062";"49";"0,00";"01"',
    ]
    csv = _write_latin1(tmp_path / "E.EMPRECSV", rows)
    result = check_layout(read_raw_lines(spark, [csv]), EMPRESAS)
    assert not result.ok
    assert result.bad_lines == 2
    assert result.field_counts == {6: 1, 7: 3, 8: 1}
    assert len(result.examples) == 2
    assert any("EXTRA FIELD" in example for example in result.examples)


def test_ingest_fails_on_a_layout_change(spark: SparkSession, tmp_path: Path) -> None:
    csv = _write_latin1(tmp_path / "F.CNAECSV", [*CNAES_ROWS, '"0111302";"Milho";"extra"'])
    with pytest.raises(LayoutError, match="1 of 3 line"):
        ingest_table(spark, CNAES, [csv], "2026-09", str(tmp_path / "out"), TS, fmt="parquet")


# --- DataFrame building ---------------------------------------------------------------------


def test_build_bronze_applies_schema_and_metadata(spark: SparkSession, tmp_path: Path) -> None:
    csv = _write_latin1(tmp_path / "K3241.K03200Y1.D60912.EMPRECSV", EMPRESAS_ROWS)
    df = build_bronze(spark, EMPRESAS, [csv], "2026-09", TS)

    assert df.columns == [*EMPRESAS.column_names, INGEST_TS, SOURCE_FILE, PARTITION_COLUMN]
    assert all(
        df.schema[name].dataType.simpleString() == "string" for name in EMPRESAS.column_names
    )
    assert df.schema[INGEST_TS].dataType.simpleString() == "timestamp"
    rows = sorted(df.collect(), key=lambda r: r["cnpj_basico"])
    assert len(rows) == 3
    assert rows[1]["razao_social"] == 'NIGHT CLUB "TUTU" LTDA'
    assert rows[2]["razao_social"] == "CAFÉ; AÇÚCAR & CIA"
    assert rows[1]["ente_federativo_responsavel"] is None
    # collect() returns naive local-time datetimes: compare as text in the UTC session.
    assert {r[0] for r in df.select(df[INGEST_TS].cast("string")).collect()} == {TS_TEXT}
    assert all(r[SOURCE_FILE].endswith("K3241.K03200Y1.D60912.EMPRECSV") for r in rows)
    assert {r[PARTITION_COLUMN] for r in rows} == {"2026-09"}


def test_add_metadata_converts_the_timestamp_to_utc(spark: SparkSession, tmp_path: Path) -> None:
    csv = _write_latin1(tmp_path / "F.CNAECSV", CNAES_ROWS)
    brasilia = timezone(timedelta(hours=-3))
    df = add_metadata(read_source(spark, CNAES, [csv]), "2026-09", TS.astimezone(brasilia))
    assert {r[0] for r in df.select(df[INGEST_TS].cast("string")).collect()} == {TS_TEXT}
    with pytest.raises(ValueError, match="timezone-aware"):
        add_metadata(read_source(spark, CNAES, [csv]), "2026-09", datetime(2026, 1, 1))
    with pytest.raises(ValueError):
        add_metadata(read_source(spark, CNAES, [csv]), "2026-13", TS)


def test_apply_sample_is_deterministic(spark: SparkSession) -> None:
    df = spark.range(2000)
    assert apply_sample(df, 1.0) is df
    first = sorted(r.id for r in apply_sample(df, 0.1).collect())
    second = sorted(r.id for r in apply_sample(df, 0.1).collect())
    assert first == second
    assert 100 < len(first) < 300


# --- write and orchestration ----------------------------------------------------------------


@needs_hadoop_fs
def test_run_writes_partitioned_bronze_and_replaces_the_month_on_rerun(
    spark: SparkSession, tmp_path: Path
) -> None:
    raw = tmp_path / "raw"
    _release(
        raw,
        "2026-09",
        {
            "Empresas1.zip": ("K3241.K03200Y1.D60912.EMPRECSV", EMPRESAS_ROWS),
            "Cnaes.zip": ("F.K03200$Z.D60912.CNAECSV", CNAES_ROWS),
        },
    )
    _release(raw, "2026-10", {"Cnaes.zip": ("F.K03200$Z.D61010.CNAECSV", CNAES_ROWS[:1])})
    out = str(tmp_path / "bronze")

    reports = run(spark, "2026-09", raw, out, fmt="parquet", ingest_ts=TS)
    assert [(r.table, r.rows_read, r.rows_written) for r in reports] == [
        ("empresas", 3, 3),
        ("cnaes", 2, 2),
    ]
    assert reports[0].source_files == ("K3241.K03200Y1.D60912.EMPRECSV",)
    assert not reports[0].sampled and reports[0].ingest_ts == "2026-10-17T12:30:00Z"
    assert (tmp_path / "bronze" / "empresas" / "competencia=2026-09").is_dir()

    run(spark, "2026-10", raw, out, tables=["cnaes"], fmt="parquet", ingest_ts=TS)
    run(spark, "2026-09", raw, out, tables=["cnaes"], fmt="parquet", ingest_ts=TS)  # re-run
    cnaes = spark.read.parquet(f"{out}/cnaes")
    counts = {
        r[PARTITION_COLUMN]: r["count"] for r in cnaes.groupBy(PARTITION_COLUMN).count().collect()
    }
    assert counts == {"2026-09": 2, "2026-10": 1}  # replaced, not duplicated; other month kept
    assert cnaes.where(cnaes[SOURCE_FILE].isNull() | cnaes[INGEST_TS].isNull()).count() == 0


@needs_hadoop_fs
def test_run_with_sampling_reports_it(spark: SparkSession, tmp_path: Path) -> None:
    rows = [f'"{i:07d}";"Atividade {i}"' for i in range(500)]
    raw = tmp_path / "raw"
    _release(raw, "2026-09", {"Cnaes.zip": ("F.CNAECSV", rows)})
    (report,) = run(spark, "2026-09", raw, str(tmp_path / "b"), sample_fraction=0.2, fmt="parquet")
    assert report.sampled and report.sample_fraction == 0.2
    assert report.rows_read == 500
    assert 50 < report.rows_written < 150


def test_run_requires_a_manifest(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="make download"):
        run(spark, "2026-09", tmp_path, str(tmp_path / "b"))


def test_ingest_table_requires_files(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no source files"):
        ingest_table(spark, CNAES, [], "2026-09", str(tmp_path), TS)


# --- CLI ------------------------------------------------------------------------------------


@pytest.fixture
def cli_spark(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> SparkSession:
    """The module session, returned by `get_spark` and protected from `main`'s `stop()`."""
    monkeypatch.setattr(bronze, "get_spark", lambda *_args, **_kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    return spark


@needs_hadoop_fs
def test_main_ingests_and_writes_a_summary(
    cli_spark: SparkSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw = tmp_path / "raw"
    _release(raw, "2026-09", {"Cnaes.zip": ("F.CNAECSV", CNAES_ROWS)})
    monkeypatch.setenv("SPARK_SAMPLE_FRACTION", "1.0")
    summary = tmp_path / "build" / "summary.json"
    argv = ["--competencia", "2026-09", "--raw-dir", str(raw), "--output", str(tmp_path / "b")]
    code = bronze.main([*argv, "--format", "parquet", "--summary", str(summary)])
    assert code == 0
    data = json.loads(summary.read_text(encoding="utf-8"))
    assert data["tables"][0]["rows_written"] == 2
    assert "[bronze] cnaes: read 2, written 2" in capsys.readouterr().out


def test_main_returns_an_error_code(
    cli_spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    argv = ["--competencia", "2026-09", "--raw-dir", str(tmp_path), "--output", str(tmp_path)]
    assert bronze.main(argv) == 1  # no manifest
    monkeypatch.setenv("SPARK_SAMPLE_FRACTION", "2")
    assert bronze.main(argv) == 2
    with pytest.raises(SystemExit):
        bronze.main(["--competencia", "2026/09"])
