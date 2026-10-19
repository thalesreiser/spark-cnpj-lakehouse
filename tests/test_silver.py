"""RF-03 / RN-01 silver tests: typing, current-record choice, deduplication and quality counts.

The pure functions run on tiny in-memory DataFrames (no file I/O, so also on Windows). The
end-to-end tests run bronze and silver on the synthetic release of ``tests/fixtures/release``
with Parquet (like ``tests/test_bronze.py``); the Delta path is proven by the CI ``bronze`` job.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from cnpj_lakehouse import bronze, silver
from cnpj_lakehouse.bronze import INGEST_TS, PARTITION_COLUMN, SOURCE_FILE
from cnpj_lakehouse.schemas import TABLES, get_table
from cnpj_lakehouse.silver import (
    CNPJ_COLUMN,
    CNPJ_VALID_COLUMN,
    RULES,
    InvalidCnpjPolicy,
    SilverOptions,
    build_silver,
    date_columns,
    duplicate_keys,
    quality_counts,
    type_and_flag,
)

needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="local Spark writes on Windows need HADOOP_HOME/winutils",
)

FIXTURE_RAW = Path(__file__).parent / "fixtures" / "release"
COMPETENCIA = "2026-09"
TS1 = datetime(2026, 10, 1, 8, 0)
TS2 = datetime(2026, 10, 2, 8, 0)
DROP = SilverOptions(invalid_cnpj_policy=InvalidCnpjPolicy.DROP)
ZERO_IS_VALID = SilverOptions(zero_cnpj_is_invalid=False)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-silver")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def dv(root: str) -> str:
    """Module 11 check digits of a 12-digit root (independent of `normalize`)."""
    digits = [int(c) for c in root]
    first = _mod11(digits, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    second = _mod11([*digits, first], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return f"{first}{second}"


def _mod11(digits: list[int], weights: list[int]) -> int:
    rest = sum(d * w for d, w in zip(digits, weights, strict=True)) % 11
    return 0 if rest < 2 else 11 - rest


def bronze_df(
    spark: SparkSession, table: str, rows: list[dict[str, Any]], partitions: int = 1
) -> DataFrame:
    """Bronze-shaped DataFrame: every source column a string, plus the metadata columns."""
    spec = get_table(table)
    schema = StructType(
        [StructField(name, StringType()) for name in spec.column_names]
        + [
            StructField(INGEST_TS, TimestampType()),
            StructField(SOURCE_FILE, StringType()),
            StructField(PARTITION_COLUMN, StringType()),
        ]
    )
    defaults = {INGEST_TS: TS1, SOURCE_FILE: "file:/raw/a.csv", PARTITION_COLUMN: COMPETENCIA}
    data = [tuple({**defaults, **row}.get(f.name) for f in schema.fields) for row in rows]
    return spark.createDataFrame(data, schema).repartition(partitions)


def establishment(order: str = "0001", base: str = "80300911", **values: Any) -> dict[str, Any]:
    return {"cnpj_basico": base, "cnpj_ordem": order, "cnpj_dv": dv(base + order), **values}


def by_key(df: DataFrame, *key: str) -> dict[tuple[Any, ...], Row]:
    return {tuple(row[k] for k in key): row for row in df.collect()}


# --- rules -------------------------------------------------------------------------------------


def test_every_table_has_a_rule_with_its_key_columns() -> None:
    assert set(RULES) == set(TABLES)
    assert RULES["estabelecimentos"].key == ("cnpj_basico", "cnpj_ordem", "cnpj_dv")  # RN-01
    assert RULES["empresas"].key == RULES["simples"].key == ("cnpj_basico",)
    assert RULES["socios"].key[0] == "cnpj_basico" and len(RULES["socios"].key) == 4
    assert all(RULES[name].key == ("codigo",) for name in ("cnaes", "motivos", "paises"))


def test_ordering_ends_with_the_common_deterministic_tie_breakers() -> None:
    columns = [o.column for o in RULES["estabelecimentos"].ordering()]
    assert columns == [
        PARTITION_COLUMN,
        "data_situacao_cadastral",
        "data_inicio_atividade",
        INGEST_TS,
        SOURCE_FILE,
        "_row_hash",
    ]
    assert all(o.descending for o in RULES["estabelecimentos"].ordering())


# --- current record ----------------------------------------------------------------------------


def test_estabelecimentos_keeps_the_latest_status_date_per_rn01_key(spark: SparkSession) -> None:
    rows = [
        establishment(situacao_cadastral="02", data_situacao_cadastral="20200101"),
        establishment(situacao_cadastral="08", data_situacao_cadastral="20250630"),
        establishment(situacao_cadastral="04", data_situacao_cadastral="00000000"),
        establishment("0002", situacao_cadastral="02", data_situacao_cadastral="20190101"),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows, 3), "estabelecimentos")
    current = by_key(out, "cnpj_ordem")
    assert out.count() == 2 and duplicate_keys(out, "estabelecimentos") == 0
    assert current[("0001",)]["situacao_cadastral"] == "08"
    assert current[("0001",)]["data_situacao_cadastral"] == date(2025, 6, 30)
    assert current[("0001",)][CNPJ_COLUMN] == "80300911" + "0001" + dv("803009110001")
    assert current[("0001",)][CNPJ_VALID_COLUMN] is True
    assert isinstance(out.schema["data_situacao_cadastral"].dataType, DateType)


def test_later_competencia_wins_over_a_later_date(spark: SparkSession) -> None:
    rows = [
        establishment(
            nome_fantasia="OLD", data_situacao_cadastral="20250101", competencia="2026-08"
        ),
        establishment(nome_fantasia="NEW", data_situacao_cadastral="20200101"),
    ]
    out = build_silver(bronze_df(spark, "estabelecimentos", rows), "estabelecimentos")
    assert [row["nome_fantasia"] for row in out.collect()] == ["NEW"]


def test_later_ingest_ts_breaks_a_date_tie(spark: SparkSession) -> None:
    rows = [
        {"cnpj_basico": "80300911", "razao_social": "SECOND", INGEST_TS: TS2},
        {"cnpj_basico": "80300911", "razao_social": "FIRST", INGEST_TS: TS1},
    ]
    out = build_silver(bronze_df(spark, "empresas", rows), "empresas")
    assert [row["razao_social"] for row in out.collect()] == ["SECOND"]


def test_full_ties_are_resolved_the_same_way_whatever_the_row_order(spark: SparkSession) -> None:
    rows = [{"cnpj_basico": "80300911", "razao_social": f"NAME {i}"} for i in range(8)]
    picked = {
        build_silver(bronze_df(spark, "empresas", order, parts), "empresas").first()["razao_social"]
        for order, parts in ((rows, 1), (rows[::-1], 3), (rows[3:] + rows[:3], 4))
    }
    assert len(picked) == 1


def test_simples_keeps_the_row_with_the_latest_event(spark: SparkSession) -> None:
    rows = [
        {"cnpj_basico": "80300911", "opcao_pelo_simples": "S", "data_opcao_simples": "20180101"},
        {
            "cnpj_basico": "80300911",
            "opcao_pelo_simples": "N",
            "data_opcao_simples": "20180101",
            "data_exclusao_simples": "20231231",
        },
        {"cnpj_basico": "803009", "opcao_pelo_simples": "S"},  # left-padded to 00803009
    ]
    out = build_silver(bronze_df(spark, "simples", rows), "simples")
    current = by_key(out, "cnpj_basico")
    assert current[("80300911",)]["opcao_pelo_simples"] == "N"
    assert ("00803009",) in current and "_latest_event" not in out.columns


def test_socios_key_keeps_different_partners_and_collapses_repeats(spark: SparkSession) -> None:
    partner = {"cnpj_basico": "80300911", "identificador_socio": "2", "cpf_cnpj_socio": "***1**"}
    rows = [
        {**partner, "nome_socio_razao_social": "SOCIO A", "data_entrada_sociedade": "20100101"},
        {**partner, "nome_socio_razao_social": "SOCIO A", "data_entrada_sociedade": "20200101"},
        {**partner, "nome_socio_razao_social": "SOCIO B", "data_entrada_sociedade": "20100101"},
        # foreign partners have no CPF: null key parts form one group
        {"cnpj_basico": "80300911", "identificador_socio": "3", "nome_socio_razao_social": "X"},
        {"cnpj_basico": "80300911", "identificador_socio": "3", "nome_socio_razao_social": "X"},
    ]
    out = build_silver(bronze_df(spark, "socios", rows), "socios")
    assert out.count() == 3 and duplicate_keys(out, "socios") == 0
    names = by_key(out, "nome_socio_razao_social")
    assert names[("SOCIO A",)]["data_entrada_sociedade"] == date(2020, 1, 1)


def test_lookups_keep_one_row_per_code(spark: SparkSession) -> None:
    rows = [
        {"codigo": "0111301", "descricao": "Cultivo de arroz"},
        {"codigo": "0111301", "descricao": "Cultivo de arroz", SOURCE_FILE: "file:/raw/b.csv"},
        {"codigo": " 01 ", "descricao": "Motivo"},
        {"codigo": "", "descricao": "Sem codigo"},
    ]
    flagged = type_and_flag(bronze_df(spark, "cnaes", rows), "cnaes")
    counts = quality_counts(flagged, "cnaes")
    out = build_silver(bronze_df(spark, "cnaes", rows), "cnaes")
    assert sorted(row["codigo"] for row in out.collect()) == ["01", "0111301"]
    assert by_key(out, "codigo")[("0111301",)][SOURCE_FILE] == "file:/raw/b.csv"
    assert counts.rows_in == 4 and counts.rows_without_key == 1
    assert counts.rows_deduplicated_in == 3 and counts.date_values == {}


# --- invalid CNPJs (ADR 010) -------------------------------------------------------------------


def _cnpj_rows() -> list[dict[str, Any]]:
    good = establishment()
    return [
        good,
        {**establishment("0002"), "cnpj_dv": "00" if dv("803009110002") != "00" else "11"},
        {"cnpj_basico": "00000000", "cnpj_ordem": "0000", "cnpj_dv": "00"},
        {"cnpj_basico": "80300911", "cnpj_ordem": "0003", "cnpj_dv": "X"},  # no key
        # Banco do Brasil pattern: all-zero root but a real order and DV, a valid CNPJ
        {"cnpj_basico": "00000000", "cnpj_ordem": "0001", "cnpj_dv": "91"},
    ]


def test_invalid_cnpjs_are_flagged_and_kept_by_default(spark: SparkSession) -> None:
    df = bronze_df(spark, "estabelecimentos", _cnpj_rows())
    counts = quality_counts(type_and_flag(df, "estabelecimentos"), "estabelecimentos")
    out = by_key(build_silver(df, "estabelecimentos"), CNPJ_COLUMN)
    assert len(out) == 4
    assert out[("00000000000000",)][CNPJ_VALID_COLUMN] is False
    assert out[("00000000000191",)][CNPJ_VALID_COLUMN] is True
    assert sum(1 for row in out.values() if row[CNPJ_VALID_COLUMN] is False) == 2
    assert (counts.invalid_cnpj, counts.zero_cnpj, counts.invalid_cnpj_dropped) == (2, 1, 0)
    assert counts.rows_without_key == 1 and counts.rows_deduplicated_in == 4


def test_drop_policy_discards_and_counts_invalid_cnpjs(spark: SparkSession) -> None:
    df = bronze_df(spark, "estabelecimentos", _cnpj_rows())
    counts = quality_counts(type_and_flag(df, "estabelecimentos", DROP), "estabelecimentos", DROP)
    out = build_silver(df, "estabelecimentos", DROP)
    assert sorted(row[CNPJ_COLUMN] for row in out.collect()) == [
        "00000000000191",
        "80300911" + "0001" + dv("803009110001"),
    ]
    assert counts.invalid_cnpj_dropped == 2 and counts.rows_deduplicated_in == 2


def test_all_zero_cnpj_rule_can_be_switched_off(spark: SparkSession) -> None:
    df = bronze_df(spark, "estabelecimentos", _cnpj_rows())
    flagged = type_and_flag(df, "estabelecimentos", ZERO_IS_VALID)
    counts = quality_counts(flagged, "estabelecimentos", ZERO_IS_VALID)
    out = by_key(build_silver(df, "estabelecimentos", ZERO_IS_VALID), CNPJ_COLUMN)
    assert out[("00000000000000",)][CNPJ_VALID_COLUMN] is True  # mod 11 accepts it
    assert (counts.invalid_cnpj, counts.zero_cnpj) == (1, 1)


def test_non_establishment_tables_have_no_cnpj_flag(spark: SparkSession) -> None:
    out = build_silver(bronze_df(spark, "empresas", [{"cnpj_basico": "1"}]), "empresas")
    assert CNPJ_VALID_COLUMN not in out.columns
    assert [row["cnpj_basico"] for row in out.collect()] == ["00000001"]


# --- dates -------------------------------------------------------------------------------------


def test_dates_are_typed_and_parse_failures_counted(spark: SparkSession) -> None:
    rows = [
        {"cnpj_basico": "1", "data_opcao_simples": "20240229", "data_exclusao_simples": "00000000"},
        {"cnpj_basico": "2", "data_opcao_simples": "20251340", "data_exclusao_simples": "0"},
        {"cnpj_basico": "3", "data_opcao_simples": "2025011", "data_opcao_mei": ""},
        {"cnpj_basico": "4", "data_opcao_simples": "20190315", "data_opcao_mei": "20190315"},
    ]
    df = bronze_df(spark, "simples", rows)
    counts = quality_counts(type_and_flag(df, "simples"), "simples")
    out = build_silver(df, "simples")
    assert date_columns("simples") == (
        "data_opcao_simples",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_exclusao_mei",
    )
    assert all(isinstance(out.schema[c].dataType, DateType) for c in date_columns("simples"))
    assert counts.date_values["data_opcao_simples"] == 4
    assert counts.date_parse_failures == {
        "data_opcao_simples": 2,
        "data_exclusao_simples": 0,
        "data_opcao_mei": 0,
        "data_exclusao_mei": 0,
    }
    # 100% of the valid non-empty source dates are typed
    typed = sum(1 for row in out.collect() if row["data_opcao_simples"] is not None)
    assert typed == counts.date_values["data_opcao_simples"] - 2


def test_silver_columns_drop_every_helper(spark: SparkSession) -> None:
    out = build_silver(bronze_df(spark, "estabelecimentos", [establishment()]), "estabelecimentos")
    spec = get_table("estabelecimentos").column_names
    assert out.columns == [
        *spec,
        INGEST_TS,
        SOURCE_FILE,
        PARTITION_COLUMN,
        CNPJ_COLUMN,
        CNPJ_VALID_COLUMN,
    ]


# --- CLI ---------------------------------------------------------------------------------------


class _FakeSpark:
    stopped = False

    def stop(self) -> None:
        self.stopped = True


def test_main_writes_the_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _FakeSpark()
    seen: dict[str, Any] = {}
    report = silver.SilverReport(
        "empresas",
        COMPETENCIA,
        "s3a://b/silver/empresas",
        ("cnpj_basico",),
        10,
        8,
        1,
        1,
        0,
        0,
        "drop",
        0,
        {},
        {},
    )

    def fake_run(spark: Any, competencia: str, bronze_root: str, output_root: str, **kw: Any):
        seen.update(bronze=bronze_root, output=output_root, **kw)
        return [report]

    monkeypatch.setattr(silver, "get_spark", lambda *a, **k: fake)
    monkeypatch.setattr(silver, "run", fake_run)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "b")
    out = tmp_path / "silver.json"
    code = silver.main(
        ["--competencia", COMPETENCIA, "--invalid-cnpj", "drop", "--summary", str(out)]
    )
    assert code == 0 and fake.stopped
    assert seen["bronze"] == "s3a://b/bronze" and seen["output"] == "s3a://b/silver"
    assert seen["options"].invalid_cnpj_policy is InvalidCnpjPolicy.DROP
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["invalid_cnpj_policy"] == "drop" and data["tables"][0]["rows_out"] == 8


def test_main_returns_1_on_a_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*a: Any, **k: Any) -> None:
        raise ValueError("no bronze rows")

    monkeypatch.setattr(silver, "get_spark", lambda *a, **k: _FakeSpark())
    monkeypatch.setattr(silver, "run", fail)
    assert silver.main(["--competencia", COMPETENCIA]) == 1


# --- end to end on the synthetic release (Parquet) ---------------------------------------------


@pytest.fixture(scope="module")
def lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("lake")
    ts = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)
    bronze.run(
        spark, COMPETENCIA, FIXTURE_RAW, (root / "bronze").as_posix(), fmt="parquet", ingest_ts=ts
    )
    return root


@needs_hadoop_fs
def test_run_builds_every_bronze_table_of_the_fixture(spark: SparkSession, lake: Path) -> None:
    reports = silver.run(
        spark,
        COMPETENCIA,
        (lake / "bronze").as_posix(),
        (lake / "silver").as_posix(),
        fmt="parquet",
    )
    by_table = {r.table: r for r in reports}
    assert set(by_table) == {"empresas", "simples", "cnaes"}
    for r in reports:
        out = spark.read.parquet(f"{lake.as_posix()}/silver/{r.table}")
        assert out.count() == r.rows_out > 0
        assert duplicate_keys(out, r.table) == 0
        assert r.rows_in == r.rows_out + r.duplicates_removed + r.rows_without_key
        for name in date_columns(r.table):
            assert isinstance(out.schema[name].dataType, DateType)
            typed = out.where(out[name].isNotNull()).count()
            assert typed == r.date_values[name] - r.date_parse_failures[name]
    assert by_table["simples"].date_values["data_opcao_simples"] > 0


@needs_hadoop_fs
def test_rerun_replaces_the_month_and_requested_tables_must_exist(
    spark: SparkSession, lake: Path
) -> None:
    args = (spark, COMPETENCIA, (lake / "bronze").as_posix(), (lake / "silver2").as_posix())
    first = silver.run(*args, tables=["cnaes"], fmt="parquet")
    second = silver.run(*args, tables=["cnaes"], fmt="parquet")
    assert first[0].rows_out == second[0].rows_out
    assert spark.read.parquet(f"{lake.as_posix()}/silver2/cnaes").count() == first[0].rows_out
    with pytest.raises(ValueError, match="socios"):
        silver.run(*args, tables=["socios"], fmt="parquet")
    with pytest.raises(ValueError, match="no bronze table"):
        silver.run(spark, "2020-01", *args[2:], fmt="parquet")
