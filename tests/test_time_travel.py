"""RF-06 time travel tests (Delta): history, versionAsOf N-1, timestampAsOf, reproducibility.

Same Delta-enabled local session as `test_silver_merge.py` (marker ``delta``, own pytest run).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import test_silver as base
from pyspark.sql import SparkSession
from test_silver_merge import MONTH1, MONTH2, batch, company, merge, pytestmark, spark

from cnpj_lakehouse import time_travel as tt
from cnpj_lakehouse.bronze import INGEST_TS

__all__ = ["pytestmark", "spark"]  # module-scoped Delta session and markers, reused


@pytest.fixture()
def two_runs(spark: SparkSession, tmp_path: Path) -> tuple[str, dict[str, object]]:
    """Month 1 (create, recorded right after the run), then month 2 (MERGE: 1 update, 1 insert)."""
    path = (tmp_path / "empresas").as_posix()
    month1 = [company("10000001", "A"), company("10000002", "B"), company("10000003", "C")]
    month2 = [
        {**company("10000002", "B LTDA"), INGEST_TS: base.TS2},
        {**company("10000004", "D"), "porte_empresa": "03", INGEST_TS: base.TS2},
    ]
    merge(spark, batch(spark, "empresas", month1, MONTH1), path)
    recorded = {"version": 0, **tt.snapshot(tt.read_version(spark, path, 0))}
    merge(spark, batch(spark, "empresas", month2, MONTH2), path)
    return path, recorded


def test_history_lists_the_create_and_the_merge(
    spark: SparkSession, two_runs: tuple[str, dict[str, object]]
) -> None:
    commits = tt.history(spark, two_runs[0])
    assert [(c["version"], c["operation"]) for c in commits] == [(0, "WRITE"), (1, "MERGE")]
    assert commits[0]["output_rows"] == 3
    assert (commits[1]["source_rows"], commits[1]["inserted"], commits[1]["updated"]) == (2, 1, 1)
    assert commits[0]["timestamp"] <= commits[1]["timestamp"]


def test_version_and_timestamp_as_of_reproduce_the_recorded_snapshot(
    spark: SparkSession, two_runs: tuple[str, dict[str, object]], tmp_path: Path
) -> None:
    path, recorded = two_runs
    report = tt.demo(spark, path, "empresas", recorded)
    assert report["ok"] and set(report["checks"]) == {
        "timestamp_as_of_equals_version_as_of",
        "version_as_of_reproduces_recorded",
    }
    assert (report["previous"]["version"], report["latest"]["version"]) == (0, 1)
    assert (report["previous"]["rows"], report["latest"]["rows"]) == (3, 4)
    assert report["previous"]["by"]["porte_empresa"] == {"01": 3}
    assert report["latest"]["by"]["porte_empresa"] == {"01": 3, "03": 1}
    assert report["previous"]["checksum"] != report["latest"]["checksum"]
    assert report["changes"] == {"changed": 1, "only_new": 1, "only_old": 0}
    # timestampAsOf of the version 0 commit resolves to version 0, not to the latest one
    ts = report["history"][0]["timestamp"]
    assert tt.snapshot(tt.read_timestamp(spark, path, ts)) == tt.snapshot(
        tt.read_version(spark, path, 0)
    )
    md = tt.to_markdown(report)
    assert "| 1 |" in md and "PASS: version as of reproduces recorded" in md
    assert "10000002" not in md  # aggregates only, never keys (RST-01)

    tampered = {**recorded, "rows": 99}
    assert not tt.demo(spark, path, "empresas", tampered)["ok"]


def test_cli_snapshot_then_demo(
    spark: SparkSession, tmp_path: Path, two_runs: tuple[str, dict[str, object]]
) -> None:
    path = two_runs[0]
    before, out = tmp_path / "before.json", tmp_path / "tt"
    assert tt.main(["--path", path, "--snapshot", str(before)], spark=spark) == 0
    assert json.loads(before.read_text(encoding="utf-8"))["version"] == 1
    # with no recorded snapshot, N-1 is the previous version in the history
    assert tt.main(["--path", path, "--out", str(out)], spark=spark) == 0
    report = json.loads((out / "time-travel.json").read_text(encoding="utf-8"))
    assert report["previous"]["version"] == 0 and report["ok"]
    assert (out / "time-travel.md").read_text(encoding="utf-8").startswith("### Time travel")
    with pytest.raises(ValueError, match="older than the latest"):
        tt.main(["--path", path, "--expected", str(before), "--out", str(out)], spark=spark)


def test_demo_needs_two_versions(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "one").as_posix()
    merge(spark, batch(spark, "empresas", [company("10000001", "A")], MONTH1), path)
    with pytest.raises(ValueError, match="at least 2"):
        tt.demo(spark, path, "empresas")
