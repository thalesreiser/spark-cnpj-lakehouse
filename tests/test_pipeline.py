"""RNF-05: orchestration of ``make run`` (steps mocked: order, stop on failure, summary)."""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest

from cnpj_lakehouse import pipeline

ORDER = [
    "download",
    "verify",
    "bronze",
    "bronze-check",
    "silver",
    "silver-check",
    "quality",
    "gold",
    "reconcile",
]


class FakeRunner:
    """Records the command lines and fails the step whose argv contains ``fail_on``."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_on = fail_on

    def __call__(self, argv: Sequence[str]) -> int:
        self.calls.append(tuple(argv))
        return 3 if self.fail_on and self.fail_on in " ".join(argv) else 0


def ticking_clock() -> Iterator[float]:
    t = 0.0
    while True:
        yield t
        t += 2.0


def clock() -> pipeline.Clock:
    ticks = ticking_clock()
    return lambda: next(ticks)


def test_steps_follow_the_make_targets_in_order() -> None:
    steps = pipeline.build_steps("2026-09", ["lookups", "Empresas1"], python="py")
    assert [s.name for s in steps] == ORDER
    by_name = {s.name: s.argv for s in steps}
    assert by_name["download"] == (
        "py", "-m", "cnpj_lakehouse.download", "--competencia", "2026-09",
        "--files", "lookups", "Empresas1",
    )  # fmt: skip
    assert by_name["verify"][-1] == "--verify-only"
    assert by_name["bronze-check"][1] == str(Path("scripts") / "bronze_check.py")
    assert by_name["silver-check"][-4:] == (
        "--summary", str(Path("build") / "silver-2026-09.json"),
        "--out", str(Path("build") / "silver-check"),
    )  # fmt: skip
    assert by_name["quality"][-1] == str(Path("build") / "quality-2026-09")
    assert by_name["gold"][-1] == str(Path("build") / "gold-2026-09.json")
    # no --files: the downloader's own default applies
    assert "--files" not in pipeline.build_steps("2026-09")[0].argv


def test_every_step_runs_once_in_order_and_the_summary_is_written(tmp_path: Path) -> None:
    runner = FakeRunner()
    code = pipeline.main(
        ["--competencia", "2026-09", "--files", "lookups", "--build-dir", str(tmp_path)],
        runner=runner,
        clock=clock(),
    )
    assert code == 0
    modules = [argv[2] if argv[1] == "-m" else Path(argv[1]).name for argv in runner.calls]
    assert modules == [
        "cnpj_lakehouse.download",
        "cnpj_lakehouse.download",
        "cnpj_lakehouse.bronze",
        "bronze_check.py",
        "cnpj_lakehouse.silver",
        "silver_check.py",
        "cnpj_lakehouse.quality",
        "cnpj_lakehouse.gold",
        "cnpj_lakehouse.reconcile",
    ]
    report = json.loads((tmp_path / "pipeline-2026-09.json").read_text(encoding="utf-8"))
    assert report["passed"] and report["failed_step"] is None
    assert [s["name"] for s in report["steps"]] == ORDER
    assert all(s["status"] == "passed" and s["exit_code"] == 0 for s in report["steps"])
    assert all(s["duration_s"] == 2.0 for s in report["steps"])
    assert report["steps_by_status"] == {"passed": 9, "failed": 0, "skipped": 0, "not run": 0}
    # RST-01: only names, statuses, exit codes and durations
    assert set(report["steps"][0]) == {"name", "requirement", "status", "exit_code", "duration_s"}
    md = (tmp_path / "pipeline-2026-09.md").read_text(encoding="utf-8")
    assert "Pipeline 2026-09: passed" in md and "| 9 | reconcile | RF-05 | passed | 0 |" in md


def test_stops_at_the_first_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner = FakeRunner(fail_on="cnpj_lakehouse.silver")
    code = pipeline.main(
        ["--competencia", "2026-09", "--build-dir", str(tmp_path)], runner=runner, clock=clock()
    )
    assert code == 1
    assert len(runner.calls) == 5  # download, verify, bronze, bronze-check, silver
    report = json.loads((tmp_path / "pipeline-2026-09.json").read_text(encoding="utf-8"))
    statuses = {s["name"]: (s["status"], s["exit_code"]) for s in report["steps"]}
    assert statuses["silver"] == ("failed", 3)
    assert all(statuses[n] == ("not run", None) for n in ORDER[5:])
    assert not report["passed"] and report["failed_step"] == "silver"
    err = capsys.readouterr().err
    assert "FAILED at step 'silver'" in err and "silver-check, quality, gold, reconcile" in err
    assert "FAILED at `silver`" in (tmp_path / "pipeline-2026-09.md").read_text(encoding="utf-8")


def test_skip_download_still_verifies_the_manifest(tmp_path: Path) -> None:
    runner = FakeRunner()
    code = pipeline.main(
        ["--competencia", "2026-09", "--skip-download", "--build-dir", str(tmp_path)],
        runner=runner,
        clock=clock(),
    )
    assert code == 0
    assert runner.calls[0][-1] == "--verify-only"
    assert not any(c[2:3] == ("cnpj_lakehouse.download",) and c[-1] != "--verify-only"
                   for c in runner.calls)  # fmt: skip
    report = json.loads((tmp_path / "pipeline-2026-09.json").read_text(encoding="utf-8"))
    assert report["steps"][0] == {
        "name": "download", "requirement": "RF-01", "status": "skipped",
        "exit_code": None, "duration_s": 0.0,
    }  # fmt: skip
    assert report["passed"]


def test_a_failing_verify_stops_before_spark(tmp_path: Path) -> None:
    runner = FakeRunner(fail_on="--verify-only")
    code = pipeline.main(
        ["--competencia", "2026-09", "--skip-download", "--build-dir", str(tmp_path)],
        runner=runner,
        clock=clock(),
    )
    assert code == 1 and len(runner.calls) == 1


def test_invalid_competencia_is_a_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        pipeline.main(["--competencia", "2026-13"], runner=FakeRunner())
    assert exc.value.code == 2


def test_subprocess_runner_returns_the_exit_code() -> None:
    import sys

    assert pipeline.subprocess_runner([sys.executable, "-c", "raise SystemExit(4)"]) == 4
