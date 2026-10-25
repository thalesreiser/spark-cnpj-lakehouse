"""RNF-05: the whole pipeline of one reference month in one command (``make run``).

Pure orchestration of the existing steps, in order, each through its own public CLI:

1. ``download``      RF-01  ``python -m cnpj_lakehouse.download`` (``--skip-download``: skipped)
2. ``verify``        RF-01  ``python -m cnpj_lakehouse.download --verify-only`` (SHA-256)
3. ``bronze``        RF-02  ``python -m cnpj_lakehouse.bronze``
4. ``bronze-check``  RF-02  ``python scripts/bronze_check.py``
5. ``silver``        RF-03  ``python -m cnpj_lakehouse.silver``
6. ``silver-check``  RF-03  ``python scripts/silver_check.py``
7. ``quality``       RF-03  ``python -m cnpj_lakehouse.quality`` (``error`` rules block)
8. ``gold``          RF-05  ``python -m cnpj_lakehouse.gold``
9. ``reconcile``     RF-05  ``python -m cnpj_lakehouse.reconcile``

Each step runs as a child process, so each Spark step is its own Spark application (a fresh JVM
and a clean session per step, exactly as the ``make <step>`` targets run them); the steps share
nothing but the Delta tables on MinIO and the files under ``build/``. The run stops at the first
step that exits with a non-zero code, prints which step failed and exits 1; the later steps are
recorded as ``not run``. A run summary with step names, exit codes and durations only (RST-01: no
data) is written to ``build/pipeline-<competencia>.json`` and ``.md`` whatever the outcome.

    python -m cnpj_lakehouse.pipeline --competencia 2026-09 --files lookups Empresas1
    python -m cnpj_lakehouse.pipeline --competencia 2026-09 --files lookups --skip-download
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cnpj_lakehouse.download import parse_competencia

PASSED = "passed"
FAILED = "failed"
SKIPPED = "skipped"
NOT_RUN = "not run"

Runner = Callable[[Sequence[str]], int]
Clock = Callable[[], float]


@dataclass(frozen=True)
class Step:
    """One pipeline step: a name, the requirement it proves and the command line to run."""

    name: str
    requirement: str
    argv: tuple[str, ...]


@dataclass(frozen=True)
class StepResult:
    name: str
    requirement: str
    status: str
    exit_code: int | None
    duration_s: float


def build_steps(
    competencia: str,
    files: Sequence[str] | None = None,
    *,
    build_dir: Path = Path("build"),
    scripts_dir: Path = Path("scripts"),
    python: str = sys.executable,
) -> list[Step]:
    """The ordered steps of one month, with the same arguments as the ``make <step>`` targets."""
    c = competencia
    files_args = ("--files", *files) if files else ()

    def module(name: str, *args: str) -> tuple[str, ...]:
        return (python, "-m", f"cnpj_lakehouse.{name}", "--competencia", c, *args)

    def script(name: str, *args: str) -> tuple[str, ...]:
        return (python, str(scripts_dir / name), "--competencia", c, *args)

    silver_summary = str(build_dir / f"silver-{c}.json")
    bronze_check = ("--out", str(build_dir / "bronze-check"))
    silver_check = ("--summary", silver_summary, "--out", str(build_dir / "silver-check"))
    return [
        Step("download", "RF-01", module("download", *files_args)),
        Step("verify", "RF-01", module("download", *files_args, "--verify-only")),
        Step("bronze", "RF-02", module("bronze", "--summary", str(build_dir / f"bronze-{c}.json"))),
        Step("bronze-check", "RF-02", script("bronze_check.py", *bronze_check)),
        Step("silver", "RF-03", module("silver", "--summary", silver_summary)),
        Step(
            "silver-check",
            "RF-03",
            script("silver_check.py", *silver_check),
        ),
        Step("quality", "RF-03", module("quality", "--report", str(build_dir / f"quality-{c}"))),
        Step("gold", "RF-05", module("gold", "--summary", str(build_dir / f"gold-{c}.json"))),
        Step("reconcile", "RF-05", module("reconcile", "--out", str(build_dir / "reconcile"))),
    ]


def subprocess_runner(argv: Sequence[str]) -> int:
    """Runs one step as a child process, streaming its output; returns its exit code."""
    return subprocess.run(list(argv), check=False).returncode


def run_steps(
    steps: Sequence[Step],
    runner: Runner = subprocess_runner,
    *,
    skip: Sequence[str] = (),
    clock: Clock = time.monotonic,
    log: Callable[[str], None] = print,
) -> list[StepResult]:
    """Runs the steps in order and stops at the first failure (later steps: ``not run``)."""
    results: list[StepResult] = []
    failed = False
    for step in steps:
        if failed:
            results.append(StepResult(step.name, step.requirement, NOT_RUN, None, 0.0))
            continue
        if step.name in skip:
            log(f"[pipeline] {step.name}: skipped")
            results.append(StepResult(step.name, step.requirement, SKIPPED, None, 0.0))
            continue
        log(f"[pipeline] {step.name} ({step.requirement}): {' '.join(step.argv[1:])}")
        start = clock()
        code = runner(step.argv)
        duration = round(clock() - start, 1)
        status = PASSED if code == 0 else FAILED
        log(f"[pipeline] {step.name}: {status} (exit {code}) in {duration:.1f} s")
        results.append(StepResult(step.name, step.requirement, status, code, duration))
        failed = code != 0
    return results


def summary(
    competencia: str, results: Sequence[StepResult], started_at: str, duration_s: float
) -> dict[str, Any]:
    """Run summary (step statuses, exit codes and durations only: RST-01)."""
    failed = next((r.name for r in results if r.status == FAILED), None)
    counts = {s: sum(r.status == s for r in results) for s in (PASSED, FAILED, SKIPPED, NOT_RUN)}
    return {
        "competencia": competencia,
        "started_at": started_at,
        "duration_s": round(duration_s, 1),
        "passed": failed is None,
        "failed_step": failed,
        "steps_by_status": counts,
        "steps": [asdict(r) for r in results],
        "spark_applications": "one per step (child process)",
    }


def to_markdown(report: dict[str, Any]) -> str:
    outcome = "passed" if report["passed"] else f"FAILED at `{report['failed_step']}`"
    lines = [
        f"### Pipeline {report['competencia']}: {outcome} in {report['duration_s']:.1f} s",
        "",
        "| # | Step | Requirement | Status | Exit code | Duration (s) |",
        "|---:|---|---|---|---:|---:|",
    ]
    for i, step in enumerate(report["steps"], start=1):
        code = "" if step["exit_code"] is None else str(step["exit_code"])
        lines.append(
            f"| {i} | {step['name']} | {step['requirement']} | {step['status']} | {code}"
            f" | {step['duration_s']:.1f} |"
        )
    return "\n".join(lines) + "\n"


def write_summary(report: dict[str, Any], build_dir: Path) -> tuple[Path, Path]:
    build_dir.mkdir(parents=True, exist_ok=True)
    stem = build_dir / f"pipeline-{report['competencia']}"
    json_path, md_path = stem.with_suffix(".json"), stem.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(to_markdown(report), encoding="utf-8")
    return json_path, md_path


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.pipeline",
        description="RNF-05: download -> bronze -> silver -> quality -> gold of one month.",
    )
    parser.add_argument(
        "--competencia", required=True, type=_competencia_arg, help="reference month, YYYY-MM"
    )
    parser.add_argument(
        "--files",
        nargs="+",
        default=None,
        help="files or groups to download and verify (default: the downloader's, lookups)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="files already on disk (e.g. CI cache): only verify them against the manifest",
    )
    parser.add_argument("--build-dir", type=Path, default=Path("build"), help="default: build")
    parser.add_argument(
        "--scripts-dir", type=Path, default=Path("scripts"), help="default: scripts"
    )
    return parser.parse_args(argv)


def _competencia_arg(value: str) -> str:
    try:
        return parse_competencia(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(
    argv: Sequence[str] | None = None,
    runner: Runner = subprocess_runner,
    clock: Clock = time.monotonic,
) -> int:
    """CLI entry point: 0 when every step passes, 1 at the first failing step."""
    args = parse_args(argv)
    steps = build_steps(
        args.competencia, args.files, build_dir=args.build_dir, scripts_dir=args.scripts_dir
    )
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    start = clock()
    skip = ("download",) if args.skip_download else ()
    results = run_steps(steps, runner, skip=skip, clock=clock)
    report = summary(args.competencia, results, started_at, clock() - start)
    json_path, md_path = write_summary(report, args.build_dir)
    print(f"[pipeline] summary: {json_path} and {md_path}")
    if not report["passed"]:
        not_run = [r.name for r in results if r.status == NOT_RUN]
        print(
            f"[pipeline] FAILED at step '{report['failed_step']}'"
            f" (later steps not run: {', '.join(not_run) or 'none'})",
            file=sys.stderr,
        )
        return 1
    print(f"[pipeline] {args.competencia}: every step passed in {report['duration_s']:.1f} s")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
