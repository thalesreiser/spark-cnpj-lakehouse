"""RF-08 / RNF-01 baseline (task 6.1): runs the main job N times and records its metrics.

Run inside the ``spark-client`` container (``make benchmark``, ``Benchmark`` workflow) after
``make bronze`` for ``--competencia`` and ``scripts/silver_next_month.py`` for ``--next`` (the
synthetic next month that the silver MERGE consumes). Every step is its own Spark application
with event logs enabled (``SPARK_EVENT_LOG_DIR``); pipeline code is unchanged.

* **setup** (once): ``cnpj_lakehouse.silver`` for ``--competencia`` creates the silver tables
  under ``s3a://<bucket>/benchmark/silver`` (version 0); measured and reported, not repeated.
* **main job** (``--runs`` times), the monthly incremental load:

  1. ``silver_merge``: ``cnpj_lakehouse.silver`` for ``--next`` on ``--merge-tables`` (MERGE);
  2. ``gold``: ``cnpj_lakehouse.gold`` from that silver into ``benchmark/run-<i>/gold``.

  Before runs 2..N the merged tables are restored to version 0 (Delta ``RESTORE``) and the files
  of the undone MERGE are vacuumed, so every run merges the same month into the same table
  without reloading it (the runner has 14 GB of disk).

After the runs, ``DESCRIBE DETAIL`` gives the files and size of the bronze tables, of silver
after the MERGE and of the gold tables of the median run, with live files per partition value.
Output (metadata only, never rows, RST-01): ``<out>/benchmark.json``, ``<out>/benchmark.md``,
``<out>/eventlogs/`` and the run summaries in ``<out>/setup`` and ``<out>/run-<i>/``.

``ui`` rebuilds the Spark UI of the median run from the event logs with a Spark History Server
and saves its REST API JSON and the server-rendered jobs/stages/SQL pages to ``<out>/spark-ui``.

    python scripts/benchmark.py run --competencia 2026-09 --next 2026-10 \
        --merge-tables empresas estabelecimentos --runs 3 --out build/benchmark
    python scripts/benchmark.py ui --out build/benchmark
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from cnpj_lakehouse.benchmark import (
    AppMetrics,
    file_stats,
    parse_event_log,
    render_markdown,
    summarize,
)
from cnpj_lakehouse.bronze import BRONZE_LAYER
from cnpj_lakehouse.spark import LakehouseConfig, get_spark
from cnpj_lakehouse.versions import DELTA_VERSION, SPARK_VERSION

MAIN_STEPS = ("silver_merge", "gold")
HISTORY_PORT = 18080


def _event_logs(directory: Path) -> set[Path]:
    return {p for p in directory.iterdir() if not p.name.endswith(".inprogress")}


def run_step(cmd: list[str], env: dict[str, str], logs: Path) -> tuple[float, AppMetrics, Path]:
    """Runs one Spark application and parses the event log it wrote."""
    before = _event_logs(logs)
    print(f"[benchmark] $ {' '.join(cmd)}", flush=True)
    start = time.monotonic()
    subprocess.run(cmd, env=env, check=True)
    wall = round(time.monotonic() - start, 1)
    new = sorted(_event_logs(logs) - before)
    if len(new) != 1:
        raise RuntimeError(f"expected one new event log in {logs}, found {new}")
    with new[0].open(encoding="utf-8") as fh:
        return wall, parse_event_log(fh), new[0]


def _silver(args: argparse.Namespace, competencia: str, silver: str, summary: Path) -> list[str]:
    cmd = [sys.executable, "-m", "cnpj_lakehouse.silver", "--competencia", competencia]
    if competencia == args.next:
        cmd += ["--tables", *args.merge_tables]
    return [*cmd, "--output", silver, "--summary", str(summary)]


def _gold(args: argparse.Namespace, silver: str, gold: str, summary: Path) -> list[str]:
    return [
        *(sys.executable, "-m", "cnpj_lakehouse.gold", "--competencia", args.next),
        *("--silver", silver, "--output", gold, "--summary", str(summary)),
    ]


def reset_silver(cfg: LakehouseConfig, paths: list[str]) -> None:
    """Restores the merged silver tables to version 0 and vacuums the undone MERGE files."""
    from delta.tables import DeltaTable

    spark = get_spark("benchmark-reset", cfg)
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        for path in paths:
            table = DeltaTable.forPath(spark, path)
            table.restoreToVersion(0)
            table.vacuum(0)
    finally:
        spark.stop()


def _environment(args: argparse.Namespace, properties: dict[str, str]) -> dict[str, Any]:
    mem = "unknown"
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                mem = f"{int(line.split()[1]) / 1024 / 1024:.1f} GiB"
    except OSError:
        pass
    env: dict[str, Any] = {
        "CPUs (host)": os.cpu_count(),
        "RAM (host)": mem,
        "Spark": SPARK_VERSION,
        "Delta Lake": DELTA_VERSION,
        "competencia": args.competencia,
        "next month (MERGE)": args.next,
        "MERGE tables": " ".join(args.merge_tables),
        "bronze sample fraction (SPARK_SAMPLE_FRACTION)": os.environ.get(
            "SPARK_SAMPLE_FRACTION", "1.0"
        ),
    }
    for item in args.info:
        key, _, value = item.partition("=")
        env[key] = value
    env["Spark properties (set explicitly; others are defaults)"] = properties
    return env


def _files(cfg: LakehouseConfig, tables: dict[str, str]) -> dict[str, dict[str, Any]]:
    """`DESCRIBE DETAIL` + live files per partition value for each Delta table path."""
    from pyspark.sql import functions as F

    spark = get_spark("benchmark-files", cfg)
    result: dict[str, dict[str, Any]] = {}
    try:
        for name, path in tables.items():
            detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
            if detail is None:
                continue
            columns = list(detail["partitionColumns"] or [])
            partitions: dict[str, int] = {}
            if columns:
                rows = (
                    spark.read.format("delta")
                    .load(path)
                    .select(*columns, F.input_file_name().alias("_file"))
                    .groupBy(*columns)
                    .agg(F.countDistinct("_file").alias("files"))
                    .collect()
                )
                partitions = {
                    "/".join(f"{c}={r[c]}" for c in columns): int(r["files"]) for r in rows
                }
            result[name] = file_stats(
                int(detail["numFiles"] or 0), int(detail["sizeInBytes"] or 0), partitions
            )
    finally:
        spark.stop()
    return result


def cmd_run(args: argparse.Namespace) -> int:
    cfg = LakehouseConfig.from_env(os.environ)
    out: Path = args.out.resolve()
    logs = out / "eventlogs"
    logs.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "SPARK_EVENT_LOG_DIR": logs.as_uri()}
    silver = cfg.path("benchmark", "silver")
    setup_dir = out / "setup"
    setup_dir.mkdir(parents=True, exist_ok=True)
    wall, app, log_file = run_step(
        _silver(args, args.competencia, silver, setup_dir / "silver-initial.json"), env, logs
    )
    setup = {"wall_s": wall, **app.totals()}
    setup_app = {**app.to_dict(), "event_log": log_file.name}
    print(f"[benchmark] setup silver_initial: {json.dumps(setup)}", flush=True)

    runs: list[dict[str, dict[str, float]]] = []
    details: list[dict[str, Any]] = []
    merged = [f"{silver}/{t}" for t in args.merge_tables]
    for i in range(1, args.runs + 1):
        if i > 1:
            reset_silver(cfg, merged)
        run_dir = out / f"run-{i}"
        run_dir.mkdir(parents=True, exist_ok=True)
        gold = cfg.path("benchmark", f"run-{i}", "gold")
        commands = {
            "silver_merge": _silver(args, args.next, silver, run_dir / "silver-merge.json"),
            "gold": _gold(args, silver, gold, run_dir / "gold.json"),
        }
        metrics: dict[str, dict[str, float]] = {}
        apps: dict[str, Any] = {}
        for step, cmd in commands.items():
            wall, app, log_file = run_step(cmd, env, logs)
            metrics[step] = {"wall_s": wall, **app.totals()}
            apps[step] = {**app.to_dict(), "event_log": log_file.name}
            print(f"[benchmark] run {i} {step}: {json.dumps(metrics[step])}", flush=True)
        runs.append(metrics)
        details.append({"apps": apps})

    summary = summarize(runs, MAIN_STEPS)
    best = details[summary["median_run"]]
    initial = json.loads((setup_dir / "silver-initial.json").read_text(encoding="utf-8"))
    silver_tables = [t["table"] for t in initial["tables"]]
    median_dir = out / f"run-{summary['median_run'] + 1}"
    gold_paths = json.loads((median_dir / "gold.json").read_text(encoding="utf-8"))["paths"]
    tables = {f"bronze/{t}": cfg.path(BRONZE_LAYER, t) for t in silver_tables}
    tables |= {f"silver/{t}": f"{silver}/{t}" for t in silver_tables}
    tables |= {f"gold/{t}": p for t, p in gold_paths.items()}
    properties = best["apps"][MAIN_STEPS[0]]["properties"]
    result = {
        "environment": _environment(args, properties),
        "summary": summary,
        "setup": {"silver_initial": setup},
        "files": _files(cfg, tables),
        "top_stages": {
            "silver_initial (setup)": setup_app["top_stages"],
            **{step: app["top_stages"] for step, app in best["apps"].items()},
        },
        "setup_app": setup_app,
        "runs": details,
    }
    (out / "benchmark.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (out / "benchmark.md").write_text(render_markdown(result), encoding="utf-8")
    print((out / "benchmark.md").read_text(encoding="utf-8"))
    return 0


def _get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 (localhost only)
        data: bytes = response.read()
        return data


def cmd_ui(args: argparse.Namespace) -> int:
    out: Path = args.out.resolve()
    result = json.loads((out / "benchmark.json").read_text(encoding="utf-8"))
    best = result["runs"][result["summary"]["median_run"]]
    app_ids = [app["app_id"] for app in best["apps"].values()]
    ui = out / "spark-ui"
    ui.mkdir(parents=True, exist_ok=True)
    opts = (
        f"-Dspark.history.fs.logDirectory={(out / 'eventlogs').as_uri()}"
        f" -Dspark.history.ui.port={HISTORY_PORT} -Dspark.history.fs.update.interval=2s"
    )
    server = subprocess.Popen(
        ["spark-class", "org.apache.spark.deploy.history.HistoryServer"],
        env={**os.environ, "SPARK_HISTORY_OPTS": opts},
    )
    base = f"http://localhost:{HISTORY_PORT}"
    try:
        deadline = time.monotonic() + args.timeout
        while True:
            try:
                listed = {a["id"] for a in json.loads(_get(f"{base}/api/v1/applications"))}
                if set(app_ids) <= listed:
                    break
            except (urllib.error.URLError, ConnectionError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"History Server did not list {app_ids} in {args.timeout}s")
            time.sleep(2)
        for step, app in best["apps"].items():
            app_id = app["app_id"]
            target = ui / step
            target.mkdir(exist_ok=True)
            api = f"{base}/api/v1/applications/{app_id}"
            (target / "jobs.json").write_bytes(_get(f"{api}/jobs"))
            (target / "stages.json").write_bytes(_get(f"{api}/stages"))
            (target / "executors.json").write_bytes(_get(f"{api}/allexecutors"))
            quantiles = "quantiles=0.0,0.25,0.5,0.75,1.0"
            for s in app["top_stages"][:3]:
                name = f"stage-{s['stage_id']}-{s['attempt']}"
                (target / f"{name}-task-summary.json").write_bytes(
                    _get(f"{api}/stages/{s['stage_id']}/{s['attempt']}/taskSummary?{quantiles}")
                )
            for page in ("jobs", "stages", "SQL"):
                (target / f"{page}.html").write_bytes(_get(f"{base}/history/{app_id}/{page}/"))
            print(f"[benchmark] Spark UI of {step} ({app_id}) saved to {target}", flush=True)
    finally:
        server.terminate()
        server.wait(timeout=30)
    return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python scripts/benchmark.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run the main job N times and record the metrics")
    run.add_argument("--competencia", required=True, help="bronze month of the initial load")
    run.add_argument("--next", required=True, help="synthetic next month merged into silver")
    run.add_argument("--merge-tables", nargs="+", default=["empresas"])
    run.add_argument("--runs", type=int, default=3)
    run.add_argument("--info", nargs="*", default=[], help="extra KEY=VALUE environment notes")
    run.add_argument("--out", type=Path, default=Path("build/benchmark"))
    ui = sub.add_parser("ui", help="save the Spark UI of the median run (History Server)")
    ui.add_argument("--out", type=Path, default=Path("build/benchmark"))
    ui.add_argument("--timeout", type=int, default=180)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "run":
        if args.runs < 1:
            raise SystemExit("--runs must be >= 1")
        return cmd_run(args)
    return cmd_ui(args)


if __name__ == "__main__":
    sys.exit(main())
