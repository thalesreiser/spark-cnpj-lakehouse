"""RF-08 / RNF-01 (task 6.1): metrics of a benchmark run, read from Spark event logs.

The benchmark (``scripts/benchmark.py``, ``make benchmark``, ``Benchmark`` workflow) runs each
step of the main job as its own Spark application with event logs enabled
(``SPARK_EVENT_LOG_DIR``). The event log is the data behind the Spark UI: this module reads it
without a ``SparkListener`` or a live UI and aggregates, per application,

* wall time of the application, number of jobs, stages and tasks (failed tasks too);
* shuffle read (local + remote) and shuffle write bytes, memory and disk spill bytes, input and
  output bytes, executor run time and GC time, summed over the successful and failed tasks;
* per stage: duration, tasks, shuffle, spill and the task-duration skew (max / median);
* a whitelist of Spark properties (never credentials).

It also computes the median of N runs and renders the Markdown measurement sheet. Everything is
pure (lists of dicts in, dicts out), so the logic is unit-tested on synthetic events.
"""

from __future__ import annotations

import json
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

# Spark properties worth recording in the measurement sheet (no credentials: allowlist only).
KEY_PROPERTIES = (
    "spark.master",
    "spark.driver.memory",
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.cores.max",
    "spark.default.parallelism",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes",
)

# Stages with fewer tasks than this are left out of the skew ratio (a 1-task stage has no skew).
MIN_TASKS_FOR_SKEW = 4

# Metric columns of the measurement sheet, in order: (key, label, unit).
METRICS: tuple[tuple[str, str, str], ...] = (
    ("wall_s", "Wall time", "s"),
    ("app_s", "Spark app time", "s"),
    ("jobs", "Jobs", ""),
    ("stages", "Stages", ""),
    ("tasks", "Tasks", ""),
    ("shuffle_read_bytes", "Shuffle read", "bytes"),
    ("shuffle_write_bytes", "Shuffle write", "bytes"),
    ("memory_spill_bytes", "Spill (memory)", "bytes"),
    ("disk_spill_bytes", "Spill (disk)", "bytes"),
    ("input_bytes", "Input", "bytes"),
    ("output_bytes", "Output", "bytes"),
    ("executor_run_s", "Executor run time", "s"),
    ("gc_s", "GC time", "s"),
    ("max_task_skew", "Max task skew (max/median)", "x"),
)


@dataclass
class StageMetrics:
    """Totals of one stage attempt, from its ``SparkListenerTaskEnd`` and completion events."""

    stage_id: int
    attempt: int
    name: str = ""
    tasks: int = 0
    failed_tasks: int = 0
    duration_ms: int = 0
    executor_run_ms: int = 0
    gc_ms: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    task_ms: list[int] = field(default_factory=list)

    @property
    def skew(self) -> float:
        """Longest task / median task duration (1.0 = balanced); 0.0 without tasks."""
        if not self.task_ms:
            return 0.0
        median = statistics.median(self.task_ms)
        return round(max(self.task_ms) / median, 2) if median > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("task_ms")
        data["skew"] = self.skew
        data["task_ms_max"] = max(self.task_ms, default=0)
        data["task_ms_median"] = statistics.median(self.task_ms) if self.task_ms else 0
        return data


@dataclass
class AppMetrics:
    """Totals of one Spark application (one event log)."""

    app_id: str = ""
    app_name: str = ""
    start_ms: int = 0
    end_ms: int = 0
    jobs: int = 0
    failed_jobs: int = 0
    spark_version: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    stages: dict[tuple[int, int], StageMetrics] = field(default_factory=dict)

    def stage(self, stage_id: int, attempt: int) -> StageMetrics:
        key = (stage_id, attempt)
        if key not in self.stages:
            self.stages[key] = StageMetrics(stage_id, attempt)
        return self.stages[key]

    def _sum(self, name: str) -> int:
        return sum(int(getattr(s, name)) for s in self.stages.values())

    def totals(self) -> dict[str, float]:
        """Flat metrics of the application (the columns of `METRICS`, except ``wall_s``)."""
        skewed = [s.skew for s in self.stages.values() if len(s.task_ms) >= MIN_TASKS_FOR_SKEW]
        return {
            "app_s": round(max(self.end_ms - self.start_ms, 0) / 1000, 1),
            "jobs": self.jobs,
            "failed_jobs": self.failed_jobs,
            "stages": len(self.stages),
            "tasks": self._sum("tasks"),
            "failed_tasks": self._sum("failed_tasks"),
            "shuffle_read_bytes": self._sum("shuffle_read_bytes"),
            "shuffle_write_bytes": self._sum("shuffle_write_bytes"),
            "memory_spill_bytes": self._sum("memory_spill_bytes"),
            "disk_spill_bytes": self._sum("disk_spill_bytes"),
            "input_bytes": self._sum("input_bytes"),
            "output_bytes": self._sum("output_bytes"),
            "executor_run_s": round(self._sum("executor_run_ms") / 1000, 1),
            "gc_s": round(self._sum("gc_ms") / 1000, 1),
            "max_task_skew": max(skewed, default=0.0),
        }

    def top_stages(self, n: int = 5) -> list[dict[str, Any]]:
        """The `n` longest stages (bottleneck candidates)."""
        ranked = sorted(self.stages.values(), key=lambda s: s.duration_ms, reverse=True)
        return [s.to_dict() for s in ranked[:n]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "app_name": self.app_name,
            "spark_version": self.spark_version,
            "properties": self.properties,
            "totals": self.totals(),
            "top_stages": self.top_stages(),
        }


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _properties(raw: Any) -> dict[str, str]:
    """Spark properties of an ``EnvironmentUpdate`` (an object, or a list of [key, value])."""
    pairs = raw.items() if isinstance(raw, Mapping) else (raw or [])
    found = {str(k): str(v) for k, v in pairs}
    return {k: found[k] for k in KEY_PROPERTIES if k in found}


def _task_end(app: AppMetrics, event: Mapping[str, Any]) -> None:
    stage = app.stage(_int(event.get("Stage ID")), _int(event.get("Stage Attempt ID")))
    info = event.get("Task Info") or {}
    reason = (event.get("Task End Reason") or {}).get("Reason", "Success")
    stage.tasks += 1
    if reason != "Success" or info.get("Failed"):
        stage.failed_tasks += 1
    launch, finish = _int(info.get("Launch Time")), _int(info.get("Finish Time"))
    if finish >= launch > 0:
        stage.task_ms.append(finish - launch)
    metrics = event.get("Task Metrics") or {}
    shuffle_read = metrics.get("Shuffle Read Metrics") or {}
    shuffle_write = metrics.get("Shuffle Write Metrics") or {}
    stage.executor_run_ms += _int(metrics.get("Executor Run Time"))
    stage.gc_ms += _int(metrics.get("JVM GC Time"))
    stage.memory_spill_bytes += _int(metrics.get("Memory Bytes Spilled"))
    stage.disk_spill_bytes += _int(metrics.get("Disk Bytes Spilled"))
    stage.shuffle_read_bytes += _int(shuffle_read.get("Remote Bytes Read")) + _int(
        shuffle_read.get("Local Bytes Read")
    )
    stage.shuffle_write_bytes += _int(shuffle_write.get("Shuffle Bytes Written"))
    stage.input_bytes += _int((metrics.get("Input Metrics") or {}).get("Bytes Read"))
    stage.output_bytes += _int((metrics.get("Output Metrics") or {}).get("Bytes Written"))


def _stage_completed(app: AppMetrics, event: Mapping[str, Any]) -> None:
    info = event.get("Stage Info") or {}
    stage = app.stage(_int(info.get("Stage ID")), _int(info.get("Stage Attempt ID")))
    stage.name = str(info.get("Stage Name", ""))
    submitted, completed = _int(info.get("Submission Time")), _int(info.get("Completion Time"))
    stage.duration_ms = max(completed - submitted, 0)


def parse_events(events: Iterable[Mapping[str, Any]]) -> AppMetrics:
    """Aggregates the events of one Spark application (event log lines, already decoded)."""
    app = AppMetrics()
    for event in events:
        kind = event.get("Event")
        if kind == "SparkListenerLogStart":
            app.spark_version = str(event.get("Spark Version", ""))
        elif kind == "SparkListenerApplicationStart":
            app.app_id = str(event.get("App ID", ""))
            app.app_name = str(event.get("App Name", ""))
            app.start_ms = _int(event.get("Timestamp"))
        elif kind == "SparkListenerApplicationEnd":
            app.end_ms = _int(event.get("Timestamp"))
        elif kind == "SparkListenerEnvironmentUpdate":
            app.properties = _properties(event.get("Spark Properties"))
        elif kind == "SparkListenerJobEnd":
            app.jobs += 1
            if (event.get("Job Result") or {}).get("Result") != "JobSucceeded":
                app.failed_jobs += 1
        elif kind == "SparkListenerStageCompleted":
            _stage_completed(app, event)
        elif kind == "SparkListenerTaskEnd":
            _task_end(app, event)
    return app


def parse_event_log(lines: Iterable[str]) -> AppMetrics:
    """Parses an uncompressed event log (one JSON event per line); blank lines are ignored."""
    return parse_events(json.loads(line) for line in lines if line.strip())


def median(values: Sequence[float]) -> float:
    """Median of the runs (0.0 for no runs); integers stay integers when the median is exact."""
    if not values:
        return 0.0
    result = statistics.median(values)
    return int(result) if float(result).is_integer() else round(float(result), 2)


def combine(steps: Sequence[Mapping[str, float]]) -> dict[str, float]:
    """Metrics of a job made of several steps (applications): sums, except the skew (max)."""
    keys = {k for step in steps for k in step}
    return {
        k: (
            max(float(s.get(k, 0)) for s in steps)
            if k == "max_task_skew"
            else round(sum(float(s.get(k, 0)) for s in steps), 1)
        )
        for k in sorted(keys)
    }


def summarize(
    runs: Sequence[Mapping[str, Mapping[str, float]]], main_steps: Sequence[str]
) -> dict[str, Any]:
    """Median of N runs, per step and for the main job (the sum of `main_steps` in each run).

    `runs` holds one mapping per run: step name -> flat metrics (`AppMetrics.totals` plus
    ``wall_s``). The returned ``median_run`` is the index of the run whose main-job wall time is
    the median (its event logs are the Spark UI evidence).
    """
    if not runs:
        raise ValueError("no benchmark runs")
    steps = list(runs[0])
    main = [combine([run[s] for s in main_steps if s in run]) for run in runs]
    per_step: dict[str, dict[str, float]] = {}
    for step in steps:
        keys = sorted({k for run in runs for k in run.get(step, {})})
        values = {k: [float(r[step].get(k, 0)) for r in runs if step in r] for k in keys}
        per_step[step] = {k: median(v) for k, v in values.items()}
    main_keys = sorted({k for m in main for k in m})
    walls = [m.get("wall_s", 0.0) for m in main]
    ranked = sorted(range(len(walls)), key=lambda i: walls[i])
    return {
        "runs": len(runs),
        "main_steps": list(main_steps),
        "steps": per_step,
        "main": {k: median([m.get(k, 0.0) for m in main]) for k in main_keys},
        "main_per_run": main,
        "median_run": ranked[(len(ranked) - 1) // 2],
    }


def file_stats(num_files: int, size_bytes: int, partitions: Mapping[str, int]) -> dict[str, Any]:
    """Files of one Delta table (``DESCRIBE DETAIL``) and live files per partition value."""
    counts = sorted(partitions.values())
    return {
        "num_files": num_files,
        "size_bytes": size_bytes,
        "avg_file_bytes": round(size_bytes / num_files) if num_files else 0,
        "partitions": dict(sorted(partitions.items())),
        "files_per_partition_max": counts[-1] if counts else num_files,
        "files_per_partition_min": counts[0] if counts else num_files,
    }


def human_bytes(value: float) -> str:
    """1536 -> '1.5 KiB' (binary units, one decimal)."""
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(size) < 1024 or unit == "GiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GiB"  # pragma: no cover (loop always returns)


def _format(value: float, unit: str) -> str:
    if unit == "bytes":
        return human_bytes(value)
    if unit == "s":
        return f"{value:.1f} s"
    if unit == "x":
        return f"{value:.2f}x"
    return f"{value:g}"


def render_markdown(result: Mapping[str, Any]) -> str:
    """Measurement sheet: environment, median per step and main job, files per table."""
    summary = result["summary"]
    env = result.get("environment", {})
    lines = [
        "# Benchmark baseline (task 6.1)",
        "",
        f"- Runs: {summary['runs']} (median reported); median run: #{summary['median_run'] + 1}",
        f"- Main job: {' + '.join(summary['main_steps'])}",
    ]
    lines += [f"- {k}: `{v}`" for k, v in env.items() if not isinstance(v, Mapping)]
    for name, props in env.items():
        if isinstance(props, Mapping):
            lines += ["", f"**{name}**", ""] + [f"- `{k}` = `{v}`" for k, v in props.items()]
    setup = result.get("setup", {})
    columns = ["main job", *summary["steps"], *(f"{name} (setup, 1 run)" for name in setup)]
    lines += [
        "",
        "## Median of the runs",
        "",
        "| Metric | " + " | ".join(columns) + " |",
        "|---|" + "---:|" * len(columns),
    ]
    tables = [summary["main"], *summary["steps"].values(), *setup.values()]
    for key, label, unit in METRICS:
        cells = [_format(float(t.get(key, 0)), unit) for t in tables]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## Main job wall time per run",
        "",
        "| Run | Wall time | Shuffle write | Spill (disk) |",
        "|---:|---:|---:|---:|",
    ]
    for i, run in enumerate(summary["main_per_run"], start=1):
        lines.append(
            f"| {i} | {_format(run.get('wall_s', 0), 's')}"
            f" | {human_bytes(run.get('shuffle_write_bytes', 0))}"
            f" | {human_bytes(run.get('disk_spill_bytes', 0))} |"
        )
    files = result.get("files", {})
    if files:
        lines += [
            "",
            "## Files per table (median run)",
            "",
            "| Table | Files | Size | Avg file | Partitions | Files per partition (min-max) |",
            "|---|---:|---:|---:|---:|---|",
        ]
        for table, stats in files.items():
            parts = len(stats["partitions"]) or 1
            lines.append(
                f"| {table} | {stats['num_files']} | {human_bytes(stats['size_bytes'])}"
                f" | {human_bytes(stats['avg_file_bytes'])} | {parts}"
                f" | {stats['files_per_partition_min']}-{stats['files_per_partition_max']} |"
            )
    stages = result.get("top_stages", {})
    if stages:
        lines += ["", "## Longest stages (median run)", ""]
        lines += [
            "| Step | Stage | Duration | Tasks | Shuffle read | Shuffle write | Spill (disk)"
            " | Skew |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
        for step, items in stages.items():
            for s in items:
                lines.append(
                    f"| {step} | {s['stage_id']}: {str(s['name'])[:60].replace('|', '/')}"
                    f" | {_format(s['duration_ms'] / 1000, 's')} | {s['tasks']}"
                    f" | {human_bytes(s['shuffle_read_bytes'])}"
                    f" | {human_bytes(s['shuffle_write_bytes'])}"
                    f" | {human_bytes(s['disk_spill_bytes'])} | {s['skew']:.2f}x |"
                )
    return "\n".join(lines) + "\n"


# Queries of the storage-format comparison (ADR 001, task 6.2), run on each copy of the table.
FORMAT_QUERIES = ("count", "group_by_uf", "active_companies")


def render_formats_markdown(source: str, rows: int, results: Sequence[Mapping[str, Any]]) -> str:
    """CSV x Parquet x Delta sheet: write time, size, files and the median time of each query.

    `results` holds one mapping per format: ``format``, ``write_s``, ``size_bytes``,
    ``num_files`` and ``queries`` (query name -> median seconds). Metadata only (RST-01).
    """
    header = " | ".join(f"`{q}`" for q in FORMAT_QUERIES)
    lines = [
        f"### Storage formats (ADR 001): {source}, {rows:,} rows",
        "",
        f"| Format | Write | Size | Files | {header} |",
        "|---|---:|---:|---:|" + "---:|" * len(FORMAT_QUERIES),
    ]
    for r in results:
        times = " | ".join(f"{float(r['queries'].get(q, 0.0)):.1f} s" for q in FORMAT_QUERIES)
        lines.append(
            f"| {r['format']} | {float(r['write_s']):.1f} s | {human_bytes(r['size_bytes'])}"
            f" | {r['num_files']} | {times} |"
        )
    return "\n".join(lines) + "\n"


# Rows of the before x after sheet (task 6.4): (where, metric key, label, unit). ``where`` is
# ``main`` (main job), a step of the main job or ``setup`` (initial silver load).
COMPARISON_ROWS: tuple[tuple[str, str, str, str], ...] = (
    ("main", "wall_s", "Main job (silver MERGE + gold), wall", "s"),
    ("silver_merge", "wall_s", "silver MERGE, wall", "s"),
    ("gold", "wall_s", "gold, wall", "s"),
    ("setup", "wall_s", "Initial silver load (setup, 1 run), wall", "s"),
    ("main", "tasks", "Main job tasks", ""),
    ("main", "stages", "Main job stages", ""),
    ("main", "shuffle_read_bytes", "Main job shuffle read", "bytes"),
    ("main", "shuffle_write_bytes", "Main job shuffle write", "bytes"),
    ("main", "memory_spill_bytes", "Main job spill (memory)", "bytes"),
    ("main", "disk_spill_bytes", "Main job spill (disk)", "bytes"),
    ("main", "executor_run_s", "Main job executor run time", "s"),
    ("main", "gc_s", "Main job GC time", "s"),
    ("main", "max_task_skew", "Max task skew (max/median)", "x"),
)


def pct_change(before: float, after: float) -> str:
    """Relative change after vs before: ``-63.1%``; ``=`` when both are 0, ``n/a`` from 0."""
    if before == 0:
        return "=" if after == 0 else "n/a"
    change = (after - before) / before * 100
    return f"{change:+.1f}%".replace("-", "\N{MINUS SIGN}")


def _metric(result: Mapping[str, Any], where: str, key: str) -> float:
    if where == "main":
        source = result["summary"]["main"]
    elif where == "setup":
        source = result.get("setup", {}).get("silver_initial", {})
    else:
        source = result["summary"]["steps"].get(where, {})
    return float(source.get(key, 0.0))


def _files_cell(stats: Mapping[str, Any] | None) -> str:
    if not stats:
        return "-"
    per = stats["files_per_partition_min"], stats["files_per_partition_max"]
    spread = f"{per[0]}" if per[0] == per[1] else f"{per[0]}–{per[1]}"
    return f"{stats['num_files']} ({human_bytes(stats['avg_file_bytes'])} avg; {spread}/partition)"


def render_comparison(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    checks: Mapping[str, Mapping[str, Any]],
    reads: Mapping[str, Mapping[str, Any]] | None = None,
    info: Mapping[str, str] | None = None,
) -> str:
    """Before x after sheet of two benchmark results measured on the same runner (task 6.4).

    `before` and `after` are ``benchmark.json`` results; `checks` maps each compared table to
    ``{"before": fingerprint, "after": fingerprint}``; `reads` optionally maps ``before`` and
    ``after`` to a read-workload state (``reads``, ``reads_total_s``, ``skipping``) of the
    maintenance step. Metadata only (RST-01).
    """
    lines = ["# Before × after on the same runner (task 6.4)", ""]
    for key, value in (info or {}).items():
        lines.append(f"- {key}: {value}")
    runs = before["summary"]["runs"], after["summary"]["runs"]
    lines += [
        f"- runs: {runs[0]} before, {runs[1]} after (median reported)",
        "",
        "| Metric | Before | After | Change |",
        "|---|---:|---:|---:|",
    ]
    for where, key, label, unit in COMPARISON_ROWS:
        b, a = _metric(before, where, key), _metric(after, where, key)
        lines.append(f"| {label} | {_format(b, unit)} | {_format(a, unit)} | {pct_change(b, a)} |")
    files_b, files_a = before.get("files", {}), after.get("files", {})
    tables = [t for t in dict.fromkeys([*files_b, *files_a]) if not t.startswith("bronze/")]
    if tables:
        lines += ["", "## Files per table (live files after the main job)", ""]
        lines += ["| Table | Before | After | Change (files) |", "|---|---:|---:|---:|"]
        for table in tables:
            fb, fa = files_b.get(table), files_a.get(table)
            change = pct_change(fb["num_files"], fa["num_files"]) if fb and fa else "-"
            lines.append(f"| {table} | {_files_cell(fb)} | {_files_cell(fa)} | {change} |")
    if reads:
        rb, ra = reads["before"], reads["after"]
        lines += ["", "## Read workload on silver (median, s)", ""]
        lines += ["| Query | Before | After | Change |", "|---|---:|---:|---:|"]
        for query, value in rb["reads"].items():
            b, a = float(value["median_s"]), float(ra["reads"][query]["median_s"])
            lines.append(f"| {query} | {b:.2f} | {a:.2f} | {pct_change(b, a)} |")
        b, a = float(rb["reads_total_s"]), float(ra["reads_total_s"])
        lines.append(f"| **Total** | **{b:.2f}** | **{a:.2f}** | **{pct_change(b, a)}** |")
        lines += ["", "| Data skipping: files that may match / live | Before | After |"]
        lines.append("|---|---:|---:|")
        for pred, hit in rb.get("skipping", {}).items():
            other = ra.get("skipping", {}).get(pred, {"may_match": 0, "files": 0})
            lines.append(
                f"| `{pred}` | {hit['may_match']} / {hit['files']}"
                f" | {other['may_match']} / {other['files']} |"
            )
    lines += ["", "## Same results", ""]
    for table, check in checks.items():
        same = check["before"] == check["after"]
        rows = check["after"].get("rows", "?")
        lines.append(f"- {table}: fingerprint {'identical' if same else 'DIFFERENT'} ({rows} rows)")
    return "\n".join(lines) + "\n"
