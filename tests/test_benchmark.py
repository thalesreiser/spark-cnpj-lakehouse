"""Benchmark metrics (task 6.1): event-log parsing, medians and the sheet, on synthetic events."""

from __future__ import annotations

import json
from typing import Any

import pytest

from cnpj_lakehouse.benchmark import (
    MIN_TASKS_FOR_SKEW,
    combine,
    file_stats,
    human_bytes,
    median,
    parse_event_log,
    parse_events,
    render_markdown,
    summarize,
)


def _task(
    stage: int,
    ms: int,
    *,
    attempt: int = 0,
    reason: str = "Success",
    shuffle_read: tuple[int, int] = (0, 0),
    shuffle_write: int = 0,
    spill: tuple[int, int] = (0, 0),
) -> dict[str, Any]:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage,
        "Stage Attempt ID": attempt,
        "Task End Reason": {"Reason": reason},
        "Task Info": {"Launch Time": 1_000, "Finish Time": 1_000 + ms, "Failed": False},
        "Task Metrics": {
            "Executor Run Time": ms,
            "JVM GC Time": 10,
            "Memory Bytes Spilled": spill[0],
            "Disk Bytes Spilled": spill[1],
            "Shuffle Read Metrics": {
                "Remote Bytes Read": shuffle_read[0],
                "Local Bytes Read": shuffle_read[1],
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write},
            "Input Metrics": {"Bytes Read": 100},
            "Output Metrics": {"Bytes Written": 7},
        },
    }


def _stage(stage: int, name: str, start: int, end: int) -> dict[str, Any]:
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage,
            "Stage Attempt ID": 0,
            "Stage Name": name,
            "Submission Time": start,
            "Completion Time": end,
        },
    }


EVENTS: list[dict[str, Any]] = [
    {"Event": "SparkListenerLogStart", "Spark Version": "3.5.3"},
    {
        "Event": "SparkListenerApplicationStart",
        "App Name": "silver-2026-10",
        "App ID": "app-1",
        "Timestamp": 10_000,
    },
    {
        "Event": "SparkListenerEnvironmentUpdate",
        "Spark Properties": {
            "spark.sql.adaptive.enabled": "true",
            "spark.hadoop.fs.s3a.secret.key": "*********(redacted)",
            "spark.executor.memory": "1g",
        },
    },
    {"Event": "SparkListenerJobStart", "Job ID": 0},
    _task(0, 100, shuffle_write=1_000),
    _task(0, 100, shuffle_write=1_000),
    _task(0, 100, shuffle_write=1_000),
    _task(0, 500, shuffle_write=1_000, spill=(4_096, 1_024)),
    _task(0, 50, reason="ExceptionFailure"),
    _stage(0, "save at Delta", 10_100, 10_700),
    _task(1, 30, shuffle_read=(300, 700)),
    _stage(1, "count at x", 10_800, 10_830),
    {"Event": "SparkListenerJobEnd", "Job ID": 0, "Job Result": {"Result": "JobSucceeded"}},
    {"Event": "SparkListenerJobEnd", "Job ID": 1, "Job Result": {"Result": "JobFailed"}},
    {"Event": "SparkListenerApplicationEnd", "Timestamp": 22_500},
]


def test_parse_events_aggregates_tasks_stages_and_app() -> None:
    app = parse_events(EVENTS)
    assert (app.app_id, app.app_name, app.spark_version) == ("app-1", "silver-2026-10", "3.5.3")
    totals = app.totals()
    assert totals["app_s"] == 12.5
    assert (totals["jobs"], totals["failed_jobs"], totals["stages"]) == (2, 1, 2)
    assert (totals["tasks"], totals["failed_tasks"]) == (6, 1)
    assert totals["shuffle_write_bytes"] == 4_000
    assert totals["shuffle_read_bytes"] == 1_000  # remote + local
    assert (totals["memory_spill_bytes"], totals["disk_spill_bytes"]) == (4_096, 1_024)
    assert (totals["input_bytes"], totals["output_bytes"]) == (600, 42)
    assert totals["executor_run_s"] == 0.9
    assert totals["gc_s"] == 0.1
    # stage 0: tasks 100, 100, 100, 500, 50 -> median 100, max 500; stage 1 has too few tasks
    assert totals["max_task_skew"] == 5.0


def test_properties_are_an_allowlist_and_accept_pairs() -> None:
    app = parse_events(EVENTS)
    assert app.properties == {"spark.executor.memory": "1g", "spark.sql.adaptive.enabled": "true"}
    pairs = parse_events(
        [{"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": [["spark.master", "x"]]}]
    )
    assert pairs.properties == {"spark.master": "x"}


def test_top_stages_by_duration_with_skew() -> None:
    top = parse_events(EVENTS).top_stages()
    assert [s["stage_id"] for s in top] == [0, 1]
    first = top[0]
    assert first["name"] == "save at Delta"
    assert (first["duration_ms"], first["tasks"], first["skew"]) == (600, 5, 5.0)
    assert (first["task_ms_max"], first["task_ms_median"]) == (500, 100)
    assert "task_ms" not in first


def test_parse_event_log_reads_json_lines_and_ignores_blank_lines() -> None:
    lines = [json.dumps(e) + "\n" for e in EVENTS] + ["\n"]
    assert parse_event_log(lines).totals() == parse_events(EVENTS).totals()


def test_missing_or_bad_fields_count_as_zero() -> None:
    app = parse_events(
        [
            {"Event": "SparkListenerTaskEnd", "Stage ID": "x", "Task Metrics": None},
            {"Event": "SparkListenerApplicationStart", "Timestamp": None},
            {"Event": "Other"},
        ]
    )
    totals = app.totals()
    assert (totals["tasks"], totals["shuffle_read_bytes"], totals["app_s"]) == (1, 0, 0.0)
    assert totals["max_task_skew"] == 0.0
    assert app.to_dict()["top_stages"][0]["skew"] == 0.0


def test_skew_needs_enough_tasks() -> None:
    events = [_task(3, ms) for ms in [10] * (MIN_TASKS_FOR_SKEW - 2) + [1_000]]
    assert parse_events(events).totals()["max_task_skew"] == 0.0


def test_median_of_runs() -> None:
    assert median([3.0, 1.0, 2.0]) == 2
    assert median([1.0, 2.0]) == 1.5
    assert median([1.25, 1.0, 9.0]) == 1.25
    assert median([]) == 0.0


def test_combine_sums_steps_and_keeps_the_max_skew() -> None:
    combined = combine(
        [{"wall_s": 10.0, "max_task_skew": 2.0}, {"wall_s": 5.5, "max_task_skew": 3.0, "tasks": 4}]
    )
    assert combined == {"max_task_skew": 3.0, "tasks": 4.0, "wall_s": 15.5}


def _run(merge: float, gold: float) -> dict[str, dict[str, float]]:
    return {
        "silver_initial": {"wall_s": 100.0},
        "silver_merge": {"wall_s": merge, "shuffle_write_bytes": 10.0},
        "gold": {"wall_s": gold, "shuffle_write_bytes": 1.0},
    }


def test_summarize_medians_per_step_and_main_job() -> None:
    runs = [_run(50, 20), _run(40, 10), _run(70, 30)]
    summary = summarize(runs, ["silver_merge", "gold"])
    assert summary["runs"] == 3
    assert [m["wall_s"] for m in summary["main_per_run"]] == [70, 50, 100]
    assert summary["main"]["wall_s"] == 70
    assert summary["main"]["shuffle_write_bytes"] == 11
    assert summary["steps"]["silver_merge"]["wall_s"] == 50
    assert summary["steps"]["silver_initial"]["wall_s"] == 100
    assert summary["median_run"] == 0  # main-job wall times 70, 50, 100 -> the median is run 1


def test_summarize_even_runs_picks_the_lower_middle_and_rejects_no_runs() -> None:
    summary = summarize([_run(50, 0), _run(10, 0)], ["silver_merge", "gold"])
    assert summary["median_run"] == 1
    assert summary["main"]["wall_s"] == 30
    with pytest.raises(ValueError, match="no benchmark runs"):
        summarize([], ["gold"])


def test_file_stats() -> None:
    stats = file_stats(6, 600, {"competencia=2026-10": 2, "competencia=2026-09": 4})
    assert stats["avg_file_bytes"] == 100
    assert list(stats["partitions"]) == ["competencia=2026-09", "competencia=2026-10"]
    assert (stats["files_per_partition_min"], stats["files_per_partition_max"]) == (2, 4)
    unpartitioned = file_stats(0, 0, {})
    assert unpartitioned["avg_file_bytes"] == 0
    assert unpartitioned["files_per_partition_max"] == 0


def test_human_bytes() -> None:
    assert human_bytes(512) == "512 B"
    assert human_bytes(1536) == "1.5 KiB"
    assert human_bytes(3 * 1024**3) == "3.0 GiB"
    assert human_bytes(5 * 1024**4) == "5120.0 GiB"


def test_render_markdown_sheet() -> None:
    summary = summarize([_run(50, 20), _run(40, 10), _run(70, 30)], ["silver_merge", "gold"])
    app = parse_events(EVENTS)
    result = {
        "environment": {"CPUs (host)": 2, "Spark properties": {"spark.executor.memory": "1g"}},
        "summary": summary,
        "files": {"silver/empresas": file_stats(4, 4096, {})},
        "top_stages": {"silver_merge": app.top_stages(1)},
    }
    md = render_markdown(result)
    assert md.startswith("# Benchmark baseline (task 6.1)")
    assert "Main job: silver_merge + gold" in md
    assert "| Metric | main job | silver_initial | silver_merge | gold |" in md
    assert "| Wall time | 70.0 s | 100.0 s | 50.0 s | 20.0 s |" in md
    assert "- `spark.executor.memory` = `1g`" in md
    assert "| silver/empresas | 4 | 4.0 KiB | 1.0 KiB | 1 | 4-4 |" in md
    assert "| silver_merge | 0: save at Delta | 0.6 s | 5 |" in md
    minimal = render_markdown({"summary": summary})
    assert "Files per table" not in minimal and "Longest stages" not in minimal
