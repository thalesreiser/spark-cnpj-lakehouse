"""Spark UI screenshots of the benchmark (task 6.4), rendered headlessly on the CI runner.

Runs on the host of the ``Benchmark`` workflow (not in the Spark containers), after
``scripts/benchmark.py run`` wrote two result directories (e.g. before and after the tuning):

* ``logs`` copies the event logs of the median run of each result into one directory (the
  ``spark.history.fs.logDirectory`` of a Spark History Server) and writes ``apps.json``;
* ``shoot`` opens the History Server pages of the silver MERGE application of each result with
  Chromium (Playwright) and saves palette PNGs: jobs list, longest stage, SQL plan of the longest
  query and executors.

The Spark UI shows plans, metrics and file paths, never rows (RST-01); still, review every image
before committing it. Playwright and Pillow are installed by the workflow only (not a project
dependency).

    python scripts/spark_ui_screenshots.py logs --bench before=build/benchmark-baseline \\
        after=build/benchmark --logs build/ui-logs
    python scripts/spark_ui_screenshots.py shoot --logs build/ui-logs \\
        --url http://localhost:18080 --out build/screenshots
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

MERGE_STEP = "silver_merge"
VIEWPORT = {"width": 1400, "height": 900}
MAX_HEIGHT = 1500  # px; long pages are cropped at the top part
COLORS = 128  # palette PNG: Spark UI pages are flat colours and text


def cmd_logs(args: argparse.Namespace) -> int:
    logs: Path = args.logs
    logs.mkdir(parents=True, exist_ok=True)
    apps: dict[str, dict[str, Any]] = {}
    for item in args.bench:
        label, _, directory = item.partition("=")
        out = Path(directory)
        result = json.loads((out / "benchmark.json").read_text(encoding="utf-8"))
        best = result["runs"][result["summary"]["median_run"]]["apps"]
        for app in best.values():
            shutil.copy2(out / "eventlogs" / app["event_log"], logs / app["event_log"])
        merge = best[MERGE_STEP]
        stage = merge["top_stages"][0]
        apps[label] = {
            "app_id": merge["app_id"],
            "stage": [stage["stage_id"], stage["attempt"]],
            "apps": {step: app["app_id"] for step, app in best.items()},
        }
    (logs / "apps.json").write_text(json.dumps(apps, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(apps, indent=2))
    return 0


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310 (localhost only)
        return json.loads(response.read())


def _wait(base: str, app_ids: set[str], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            listed = {a["id"] for a in _get_json(f"{base}/api/v1/applications")}
            if app_ids <= listed:
                return
        except (urllib.error.URLError, ConnectionError, ValueError):
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"History Server did not list {sorted(app_ids)} in {timeout}s")
        time.sleep(3)


def _optimize(path: Path) -> None:
    from PIL import Image

    with Image.open(path) as img:
        rgb = img.convert("RGB")
        if rgb.height > MAX_HEIGHT:
            rgb = rgb.crop((0, 0, rgb.width, MAX_HEIGHT))
        rgb.quantize(colors=COLORS).save(path, optimize=True)


def cmd_shoot(args: argparse.Namespace) -> int:
    from playwright.sync_api import sync_playwright

    apps = json.loads((args.logs / "apps.json").read_text(encoding="utf-8"))
    base = args.url.rstrip("/")
    _wait(base, {a["app_id"] for a in apps.values()}, args.timeout)
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport=VIEWPORT)
        for label, app in apps.items():
            app_id = app["app_id"]
            stage_id, attempt = app["stage"]
            queries = _get_json(f"{base}/api/v1/applications/{app_id}/sql?details=false")
            longest = max(queries, key=lambda q: q.get("duration", 0))["id"]
            ui = f"{base}/history/{app_id}"
            pages = {
                "jobs": f"{ui}/jobs/",
                "stage": f"{ui}/stages/stage/?id={stage_id}&attempt={attempt}",
                "sql": f"{ui}/SQL/execution/?id={longest}",
                "executors": f"{ui}/executors/",
            }
            for name, url in pages.items():
                page.goto(url, wait_until="networkidle", timeout=180_000)
                page.wait_for_timeout(1500)
                target = out / f"{label}-{name}.png"
                page.screenshot(path=str(target), full_page=True)
                _optimize(target)
                print(f"[screenshots] {target} ({target.stat().st_size // 1024} KiB) <- {url}")
        browser.close()
    return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    logs = sub.add_parser("logs", help="collect the event logs of the median runs")
    logs.add_argument("--bench", nargs="+", required=True, help="LABEL=benchmark out dir")
    logs.add_argument("--logs", type=Path, default=Path("build/ui-logs"))
    shoot = sub.add_parser("shoot", help="screenshot the History Server pages")
    shoot.add_argument("--logs", type=Path, default=Path("build/ui-logs"))
    shoot.add_argument("--url", default="http://localhost:18080")
    shoot.add_argument("--out", type=Path, default=Path("build/screenshots"))
    shoot.add_argument("--timeout", type=float, default=300)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return cmd_logs(args) if args.command == "logs" else cmd_shoot(args)


if __name__ == "__main__":
    sys.exit(main())
