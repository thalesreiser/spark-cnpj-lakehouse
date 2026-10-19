"""RF-03 / RF-04 proof on CI: the silver Delta tables have no duplicated business key.

Run inside the `spark-client` container after `make silver`. For each table of the run summary
(``build/silver-<competencia>.json``) it reads ``s3a://<bucket>/silver/<table>`` (the current
state of every key across months, ADR 011) and checks that:

* the table has as many rows as the summary reports (``table_rows``), the bronze partition of
  the month as many as ``rows_in``, ``rows_in = rows_out + duplicates_removed +
  rows_without_key + invalid_cnpj_dropped`` and ``rows_out = rows_inserted + rows_updated +
  rows_unchanged`` (the MERGE metrics);
* **no business key appears more than once** (`cnpj_lakehouse.silver.duplicate_keys`);
* every date column is typed ``date``, and every non-empty source date that did not become a
  date is really invalid: the distinct failing values are re-checked with Python's
  ``datetime.strptime`` (an oracle independent of `parse_date`), so 100% of the valid source
  dates are typed.

Writes a JSON and a Markdown summary (counts only, never data) and exits non-zero on failure.

    python scripts/silver_check.py --competencia 2026-09 --summary build/silver-2026-09.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from cnpj_lakehouse.bronze import BRONZE_LAYER, PARTITION_COLUMN
from cnpj_lakehouse.normalize import parse_date
from cnpj_lakehouse.silver import SILVER_LAYER, date_columns, duplicate_keys, is_empty_date
from cnpj_lakehouse.spark import LakehouseConfig, get_spark

# Distinct failing date values re-checked per column (they are never printed).
MAX_FAILURES_CHECKED = 10_000


def really_invalid(value: str) -> bool:
    try:
        datetime.strptime(value.strip(), "%Y%m%d")
    except ValueError:
        return True
    return len(value.strip()) != 8  # strptime accepts "2025011"; the layout needs 8 digits


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competencia", required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=Path("build/silver-check"))
    args = parser.parse_args()

    run_summary = json.loads(args.summary.read_text(encoding="utf-8"))
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("silver-check", cfg)
    results: list[dict[str, Any]] = []
    try:
        for report in run_summary["tables"]:
            table = report["table"]
            month = F.col(PARTITION_COLUMN) == args.competencia
            silver = spark.read.format("delta").load(cfg.path(SILVER_LAYER, table))
            bronze = spark.read.format("delta").load(cfg.path(BRONZE_LAYER, table)).where(month)
            dates: dict[str, Any] = {}
            for name in date_columns(table):
                failing = (
                    bronze.where(~is_empty_date(F.col(name)) & parse_date(F.col(name)).isNull())
                    .select(name)
                    .distinct()
                    .limit(MAX_FAILURES_CHECKED)
                    .collect()
                )
                dates[name] = {
                    "typed_as_date": isinstance(silver.schema[name].dataType, DateType),
                    "source_values": report["date_values"][name],
                    "parse_failures": report["date_parse_failures"][name],
                    "distinct_failures_checked": len(failing),
                    "valid_dates_lost": sum(1 for row in failing if not really_invalid(row[0])),
                }
            result: dict[str, Any] = {
                "table": table,
                "key": report["key"],
                "bronze_rows": bronze.count(),
                "silver_rows": silver.count(),
                "rows_in": report["rows_in"],
                "rows_out": report["rows_out"],
                "duplicates_removed": report["duplicates_removed"],
                "operation": report["operation"],
                "table_version": report["table_version"],
                "rows_inserted": report["rows_inserted"],
                "rows_updated": report["rows_updated"],
                "rows_unchanged": report["rows_unchanged"],
                "table_rows": report["table_rows"],
                "rows_without_key": report["rows_without_key"],
                "invalid_cnpj": report["invalid_cnpj"],
                "invalid_cnpj_dropped": report["invalid_cnpj_dropped"],
                "duplicate_keys": duplicate_keys(silver, table),
                "dates": dates,
            }
            result["ok"] = (
                result["duplicate_keys"] == 0
                and result["silver_rows"] == report["table_rows"] > 0
                and report["rows_out"] > 0
                and report["rows_out"]
                == report["rows_inserted"] + report["rows_updated"] + report["rows_unchanged"]
                and result["bronze_rows"] == report["rows_in"]
                and report["rows_in"]
                == report["rows_out"]
                + report["duplicates_removed"]
                + report["rows_without_key"]
                + report["invalid_cnpj_dropped"]
                and all(d["typed_as_date"] and d["valid_dates_lost"] == 0 for d in dates.values())
            )
            print(f"[silver-check] {json.dumps(result)}")
            results.append(result)
    finally:
        spark.stop()

    ok = bool(results) and all(r["ok"] for r in results)
    args.out.mkdir(parents=True, exist_ok=True)
    summary = {"competencia": args.competencia, "ok": ok, "tables": results}
    (args.out / "silver-check.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        f"### Silver checks - competencia {args.competencia}",
        "",
        "| Table | Business key | Rows in | Rows out | Duplicates removed | Without key"
        " | Invalid CNPJ | Inserted | Updated | Unchanged | Table rows | Duplicate keys"
        " | Date columns (failures) | OK |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in results:
        dates_text = (
            ", ".join(f"`{n}` ({d['parse_failures']:,})" for n, d in r["dates"].items()) or "none"
        )
        lines.append(
            f"| {r['table']} | {' + '.join(r['key'])} | {r['rows_in']:,} | {r['rows_out']:,}"
            f" | {r['duplicates_removed']:,} | {r['rows_without_key']:,} | {r['invalid_cnpj']:,}"
            f" | {r['rows_inserted']:,} | {r['rows_updated']:,} | {r['rows_unchanged']:,}"
            f" | {r['table_rows']:,} | {r['duplicate_keys']:,} | {dates_text}"
            f" | {'yes' if r['ok'] else 'NO'} |"
        )
    (args.out / "silver-check.md").write_text("\n".join(lines) + "\n")
    print("[silver-check] OK" if ok else "[silver-check] FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
