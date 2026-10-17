"""RST-01 guard: the synthetic CNPJ bases of ``tests/fixtures`` must not exist in the real registry.

Collects every 8-digit ``cnpj_basico`` used as the first field of a fixture CSV and scans the
first field of the REAL ``Empresas*`` files of a downloaded release (``make download
FILES=empresas``). ``Empresas`` lists every base of the registry, so a fixture base absent from
it is not assigned to any company or person in that release. The CSVs are streamed line by line;
only a 100 MB presence table (one byte per possible base) is kept in memory.

Prints and writes a summary with counts only (never data) and exits with code 1 on any
collision, also suggesting free blocks of bases for the fixture.

    python scripts/check_fixture_cnpjs.py --competencia 2026-09 --summary build/fixture-cnpjs.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from cnpj_lakehouse.download import MANIFEST_NAME, competencia_dir, load_manifest

BASE_DIGITS = 8
BASE_SPACE = 10**BASE_DIGITS
EMPRESAS_ZIPS = tuple(f"Empresas{i}.zip" for i in range(10))
_FIXTURE_BASE = re.compile(rb'^"(\d{8})";')


def fixture_bases(root: Path) -> set[str]:
    """8-digit bases used as the first (quoted) field of any CSV line under ``root``."""
    bases: set[str] = set()
    for path in sorted(p for p in root.rglob("*") if p.is_file() and p.suffix != ".json"):
        for line in path.read_bytes().splitlines():
            if match := _FIXTURE_BASE.match(line):
                bases.add(match.group(1).decode("ascii"))
    return bases


def empresas_files(raw_month: Path) -> list[Path]:
    """Extracted CSVs of the ten ``Empresas*.zip`` of a release, from its ``manifest.json``."""
    entries = {e["name"]: e for e in load_manifest(raw_month / MANIFEST_NAME).get("files", [])}
    missing = [name for name in EMPRESAS_ZIPS if name not in entries]
    if missing:
        raise SystemExit(f"incomplete release in {raw_month}: missing {missing} (FILES=empresas)")
    return [raw_month / n for zip_name in EMPRESAS_ZIPS for n in entries[zip_name]["extracted"]]


def scan_real_bases(paths: Iterable[Path]) -> tuple[bytearray, dict[str, int]]:
    """Marks every numeric base found in the first field of ``paths`` in a presence table."""
    present = bytearray(BASE_SPACE)
    stats = {"lines": 0, "non_numeric": 0}
    for path in paths:
        with path.open("rb") as handle:
            for line in handle:
                stats["lines"] += 1
                field = line[1 : 1 + BASE_DIGITS] if line.startswith(b'"') else line[:BASE_DIGITS]
                if field.isdigit() and len(field) == BASE_DIGITS:
                    present[int(field)] = 1
                else:  # e.g. the alphanumeric CNPJ: it cannot match a digits-only base
                    stats["non_numeric"] += 1
    stats["distinct_numeric_bases"] = BASE_SPACE - present.count(0)
    return present, stats


def free_blocks(present: bytearray, size: int, starts: Iterable[int]) -> list[str]:
    """First run of ``size`` free bases at or after each start (as ``first-last``)."""
    needle = bytes(size)
    blocks = []
    for start in starts:
        found = present.find(needle, start)
        if found >= 0:
            blocks.append(f"{found:08d}-{found + size - 1:08d}")
    return blocks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--competencia", required=True, help="release with Empresas, YYYY-MM")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--fixtures", type=Path, default=Path("tests/fixtures"))
    parser.add_argument("--summary", type=Path, help="JSON summary to write (counts only)")
    args = parser.parse_args(argv)

    ours = fixture_bases(args.fixtures)
    if not ours:
        raise SystemExit(f"no 8-digit CNPJ base found under {args.fixtures}")
    files = empresas_files(competencia_dir(args.raw_dir, args.competencia))
    present, stats = scan_real_bases(files)
    collisions = sorted(b for b in ours if present[int(b)])

    summary: dict[str, Any] = {
        "competencia": args.competencia,
        "empresas_files": len(files),
        "real_lines_scanned": stats["lines"],
        "real_distinct_numeric_bases": stats["distinct_numeric_bases"],
        "real_non_numeric_bases": stats["non_numeric"],
        "fixture_bases": len(ours),
        "fixture_range": f"{min(ours)}-{max(ours)}",
        "collisions": len(collisions),
    }
    if collisions:
        span = int(max(ours)) - int(min(ours)) + 1
        summary["suggested_free_blocks"] = free_blocks(
            present, max(span, 20), range(0, BASE_SPACE, BASE_SPACE // 10)
        )
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(
        f"release {args.competencia}: scanned {stats['lines']} lines of {len(files)} Empresas "
        f"file(s), {stats['distinct_numeric_bases']} distinct real bases; "
        f"{len(ours)} fixture bases ({summary['fixture_range']}); {len(collisions)} collision(s)"
    )
    if collisions:
        print(f"::error::fixture bases present in the real registry: {collisions}")
        print(f"free blocks: {summary['suggested_free_blocks']}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
