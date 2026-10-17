"""RST-01 guard (``scripts/check_fixture_cnpjs.py``) on tiny synthetic files."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "check_fixture_cnpjs.py"


@pytest.fixture(scope="module")
def check() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_fixture_cnpjs", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _release(raw: Path, real_bases: list[str]) -> None:
    month = raw / "2026-09"
    month.mkdir(parents=True)
    files = []
    for i in range(10):
        name = f"EMPRE{i}.CSV"
        lines = [f'"{b}";"X";"2062"\n' for b in real_bases if int(b) % 10 == i]
        (month / name).write_bytes("".join(lines).encode("latin-1"))
        files.append({"name": f"Empresas{i}.zip", "extracted": [name]})
    (month / "manifest.json").write_text(json.dumps({"files": files}), encoding="utf-8")


def test_the_versioned_fixture_has_the_expected_bases(check: ModuleType) -> None:
    bases = check.fixture_bases(Path(__file__).parent / "fixtures")
    assert len(bases) == 10  # Empresas bases; Simples reuses them, CNAE codes have 7 digits


def test_passes_when_no_fixture_base_is_real(check: ModuleType, tmp_path: Path) -> None:
    fixtures = tmp_path / "fixtures"
    fixtures.mkdir()
    (fixtures / "a.csv").write_bytes(b'"12345678";"FAKE"\n"1234567";"CNAE"\n')
    _release(tmp_path / "raw", ["00000000", "12345677", "12345679"])
    args = ["--competencia", "2026-09", "--raw-dir", str(tmp_path / "raw")]
    summary = tmp_path / "summary.json"
    code = check.main([*args, "--fixtures", str(fixtures), "--summary", str(summary)])
    assert code == 0
    result = json.loads(summary.read_text("utf-8"))
    assert result["real_distinct_numeric_bases"] == 3
    assert result["fixture_bases"] == 1 and result["collisions"] == 0


def test_fails_and_suggests_free_blocks_on_a_collision(check: ModuleType, tmp_path: Path) -> None:
    fixtures = tmp_path / "fixtures"
    fixtures.mkdir()
    (fixtures / "a.csv").write_bytes(b'"00000005";"FAKE"\n')
    _release(tmp_path / "raw", [f"{b:08d}" for b in range(30)])
    summary = tmp_path / "summary.json"
    args = ["--competencia", "2026-09", "--raw-dir", str(tmp_path / "raw")]
    assert check.main([*args, "--fixtures", str(fixtures), "--summary", str(summary)]) == 1
    result = json.loads(summary.read_text("utf-8"))
    assert result["collisions"] == 1
    assert result["suggested_free_blocks"][0] == "00000030-00000049"


def test_requires_every_empresas_file(check: ModuleType, tmp_path: Path) -> None:
    month = tmp_path / "2026-09"
    month.mkdir()
    (month / "manifest.json").write_text('{"files": []}', encoding="utf-8")
    with pytest.raises(SystemExit, match="incomplete release"):
        check.empresas_files(month)
