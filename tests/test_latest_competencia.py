"""Monthly check helper (``scripts/latest_competencia.py``) on synthetic WebDAV listings."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
import requests

from cnpj_lakehouse.download import LOOKUP_FILES

SCRIPT = Path(__file__).parents[1] / "scripts" / "latest_competencia.py"
BASE = "https://example.test/dav/CNPJ"
PREFIX = "/dav/CNPJ"
SUBSET = (*LOOKUP_FILES, "Empresas1.zip", "Estabelecimentos1.zip")


@pytest.fixture(scope="module")
def latest() -> ModuleType:
    spec = importlib.util.spec_from_file_location("latest_competencia", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def multistatus(hrefs: list[str]) -> str:
    responses = "".join(f"<d:response><d:href>{h}</d:href></d:response>" for h in hrefs)
    return f'<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">{responses}</d:multistatus>'


def fake_portal(folders: dict[str, tuple[str, ...]]) -> dict[str, list[str]]:
    """Listing per URL: the root (months + a stray file) and one listing per month."""
    root = [f"{PREFIX}/", f"{PREFIX}/cnpj.tar.gz", *(f"{PREFIX}/{m}/" for m in folders)]
    listings = {f"{BASE}/": root}
    for month, names in folders.items():
        listings[f"{BASE}/{month}/"] = [
            f"{PREFIX}/{month}/",
            *(f"{PREFIX}/{month}/{n}" for n in names),
        ]
    return listings


def test_parse_hrefs_decodes_and_months_are_sorted(latest: ModuleType) -> None:
    hrefs = latest.parse_hrefs(
        multistatus([f"{PREFIX}/2026-09/", f"{PREFIX}/2026-10/", "/x/My%20File.zip"])
    )
    assert hrefs[-1] == "/x/My File.zip"
    assert latest.months_desc([*hrefs, f"{PREFIX}/2025-13/", f"{PREFIX}/notes/"]) == [
        "2026-10",
        "2026-09",
    ]
    assert latest.file_names(hrefs) == {"My File.zip"}


def test_newest_complete_month_wins(latest: ModuleType) -> None:
    listings = fake_portal({"2026-08": SUBSET, "2026-09": SUBSET, "2026-10": ("Cnaes.zip",)})
    seen: list[str] = []

    def lister(url: str) -> list[str]:
        seen.append(url)
        return listings[url]

    # 2026-10 is still being uploaded: the check falls back to the newest complete month.
    assert latest.latest_competencia(BASE + "/", SUBSET, lister) == "2026-09"
    assert seen == [f"{BASE}/", f"{BASE}/2026-10/", f"{BASE}/2026-09/"]


def test_main_prints_the_month_or_fails(
    latest: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    listings = fake_portal({"2026-10": SUBSET})
    assert latest.main(["--base-url", BASE], lister=listings.__getitem__) == 0
    assert capsys.readouterr().out.strip() == "2026-10"

    assert latest.main(["--base-url", BASE, "--files", "Socios0"], lister=listings.__getitem__) == 1
    assert "no published month" in capsys.readouterr().err
    assert latest.main(["--files", "nope"], lister=listings.__getitem__) == 1


def test_propfind_lister_sends_depth_1(latest: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, dict[str, str]]] = []

    class FakeResponse:
        text = multistatus([f"{PREFIX}/2026-10/"])

        def raise_for_status(self) -> None:
            pass

    def fake_request(
        self: requests.Session, method: str, url: str, **kwargs: object
    ) -> FakeResponse:
        calls.append((method, url, kwargs["headers"]))  # type: ignore[arg-type]
        return FakeResponse()

    monkeypatch.setattr(requests.Session, "request", fake_request)
    assert latest.propfind_lister(requests.Session())(f"{BASE}/") == [f"{PREFIX}/2026-10/"]
    assert calls[0][0] == "PROPFIND" and calls[0][2]["Depth"] == "1"
