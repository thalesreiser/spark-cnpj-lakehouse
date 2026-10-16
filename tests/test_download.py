"""RF-01: unit tests for the pure functions and integration tests against a local HTTP server.

The integration tests serve small generated zip files from `http.server` in a thread (no
internet, no Docker) and run the real download code end to end.
"""

from __future__ import annotations

import hashlib
import io
import json
import random
import threading
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests

from cnpj_lakehouse import download as dl

COMPETENCIA = "2026-09"


def make_zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


# --------------------------------------------------------------------------------------------
# Local HTTP server
# --------------------------------------------------------------------------------------------


@dataclass
class FakeSource:
    """In-memory files served under ``/<competencia>/<name>``, with failure injection."""

    files: dict[str, bytes] = field(default_factory=dict)
    supports_range: bool = True
    # Status codes returned (and consumed) before serving normally, per file name.
    fail_with: dict[str, list[int]] = field(default_factory=dict)
    # Number of bytes sent before dropping the connection on the next request, per file name.
    truncate_at: dict[str, int] = field(default_factory=dict)
    requests: list[tuple[str, str | None]] = field(default_factory=list)

    def gets(self, name: str) -> list[str | None]:
        return [rng for path, rng in self.requests if path.endswith("/" + name)]


class _Handler(BaseHTTPRequestHandler):
    source: FakeSource

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        src = self.server.source  # type: ignore[attr-defined]
        rng = self.headers.get("Range")
        src.requests.append((self.path, rng))
        name = self.path.rsplit("/", 1)[-1]
        if not self.path.startswith(f"/{COMPETENCIA}/") or name not in src.files:
            self.send_error(404)
            return
        if src.fail_with.get(name):
            self.send_error(src.fail_with[name].pop(0))
            return
        body = src.files[name]
        start = 0
        if rng and src.supports_range:
            start = int(rng.removeprefix("bytes=").split("-")[0])
            if start >= len(body):
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{len(body)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(body) - 1}/{len(body)}")
        else:
            self.send_response(200)
        payload = body[start:]
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Type", "application/zip")
        self.end_headers()
        cut = src.truncate_at.pop(name, None)
        if cut is not None:
            self.wfile.write(payload[:cut])
            self.wfile.flush()
            self.close_connection = True
            return
        self.wfile.write(payload)


@pytest.fixture()
def source() -> Iterator[tuple[FakeSource, str]]:
    src = FakeSource(
        files={
            "Cnaes.zip": make_zip({"F.K03200$Z.D60913.CNAECSV": b'"0111301";"Cultivo de arroz"\n'}),
            "Motivos.zip": make_zip({"F.K03200$Z.D60913.MOTICSV": b'"00";"SEM MOTIVO"\n'}),
            "Empresas0.zip": make_zip(
                {"K3241.K03200Y0.D60913.EMPRECSV": random.Random(0).randbytes(200_000)}
            ),
        }
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.source = src  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield src, f"http://127.0.0.1:{server.server_address[1]}/"
    finally:
        server.shutdown()
        server.server_close()


def no_sleep(_: float) -> None:
    pass


def run(base_url: str, dest: Path, files: tuple[str, ...]) -> dict[str, object]:
    return dl.download_competencia(
        COMPETENCIA, files, dest=dest, base_url=base_url, backoff=0, timeout=5, sleep=no_sleep
    )


# --------------------------------------------------------------------------------------------
# Integration tests
# --------------------------------------------------------------------------------------------


def test_download_writes_files_and_manifest(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    files = ("Cnaes.zip", "Motivos.zip", "Empresas0.zip")
    manifest = run(base_url, tmp_path / "raw", files)

    out = tmp_path / "raw" / COMPETENCIA
    on_disk = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert on_disk["competencia"] == COMPETENCIA
    assert on_disk["files"] == manifest["files"]
    assert [f["name"] for f in on_disk["files"]] == sorted(files)
    for entry in on_disk["files"]:
        content = src.files[entry["name"]]
        assert (out / entry["name"]).read_bytes() == content
        assert entry["sha256"] == hashlib.sha256(content).hexdigest()
        assert entry["size_bytes"] == len(content)
        assert entry["url"] == f"{base_url.rstrip('/')}/{COMPETENCIA}/{entry['name']}"
        assert entry["downloaded_at"].endswith("Z")
        assert len(entry["extracted"]) == 1
        assert (out / entry["extracted"][0]).is_file()
    assert not list(out.glob("*.part"))


def test_rerun_is_idempotent(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    run(base_url, tmp_path, ("Cnaes.zip", "Motivos.zip"))
    first = json.loads((tmp_path / COMPETENCIA / "manifest.json").read_text(encoding="utf-8"))
    src.requests.clear()

    run(base_url, tmp_path, ("Cnaes.zip", "Motivos.zip"))
    second = json.loads((tmp_path / COMPETENCIA / "manifest.json").read_text(encoding="utf-8"))
    assert src.requests == []
    assert second["files"] == first["files"]


def test_subsets_accumulate_in_the_manifest(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    _, base_url = source
    run(base_url, tmp_path, ("Cnaes.zip",))
    manifest = run(base_url, tmp_path, ("Motivos.zip",))
    assert [f["name"] for f in manifest["files"]] == ["Cnaes.zip", "Motivos.zip"]  # type: ignore[index]


def test_corrupted_zip_is_downloaded_again(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    run(base_url, tmp_path, ("Cnaes.zip",))
    (tmp_path / COMPETENCIA / "Cnaes.zip").write_bytes(b"corrupted")
    src.requests.clear()

    run(base_url, tmp_path, ("Cnaes.zip",))
    assert len(src.gets("Cnaes.zip")) == 1
    assert (tmp_path / COMPETENCIA / "Cnaes.zip").read_bytes() == src.files["Cnaes.zip"]


def test_resume_with_range(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    content = src.files["Empresas0.zip"]
    out = tmp_path / COMPETENCIA
    out.mkdir()
    (out / "Empresas0.zip.part").write_bytes(content[:1000])

    manifest = run(base_url, tmp_path, ("Empresas0.zip",))
    assert src.gets("Empresas0.zip") == ["bytes=1000-"]
    assert (out / "Empresas0.zip").read_bytes() == content
    assert manifest["files"][0]["sha256"] == hashlib.sha256(content).hexdigest()  # type: ignore[index]


def test_resume_without_range_support_restarts(
    source: tuple[FakeSource, str], tmp_path: Path
) -> None:
    src, base_url = source
    src.supports_range = False
    content = src.files["Empresas0.zip"]
    out = tmp_path / COMPETENCIA
    out.mkdir()
    (out / "Empresas0.zip.part").write_bytes(b"garbage-from-an-old-attempt")

    run(base_url, tmp_path, ("Empresas0.zip",))
    assert src.gets("Empresas0.zip") == ["bytes=27-"]
    assert (out / "Empresas0.zip").read_bytes() == content


def test_complete_part_file_is_finished_without_body(
    source: tuple[FakeSource, str], tmp_path: Path
) -> None:
    src, base_url = source
    out = tmp_path / COMPETENCIA
    out.mkdir()
    (out / "Cnaes.zip.part").write_bytes(src.files["Cnaes.zip"])

    run(base_url, tmp_path, ("Cnaes.zip",))
    assert (out / "Cnaes.zip").read_bytes() == src.files["Cnaes.zip"]


def test_interrupted_transfer_is_resumed(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    # The connection drops after 150 kB: the complete 64 KiB chunks already written are kept.
    src.truncate_at["Empresas0.zip"] = 150_000
    content = src.files["Empresas0.zip"]

    run(base_url, tmp_path, ("Empresas0.zip",))
    first, second = src.gets("Empresas0.zip")
    assert first is None
    assert second is not None
    offset = int(second.removeprefix("bytes=").rstrip("-"))
    assert 0 < offset <= 150_000
    assert (tmp_path / COMPETENCIA / "Empresas0.zip").read_bytes() == content


def test_transient_errors_are_retried_with_backoff(
    source: tuple[FakeSource, str], tmp_path: Path
) -> None:
    src, base_url = source
    src.fail_with["Cnaes.zip"] = [503, 429]
    delays: list[float] = []
    dl.download_competencia(
        COMPETENCIA,
        ("Cnaes.zip",),
        dest=tmp_path,
        base_url=base_url,
        backoff=1.0,
        sleep=delays.append,
    )
    assert delays == [1.0, 2.0]
    assert len(src.gets("Cnaes.zip")) == 3


def test_retries_are_exhausted(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    src.fail_with["Cnaes.zip"] = [500, 500, 500]
    with pytest.raises(dl.DownloadError, match="giving up after 3 attempts"):
        dl.download_competencia(
            COMPETENCIA, ("Cnaes.zip",), dest=tmp_path, base_url=base_url, retries=2, sleep=no_sleep
        )
    assert not (tmp_path / COMPETENCIA / "Cnaes.zip").exists()


def test_missing_file_fails_without_retry(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    with pytest.raises(dl.DownloadError, match="HTTP 404.*published"):
        run(base_url, tmp_path, ("Socios9.zip",))
    assert len(src.gets("Socios9.zip")) == 1


def test_connection_error_is_transient(tmp_path: Path) -> None:
    with pytest.raises(dl.DownloadError, match="giving up after 2 attempts"):
        dl.download_competencia(
            COMPETENCIA,
            ("Cnaes.zip",),
            dest=tmp_path,
            base_url="http://127.0.0.1:9",  # discard port: connection refused
            retries=1,
            timeout=2,
            sleep=no_sleep,
        )


def test_main_cli_end_to_end(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    src, base_url = source
    code = dl.main(
        [
            "--competencia",
            COMPETENCIA,
            "--files",
            "cnaes,motivos",
            "--dest",
            str(tmp_path),
            "--base-url",
            base_url,
        ]
    )
    assert code == 0
    manifest = json.loads((tmp_path / COMPETENCIA / "manifest.json").read_text(encoding="utf-8"))
    assert [f["name"] for f in manifest["files"]] == ["Cnaes.zip", "Motivos.zip"]


def test_main_cli_returns_error_code(source: tuple[FakeSource, str], tmp_path: Path) -> None:
    _, base_url = source
    argv = ["--competencia", "2020-01", "--files", "Cnaes", "--dest", str(tmp_path)]
    assert dl.main([*argv, "--base-url", base_url, "--retries", "0"]) == 1


def test_main_uses_base_url_from_env(
    source: tuple[FakeSource, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, base_url = source
    monkeypatch.setenv(dl.BASE_URL_ENV, base_url)
    assert dl.main(["--competencia", COMPETENCIA, "--files", "Cnaes", "--dest", str(tmp_path)]) == 0


# --------------------------------------------------------------------------------------------
# Unit tests (pure functions and argument validation)
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["2026-09", "2000-01", " 2023-12 "])
def test_parse_competencia_accepts_valid_months(value: str) -> None:
    assert dl.parse_competencia(value) == value.strip()


@pytest.mark.parametrize(
    "value", ["2026-13", "2026-00", "2026-9", "202609", "26-09", "1999-12", ""]
)
def test_parse_competencia_rejects_invalid_months(value: str) -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        dl.parse_competencia(value)


def test_all_files_cover_the_monthly_release() -> None:
    assert len(dl.ALL_FILES) == 37
    assert len(set(dl.ALL_FILES)) == 37
    assert dl.resolve_files(["all"]) == dl.ALL_FILES


def test_resolve_files_groups_names_and_order() -> None:
    assert dl.resolve_files(["lookups"]) == dl.LOOKUP_FILES
    assert dl.resolve_files(["simples.zip", "EMPRESAS0", "cnaes"]) == (
        "Empresas0.zip",
        "Simples.zip",
        "Cnaes.zip",
    )
    assert dl.resolve_files(["Socios1,socios1", " ,Paises"]) == ("Socios1.zip", "Paises.zip")
    assert len(dl.resolve_files(["estabelecimentos", "socios"])) == 20


@pytest.mark.parametrize("tokens", [["Empresas10"], ["foo"], [""], [" , "]])
def test_resolve_files_rejects_unknown_or_empty(tokens: list[str]) -> None:
    with pytest.raises(ValueError):
        dl.resolve_files(tokens)


def test_paths_and_urls() -> None:
    assert (
        dl.build_url("https://x/CNPJ/", "2026-09", "Cnaes.zip")
        == "https://x/CNPJ/2026-09/Cnaes.zip"
    )
    assert dl.competencia_dir(Path("data/raw"), "2026-09") == Path("data/raw/2026-09")
    assert dl.part_path(Path("a/Cnaes.zip")) == Path("a/Cnaes.zip.part")
    assert dl.DEFAULT_BASE_URL.startswith("https://arquivos.receitafederal.gov.br/")


def test_range_header() -> None:
    assert dl.range_header(0) == {}
    assert dl.range_header(10) == {"Range": "bytes=10-"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("bytes 100-1179/1180", (100, 1180)),
        ("bytes */1180", (None, 1180)),
        ("bytes 0-9/*", (0, None)),
        ("items 1-2/3", (None, None)),
        (None, (None, None)),
    ],
)
def test_parse_content_range(value: str | None, expected: tuple[int | None, int | None]) -> None:
    assert dl.parse_content_range(value) == expected


def test_backoff_delay_grows_and_is_capped() -> None:
    assert [dl.backoff_delay(n, 2.0) for n in (1, 2, 3)] == [2.0, 4.0, 8.0]
    assert dl.backoff_delay(10, 2.0, cap=30) == 30.0


@pytest.mark.parametrize(
    ("name", "safe"),
    [
        ("F.K03200$Z.D60913.CNAECSV", True),
        ("sub/file.csv", True),
        ("../evil", False),
        ("/etc/passwd", False),
        ("a\\..\\evil", False),
        ("C:evil", False),
    ],
)
def test_is_safe_member(name: str, safe: bool) -> None:
    assert dl.is_safe_member(name) is safe


def test_extract_zip_rejects_path_traversal(tmp_path: Path) -> None:
    zip_path = tmp_path / "evil.zip"
    zip_path.write_bytes(make_zip({"../evil.txt": b"x"}))
    with pytest.raises(dl.DownloadError, match="unsafe"):
        dl.extract_zip(zip_path, tmp_path / "out")
    assert not (tmp_path / "evil.txt").exists()


def test_extract_zip_rejects_invalid_archive(tmp_path: Path) -> None:
    zip_path = tmp_path / "bad.zip"
    zip_path.write_bytes(b"not a zip")
    with pytest.raises(dl.DownloadError, match="not a valid zip"):
        dl.extract_zip(zip_path, tmp_path)


def test_manifest_round_trip(tmp_path: Path) -> None:
    record = dl.FileRecord("Cnaes.zip", "u", 3, "abc", "2026-10-16T00:00:00Z", ("a.csv",))
    manifest = dl.build_manifest("2026-09", "https://x", [record], "2026-10-16T00:00:01Z")
    assert manifest["files"][0]["extracted"] == ["a.csv"]
    path = tmp_path / "manifest.json"
    dl.write_manifest(path, manifest)
    assert dl.records_from_manifest(dl.load_manifest(path)) == {"Cnaes.zip": record}
    assert dl.load_manifest(tmp_path / "missing.json") == {}


def test_parse_args_defaults_and_validation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = dl.parse_args(["--competencia", "2026-09"], {})
    assert args.files == dl.resolve_files(["lookups"])  # safe default, not the whole release
    assert dl.parse_args(["--competencia", "2026-09", "--files", "all"], {}).files == dl.ALL_FILES
    assert args.dest == Path("data/raw")
    assert args.base_url == dl.DEFAULT_BASE_URL
    assert dl.parse_args(["--competencia", "2026-09"], {dl.BASE_URL_ENV: "http://m"}).base_url == (
        "http://m"
    )
    with pytest.raises(SystemExit):
        dl.parse_args(["--competencia", "2026-9"], {})
    assert "YYYY-MM" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        dl.parse_args(["--competencia", "2026-09", "--files", "nope"], {})
    assert "unknown file or group" in capsys.readouterr().err


def test_requests_session_is_used_by_default(
    source: tuple[FakeSource, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[requests.Session] = []
    original = requests.Session

    def factory() -> requests.Session:
        created.append(original())
        return created[-1]

    monkeypatch.setattr(dl.requests, "Session", factory)
    _, base_url = source
    run(base_url, tmp_path, ("Cnaes.zip",))
    assert len(created) == 1
