"""RF-01: download parameterized by reference month, with a recorded checksum.

Downloads the zipped CSV files of one Receita Federal CNPJ release (reference month,
`COMPETENCIA`, ``YYYY-MM``) into ``<dest>/<competencia>/``, unzips them into the same folder
and records a ``manifest.json`` with the SHA-256 of every zip (see ``docs/data-source.md``).

Usage::

    python -m cnpj_lakehouse.download --competencia 2026-09 [--files lookups Empresas0]
        [--dest data/raw] [--base-url URL]

The module follows RNF-04: pure functions (validation, file selection, URLs, paths, manifest)
are kept apart from the I/O functions (HTTP download, unzip, manifest persistence) and from the
orchestration (`download_competencia`, `main`).

| Variable        | Default              | Purpose                                         |
|-----------------|----------------------|-------------------------------------------------|
| `CNPJ_BASE_URL` | `DEFAULT_BASE_URL`   | folder that contains the `YYYY-MM/` subfolders  |
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import requests

log = logging.getLogger("cnpj_lakehouse.download")

# Public WebDAV endpoint of the Receita Federal Nextcloud share (checked on 2026-10-16).
# Override it with --base-url or CNPJ_BASE_URL if the source moves again (risk R-04).
DEFAULT_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK"
    "/Dados/Cadastros/CNPJ"
)
BASE_URL_ENV = "CNPJ_BASE_URL"
DEFAULT_DEST = Path("data/raw")
MANIFEST_NAME = "manifest.json"
PART_SUFFIX = ".part"
CHUNK_SIZE = 64 * 1024
# HTTP statuses worth retrying (rate limiting and server-side errors).
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

_COMPETENCIA_RE = re.compile(r"^(\d{4})-(\d{2})$")
_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(?:(\d+)-(\d+)|\*)/(\d+|\*)$")

LOOKUP_FILES: tuple[str, ...] = (
    "Cnaes.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
)
FILE_GROUPS: dict[str, tuple[str, ...]] = {
    "empresas": tuple(f"Empresas{i}.zip" for i in range(10)),
    "estabelecimentos": tuple(f"Estabelecimentos{i}.zip" for i in range(10)),
    "socios": tuple(f"Socios{i}.zip" for i in range(10)),
    "simples": ("Simples.zip",),
    "lookups": LOOKUP_FILES,
}
# Every file of a monthly release, in a stable order (37 zips).
ALL_FILES: tuple[str, ...] = tuple(name for group in FILE_GROUPS.values() for name in group)
FILE_GROUPS["all"] = ALL_FILES


class DownloadError(RuntimeError):
    """A file could not be downloaded (non-retryable error or retries exhausted)."""


class TransientError(DownloadError):
    """A recoverable failure (network error, 5xx, truncated body): the attempt is retried."""


# --------------------------------------------------------------------------------------------
# Pure functions
# --------------------------------------------------------------------------------------------


def parse_competencia(value: str) -> str:
    """Validates a reference month in the ``YYYY-MM`` format and returns it normalized."""
    match = _COMPETENCIA_RE.match(value.strip())
    if not match or not 1 <= int(match.group(2)) <= 12 or int(match.group(1)) < 2000:
        raise ValueError(f"invalid reference month {value!r}: expected YYYY-MM (e.g. 2026-09)")
    return value.strip()


def resolve_files(tokens: Iterable[str]) -> tuple[str, ...]:
    """Expands group names and file names into the list of zip files to download.

    Tokens may be separated by commas or given one by one; they are case-insensitive and the
    ``.zip`` suffix is optional (``lookups``, ``Empresas0``, ``simples.zip``). The result keeps
    the order of `ALL_FILES` and has no duplicates.
    """
    canonical = {name.lower().removesuffix(".zip"): name for name in ALL_FILES}
    selected: set[str] = set()
    for raw in tokens:
        for token in raw.split(","):
            key = token.strip().lower().removesuffix(".zip")
            if not key:
                continue
            if key in FILE_GROUPS:
                selected.update(FILE_GROUPS[key])
            elif key in canonical:
                selected.add(canonical[key])
            else:
                groups = ", ".join(FILE_GROUPS)
                raise ValueError(f"unknown file or group {token.strip()!r} (groups: {groups})")
    if not selected:
        raise ValueError("no files selected")
    return tuple(name for name in ALL_FILES if name in selected)


def build_url(base_url: str, competencia: str, file_name: str) -> str:
    """URL of one file of a release: ``<base_url>/<competencia>/<file_name>``."""
    return f"{base_url.rstrip('/')}/{competencia}/{file_name}"


def competencia_dir(dest: Path, competencia: str) -> Path:
    """Folder of a release: ``<dest>/<competencia>/`` (e.g. ``data/raw/2026-09``)."""
    return dest / competencia


def part_path(path: Path) -> Path:
    """Temporary file used while a download is in progress (renamed when complete)."""
    return path.with_name(path.name + PART_SUFFIX)


def range_header(offset: int) -> dict[str, str]:
    """Request headers to resume a download from ``offset`` bytes (empty when starting)."""
    return {"Range": f"bytes={offset}-"} if offset > 0 else {}


def parse_content_range(value: str | None) -> tuple[int | None, int | None]:
    """Parses ``Content-Range`` into ``(start, total)``; unknown parts are ``None``."""
    if not value:
        return None, None
    match = _CONTENT_RANGE_RE.match(value.strip())
    if not match:
        return None, None
    start = int(match.group(1)) if match.group(1) is not None else None
    total = int(match.group(3)) if match.group(3) != "*" else None
    return start, total


def backoff_delay(attempt: int, base: float, cap: float = 60.0) -> float:
    """Exponential backoff (seconds) before retry number ``attempt`` (1-based)."""
    return float(min(cap, base * 2 ** (attempt - 1)))


def is_safe_member(name: str) -> bool:
    """True when a zip member can be extracted without escaping the target folder."""
    path = PurePosixPath(name.replace("\\", "/"))
    return bool(name) and not path.is_absolute() and ".." not in path.parts and ":" not in name


@dataclass(frozen=True)
class FileRecord:
    """Manifest entry of one downloaded zip."""

    name: str
    url: str
    size_bytes: int
    sha256: str
    downloaded_at: str
    extracted: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FileRecord:
        return cls(
            name=str(data["name"]),
            url=str(data["url"]),
            size_bytes=int(data["size_bytes"]),
            sha256=str(data["sha256"]),
            downloaded_at=str(data["downloaded_at"]),
            extracted=tuple(str(x) for x in data.get("extracted", ())),
        )


def build_manifest(
    competencia: str,
    base_url: str,
    records: Iterable[FileRecord],
    generated_at: str,
) -> dict[str, Any]:
    """Builds the manifest document (files sorted by name)."""
    files = sorted(records, key=lambda r: r.name)
    return {
        "competencia": competencia,
        "base_url": base_url,
        "generated_at": generated_at,
        "algorithm": "sha256",
        "files": [{**asdict(r), "extracted": list(r.extracted)} for r in files],
    }


def records_from_manifest(manifest: Mapping[str, Any]) -> dict[str, FileRecord]:
    """Indexes the records of an existing manifest by file name."""
    return {
        record.name: record
        for record in (FileRecord.from_dict(item) for item in manifest.get("files", []))
    }


def utc_now() -> str:
    """Current UTC time in ISO 8601 (seconds precision)."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# --------------------------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------------------------


def sha256_file(path: Path, chunk_size: int = CHUNK_SIZE) -> str:
    """SHA-256 (hex) of a file, read in chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict[str, Any]:
    """Reads a manifest; returns an empty document when it does not exist."""
    if not path.exists():
        return {}
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    """Writes the manifest atomically (temporary file + rename)."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def extract_zip(zip_path: Path, out_dir: Path) -> tuple[str, ...]:
    """Unzips ``zip_path`` into ``out_dir`` and returns the extracted file names."""
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = [m for m in archive.infolist() if not m.is_dir()]
            unsafe = [m.filename for m in members if not is_safe_member(m.filename)]
            if unsafe:
                raise DownloadError(f"{zip_path.name}: unsafe member path(s) {unsafe}")
            for member in members:
                archive.extract(member, out_dir)
    except zipfile.BadZipFile as exc:
        raise DownloadError(f"{zip_path.name}: not a valid zip file ({exc})") from exc
    return tuple(sorted(m.filename for m in members))


def _attempt_download(
    session: requests.Session,
    url: str,
    part: Path,
    timeout: float,
    chunk_size: int,
) -> bool:
    """One download attempt into ``part``; returns True when the file is complete.

    Resumes with HTTP ``Range`` when ``part`` already has bytes; if the server ignores the
    header (status 200) the file is restarted from scratch.
    """
    offset = part.stat().st_size if part.exists() else 0
    try:
        response = session.get(
            url, headers=range_header(offset), stream=True, timeout=(timeout, timeout)
        )
    except requests.RequestException as exc:
        raise TransientError(f"{url}: {exc}") from exc

    with response:
        status = response.status_code
        if status == 416 and offset > 0:
            _, total = parse_content_range(response.headers.get("Content-Range"))
            if total == offset:
                return True  # the partial file already holds every byte
            part.unlink()
            raise TransientError(f"{url}: invalid resume offset {offset}, restarting")
        if status in RETRYABLE_STATUS:
            raise TransientError(f"{url}: HTTP {status}")
        if status not in (200, 206):
            hint = " (is the reference month published?)" if status == 404 else ""
            raise DownloadError(f"{url}: HTTP {status}{hint}")

        if status == 206:
            start, total = parse_content_range(response.headers.get("Content-Range"))
            if start != offset:
                part.unlink(missing_ok=True)
                raise TransientError(f"{url}: unexpected Content-Range, restarting")
            mode = "ab"
            log.info("resuming %s from byte %d", url, offset)
        else:
            if offset > 0:
                log.info("server ignored Range for %s, restarting from scratch", url)
            mode, offset = "wb", 0
            length = response.headers.get("Content-Length")
            total = int(length) if length and length.isdigit() else None

        try:
            with part.open(mode) as handle:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    handle.write(chunk)
        except requests.RequestException as exc:
            raise TransientError(f"{url}: interrupted ({exc})") from exc

    size = part.stat().st_size
    if total is not None and size != total:
        raise TransientError(f"{url}: got {size} of {total} bytes")
    return True


def download_file(
    session: requests.Session,
    url: str,
    target: Path,
    *,
    retries: int = 5,
    backoff: float = 2.0,
    timeout: float = 60.0,
    chunk_size: int = CHUNK_SIZE,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Downloads ``url`` into ``target`` with retries, backoff and resume; returns its size.

    Bytes go to ``<target>.part`` and the file is renamed only when complete, so ``target``
    never holds a partial download.
    """
    part = part_path(target)
    for attempt in range(1, retries + 2):
        try:
            _attempt_download(session, url, part, timeout, chunk_size)
            break
        except TransientError as exc:
            if attempt > retries:
                raise DownloadError(f"giving up after {attempt} attempts: {exc}") from exc
            delay = backoff_delay(attempt, backoff)
            log.warning("attempt %d failed (%s); retrying in %.1fs", attempt, exc, delay)
            sleep(delay)
    os.replace(part, target)
    return target.stat().st_size


# --------------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------------


def _is_current(record: FileRecord | None, zip_path: Path, out_dir: Path) -> bool:
    """True when the zip and its extracted files are on disk and match the manifest."""
    if record is None or not zip_path.exists():
        return False
    if zip_path.stat().st_size != record.size_bytes or sha256_file(zip_path) != record.sha256:
        return False
    return all((out_dir / name).exists() for name in record.extracted)


def download_competencia(
    competencia: str,
    files: Sequence[str],
    *,
    dest: Path = DEFAULT_DEST,
    base_url: str = DEFAULT_BASE_URL,
    session: requests.Session | None = None,
    retries: int = 5,
    backoff: float = 2.0,
    timeout: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Downloads, unzips and checksums the given files of a release; returns the manifest.

    Re-runs are idempotent: files whose zip still matches the manifest are skipped. The
    manifest is rewritten after every file, so an interrupted run keeps its progress.
    """
    competencia = parse_competencia(competencia)
    out_dir = competencia_dir(dest, competencia)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / MANIFEST_NAME
    records = records_from_manifest(load_manifest(manifest_path))
    session = session or requests.Session()

    for name in files:
        zip_path = out_dir / name
        if _is_current(records.get(name), zip_path, out_dir):
            log.info("skipping %s: already downloaded, checksum matches the manifest", name)
            continue
        url = build_url(base_url, competencia, name)
        started = time.monotonic()
        log.info("downloading %s", url)
        size = download_file(
            session,
            url,
            zip_path,
            retries=retries,
            backoff=backoff,
            timeout=timeout,
            sleep=sleep,
        )
        record = FileRecord(
            name=name,
            url=url,
            size_bytes=size,
            sha256=sha256_file(zip_path),
            downloaded_at=utc_now(),
            extracted=extract_zip(zip_path, out_dir),
        )
        records[name] = record
        write_manifest(
            manifest_path, build_manifest(competencia, base_url, records.values(), utc_now())
        )
        log.info(
            "downloaded %s: %d bytes in %.1fs, sha256=%s, extracted=%s",
            name,
            size,
            time.monotonic() - started,
            record.sha256,
            list(record.extracted),
        )

    manifest = build_manifest(competencia, base_url, records.values(), utc_now())
    write_manifest(manifest_path, manifest)
    log.info("manifest written: %s (%d files)", manifest_path, len(manifest["files"]))
    return manifest


def parse_args(argv: Sequence[str] | None, env: Mapping[str, str]) -> argparse.Namespace:
    """Parses and validates the command line (pure apart from argparse's own exit on error)."""
    parser = argparse.ArgumentParser(
        prog="python -m cnpj_lakehouse.download",
        description="RF-01: download one CNPJ release (reference month) with checksums.",
    )
    parser.add_argument(
        "--competencia",
        required=True,
        type=_competencia_arg,
        help="reference month of the release, YYYY-MM",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        # Safe default: the lookup tables are a few KB, the full release is ~7.8 GB zipped.
        default=["lookups"],
        help=(
            f"files or groups to download ({', '.join(FILE_GROUPS)}); "
            "default: lookups (pass 'all' for the whole release)"
        ),
    )
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST, help="default: data/raw")
    parser.add_argument(
        "--base-url",
        default=env.get(BASE_URL_ENV, "").strip() or DEFAULT_BASE_URL,
        help=f"folder with the YYYY-MM subfolders (env {BASE_URL_ENV})",
    )
    parser.add_argument("--retries", type=int, default=5, help="retries per file (default 5)")
    parser.add_argument("--timeout", type=float, default=60.0, help="seconds (default 60)")
    args = parser.parse_args(argv)
    try:
        args.files = resolve_files(args.files)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _competencia_arg(value: str) -> str:
    try:
        return parse_competencia(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; returns the process exit code."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv, os.environ)
    log.info(
        "reference month %s: %d file(s) from %s into %s",
        args.competencia,
        len(args.files),
        args.base_url,
        competencia_dir(args.dest, args.competencia),
    )
    try:
        download_competencia(
            args.competencia,
            args.files,
            dest=args.dest,
            base_url=args.base_url,
            retries=args.retries,
            timeout=args.timeout,
        )
    except DownloadError as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
