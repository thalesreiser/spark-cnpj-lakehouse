"""RF-01 monthly check: prints the most recent reference month published on the source.

Lists the WebDAV folder used by the downloader (``PROPFIND`` with ``Depth: 1``), takes the
``YYYY-MM/`` subfolders from the newest down and prints the first one that already holds every
required file, so a month whose upload is still in progress is skipped. Only file names are
read; nothing is downloaded.

Usage::

    python scripts/latest_competencia.py [--base-url URL]
        [--files lookups Empresas1 Estabelecimentos1]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET  # noqa: N817 (stdlib convention)
from collections.abc import Callable, Iterable, Sequence
from urllib.parse import unquote

import requests

from cnpj_lakehouse.download import BASE_URL_ENV, DEFAULT_BASE_URL, resolve_files

_MONTH_RE = re.compile(r"/(\d{4}-(?:0[1-9]|1[0-2]))/?$")
_PROPFIND_BODY = (
    '<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/></d:prop>'
    "</d:propfind>"
)

Lister = Callable[[str], list[str]]


def parse_hrefs(xml_text: str) -> list[str]:
    """``href`` values (URL-decoded) of a WebDAV multistatus document."""
    root = ET.fromstring(xml_text)  # noqa: S314 (expat does not resolve external entities)
    return [unquote(el.text or "") for el in root.iter("{DAV:}href")]


def months_desc(hrefs: Iterable[str]) -> list[str]:
    """Distinct ``YYYY-MM`` folder names among the hrefs, newest first."""
    return sorted({m.group(1) for h in hrefs if (m := _MONTH_RE.search(h))}, reverse=True)


def file_names(hrefs: Iterable[str]) -> set[str]:
    """Last path segment of every href that is not a folder."""
    return {h.rsplit("/", 1)[-1] for h in hrefs if h and not h.endswith("/")}


def latest_competencia(base_url: str, required: Sequence[str], list_hrefs: Lister) -> str:
    """Newest month under ``base_url`` whose folder holds every file in ``required``."""
    base = base_url.rstrip("/")
    months = months_desc(list_hrefs(base + "/"))
    for month in months:
        missing = set(required) - file_names(list_hrefs(f"{base}/{month}/"))
        if not missing:
            return month
        print(f"skipping {month}: missing {sorted(missing)}", file=sys.stderr)
    raise LookupError(f"no published month under {base} holds {sorted(required)}")


def propfind_lister(session: requests.Session, timeout: float = 60.0) -> Lister:
    """Lists a WebDAV folder (``Depth: 1``) and returns its hrefs."""

    def list_hrefs(url: str) -> list[str]:
        response = session.request(
            "PROPFIND",
            url,
            data=_PROPFIND_BODY,
            headers={"Depth": "1", "Content-Type": "application/xml"},
            timeout=timeout,
        )
        response.raise_for_status()
        return parse_hrefs(response.text)

    return list_hrefs


def main(argv: Sequence[str] | None = None, lister: Lister | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prints the latest published reference month.")
    parser.add_argument(
        "--base-url", default=os.environ.get(BASE_URL_ENV, "").strip() or DEFAULT_BASE_URL
    )
    parser.add_argument("--files", nargs="+", default=["lookups", "Empresas1", "Estabelecimentos1"])
    args = parser.parse_args(argv)
    try:
        required = resolve_files(args.files)
        month = latest_competencia(
            args.base_url, required, lister or propfind_lister(requests.Session())
        )
    except (ValueError, LookupError, requests.RequestException, ET.ParseError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(month)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
