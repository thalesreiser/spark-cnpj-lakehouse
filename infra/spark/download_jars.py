"""Downloads the Delta Lake and s3a jars into the Spark jars directory (image build).

Usage: python download_jars.py <jars_dir>

Versions come from `cnpj_lakehouse.versions` (single source of truth, RNF-05). Each jar is
checked against the SHA-1 published on Maven Central. The script also checks that the Hadoop
version bundled with PySpark is the expected one, so that `hadoop-aws` is compatible.
"""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

from cnpj_lakehouse.versions import HADOOP_VERSION, JARS


def _download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 (fixed URL)
        content: bytes = response.read()
    return content


def main(target: Path) -> None:
    bundled_hadoop = target / f"hadoop-client-api-{HADOOP_VERSION}.jar"
    if not bundled_hadoop.exists():
        raise SystemExit(
            f"Hadoop {HADOOP_VERSION} not found in {target}; "
            "update HADOOP_VERSION in cnpj_lakehouse/versions.py"
        )
    for jar in JARS:
        content = _download(jar.url)
        expected = _download(jar.url + ".sha1").decode().split()[0].strip()
        actual = hashlib.sha1(content).hexdigest()  # noqa: S324 (Maven checksum)
        if actual != expected:
            raise SystemExit(f"SHA-1 mismatch for {jar.jar_name}: {actual} != {expected}")
        (target / jar.jar_name).write_bytes(content)
        print(f"ok {jar.coordinate} ({len(content) // 1024} KiB)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python download_jars.py <jars_dir>")
    main(Path(sys.argv[1]))
