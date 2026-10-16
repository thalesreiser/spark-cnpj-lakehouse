"""RNF-05: pinned versions are consistent across pyproject, Dockerfile and `versions.py`."""

from __future__ import annotations

import re
from pathlib import Path

from cnpj_lakehouse import versions

ROOT = Path(__file__).resolve().parents[1]


def _pin(name: str) -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    found = re.search(rf'"{re.escape(name)}==([^"]+)"', text)
    assert found, f"{name} must be pinned with == in pyproject.toml"
    return found.group(1)


def test_pyproject_pins_pyspark_and_delta_like_the_module() -> None:
    assert _pin("pyspark") == versions.SPARK_VERSION
    assert _pin("delta-spark") == versions.DELTA_VERSION


def test_pyproject_pins_python() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'python_version = "{versions.PYTHON_VERSION}"' in text
    assert f'requires-python = ">={versions.PYTHON_VERSION},' in text


def test_dockerfile_uses_the_same_python_version() -> None:
    text = (ROOT / "infra" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    found = re.search(r"ARG PYTHON_IMAGE=python:(\d+\.\d+)\.\d+-", text)
    assert found, "the Python base image must have an exact tag (e.g. 3.11.17-slim-bookworm)"
    assert found.group(1) == versions.PYTHON_VERSION
    assert f"python{versions.PYTHON_VERSION}/site-packages/pyspark" in text


def test_compose_pins_exact_tags() -> None:
    text = (ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8")
    assert ":latest" not in text
    assert f"cnpj-lakehouse/spark:{versions.SPARK_VERSION}" in text
    assert re.search(r"minio:RELEASE\.\d{4}-\d{2}-\d{2}T", text)
    assert re.search(r"mc:RELEASE\.\d{4}-\d{2}-\d{2}T", text)


def test_jars_have_maven_coordinates_and_urls() -> None:
    delta = versions.JARS[0]
    assert delta.coordinate == f"io.delta:delta-spark_2.12:{versions.DELTA_VERSION}"
    assert delta.jar_name == f"delta-spark_2.12-{versions.DELTA_VERSION}.jar"
    assert delta.url == (
        f"{versions.MAVEN_CENTRAL}/io/delta/delta-spark_2.12/{versions.DELTA_VERSION}/"
        f"delta-spark_2.12-{versions.DELTA_VERSION}.jar"
    )
    assert f"org.apache.hadoop:hadoop-aws:{versions.HADOOP_VERSION}" in versions.SPARK_PACKAGES
