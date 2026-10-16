"""RNF-05: as versões fixadas são consistentes entre pyproject, Dockerfile e `versoes.py`."""

from __future__ import annotations

import re
from pathlib import Path

from cnpj_lakehouse import versoes

RAIZ = Path(__file__).resolve().parents[1]


def _pin(nome: str) -> str:
    texto = (RAIZ / "pyproject.toml").read_text(encoding="utf-8")
    achado = re.search(rf'"{re.escape(nome)}==([^"]+)"', texto)
    assert achado, f"{nome} deve estar fixado com == no pyproject.toml"
    return achado.group(1)


def test_pyproject_fixa_pyspark_e_delta_iguais_ao_modulo() -> None:
    assert _pin("pyspark") == versoes.SPARK_VERSION
    assert _pin("delta-spark") == versoes.DELTA_VERSION


def test_pyproject_fixa_python() -> None:
    texto = (RAIZ / "pyproject.toml").read_text(encoding="utf-8")
    assert f'python_version = "{versoes.PYTHON_VERSION}"' in texto
    assert f'requires-python = ">={versoes.PYTHON_VERSION},' in texto


def test_dockerfile_usa_mesma_versao_do_python() -> None:
    texto = (RAIZ / "infra" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    achado = re.search(r"ARG PYTHON_IMAGE=python:(\d+\.\d+)\.\d+-", texto)
    assert achado, "a imagem base do Python deve ter tag exata (ex.: 3.11.17-slim-bookworm)"
    assert achado.group(1) == versoes.PYTHON_VERSION
    assert f"python{versoes.PYTHON_VERSION}/site-packages/pyspark" in texto


def test_compose_fixa_tags_exatas() -> None:
    texto = (RAIZ / "infra" / "docker-compose.yml").read_text(encoding="utf-8")
    assert ":latest" not in texto
    assert f"cnpj-lakehouse/spark:{versoes.SPARK_VERSION}" in texto
    assert re.search(r"minio:RELEASE\.\d{4}-\d{2}-\d{2}T", texto)
    assert re.search(r"mc:RELEASE\.\d{4}-\d{2}-\d{2}T", texto)


def test_jars_tem_coordenadas_e_urls_maven() -> None:
    delta = versoes.JARS[0]
    assert delta.coordenada == f"io.delta:delta-spark_2.12:{versoes.DELTA_VERSION}"
    assert delta.nome_jar == f"delta-spark_2.12-{versoes.DELTA_VERSION}.jar"
    assert delta.url == (
        f"{versoes.MAVEN_CENTRAL}/io/delta/delta-spark_2.12/{versoes.DELTA_VERSION}/"
        f"delta-spark_2.12-{versoes.DELTA_VERSION}.jar"
    )
    assert f"org.apache.hadoop:hadoop-aws:{versoes.HADOOP_VERSION}" in versoes.PACOTES_SPARK
