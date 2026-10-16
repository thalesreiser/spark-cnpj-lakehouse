"""Testes da configuração da SparkSession (sem Docker e sem iniciar a JVM)."""

from __future__ import annotations

from typing import Any

import pytest

from cnpj_lakehouse import spark as modulo_spark
from cnpj_lakehouse.spark import ConfigLakehouse, get_spark, montar_conf
from cnpj_lakehouse.versoes import PACOTES_SPARK


def test_from_env_vazio_usa_padroes_de_desenvolvimento() -> None:
    cfg = ConfigLakehouse.from_env({})
    assert cfg == ConfigLakehouse()
    assert cfg.master == "local[*]"
    assert cfg.s3_endpoint == "http://localhost:9000"
    assert cfg.bucket == "lakehouse"
    assert cfg.jars_preinstalados is False
    assert cfg.driver_host is None


def test_from_env_le_variaveis() -> None:
    cfg = ConfigLakehouse.from_env(
        {
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "S3_ENDPOINT": "http://minio:9000",
            "S3_ACCESS_KEY": "usuario",
            "S3_SECRET_KEY": "senha-dev",
            "LAKEHOUSE_BUCKET": "outro",
            "SPARK_EXECUTOR_MEMORY": "2g",
            "SPARK_DRIVER_HOST": "172.18.0.5",
            "LAKEHOUSE_JARS_PREINSTALADOS": "true",
        }
    )
    assert cfg.master == "spark://spark-master:7077"
    assert cfg.s3_endpoint == "http://minio:9000"
    assert (cfg.s3_access_key, cfg.s3_secret_key) == ("usuario", "senha-dev")
    assert cfg.bucket == "outro"
    assert cfg.executor_memory == "2g"
    assert cfg.driver_host == "172.18.0.5"
    assert cfg.jars_preinstalados is True


def test_from_env_cai_para_credenciais_do_minio() -> None:
    cfg = ConfigLakehouse.from_env({"MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p", "X": ""})
    assert (cfg.s3_access_key, cfg.s3_secret_key) == ("u", "p")


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [("1", True), ("TRUE", True), (" sim ", True), ("false", False), ("", False), ("0", False)],
)
def test_jars_preinstalados_interpreta_booleanos(valor: str, esperado: bool) -> None:
    cfg = ConfigLakehouse.from_env({"LAKEHOUSE_JARS_PREINSTALADOS": valor})
    assert cfg.jars_preinstalados is esperado


def test_caminho_monta_uri_s3a() -> None:
    cfg = ConfigLakehouse(bucket="lakehouse")
    assert cfg.caminho() == "s3a://lakehouse"
    assert cfg.caminho("bronze", "/empresas/") == "s3a://lakehouse/bronze/empresas"
    assert cfg.caminho("", "silver") == "s3a://lakehouse/silver"


def test_montar_conf_configura_delta_e_s3a() -> None:
    cfg = ConfigLakehouse(s3_endpoint="http://minio:9000", s3_access_key="a", s3_secret_key="b")
    conf = montar_conf(cfg)
    assert conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf["spark.sql.catalog.spark_catalog"].endswith("DeltaCatalog")
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "a"
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "b"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.executor.memory"] == "1g"
    assert "spark.driver.host" not in conf


def test_montar_conf_https_habilita_ssl() -> None:
    conf = montar_conf(ConfigLakehouse(s3_endpoint="https://s3.exemplo"))
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"


def test_montar_conf_pacotes_so_quando_jars_nao_preinstalados() -> None:
    assert montar_conf(ConfigLakehouse())["spark.jars.packages"] == ",".join(PACOTES_SPARK)
    assert "spark.jars.packages" not in montar_conf(ConfigLakehouse(jars_preinstalados=True))


def test_montar_conf_driver_host() -> None:
    conf = montar_conf(ConfigLakehouse(driver_host="10.0.0.2"))
    assert conf["spark.driver.host"] == "10.0.0.2"


class _BuilderFalso:
    def __init__(self) -> None:
        self.chamadas: dict[str, Any] = {"config": {}}

    def appName(self, nome: str) -> _BuilderFalso:  # noqa: N802 (API do PySpark)
        self.chamadas["app"] = nome
        return self

    def master(self, master: str) -> _BuilderFalso:
        self.chamadas["master"] = master
        return self

    def config(self, chave: str, valor: str) -> _BuilderFalso:
        self.chamadas["config"][chave] = valor
        return self

    def getOrCreate(self) -> str:  # noqa: N802 (API do PySpark)
        return "sessao"


def test_get_spark_aplica_configuracao(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = _BuilderFalso()

    class _SparkSessionFalsa:
        pass

    _SparkSessionFalsa.builder = builder  # type: ignore[attr-defined]
    monkeypatch.setattr(modulo_spark, "SparkSession", _SparkSessionFalsa)
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://m:7077")
    monkeypatch.setenv("LAKEHOUSE_JARS_PREINSTALADOS", "true")

    assert get_spark("teste") == "sessao"  # type: ignore[comparison-overlap]
    assert builder.chamadas["app"] == "teste"
    assert builder.chamadas["master"] == "spark://m:7077"
    assert builder.chamadas["config"] == montar_conf(
        ConfigLakehouse(master="spark://m:7077", jars_preinstalados=True)
    )
