"""RNF-02 / RNF-05: criação da SparkSession com Delta Lake e s3a apontando para o MinIO.

A configuração é montada por funções puras (`ConfigLakehouse.from_env` e `montar_conf`),
testáveis sem Docker; `get_spark` apenas aplica essa configuração ao builder do Spark.

Variáveis de ambiente (padrões pensados para desenvolvimento local, nunca para produção):

| Variável                      | Padrão                   | Uso                                  |
|-------------------------------|--------------------------|--------------------------------------|
| `SPARK_MASTER_URL`            | `local[*]`               | master do Spark                      |
| `S3_ENDPOINT`                 | `http://localhost:9000`  | endpoint S3 (MinIO)                  |
| `S3_ACCESS_KEY`               | `MINIO_ROOT_USER`        | credencial (dev) do MinIO            |
| `S3_SECRET_KEY`               | `MINIO_ROOT_PASSWORD`    | credencial (dev) do MinIO            |
| `LAKEHOUSE_BUCKET`            | `lakehouse`              | bucket das camadas bronze/silver/gold|
| `SPARK_EXECUTOR_MEMORY`       | `1g`                     | memória de cada executor             |
| `SPARK_DRIVER_HOST`           | (não definido)           | host/IP do driver visto pelo cluster |
| `LAKEHOUSE_JARS_PREINSTALADOS`| `false`                  | `true` na imagem Spark do projeto    |
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import SparkSession

from cnpj_lakehouse.versoes import PACOTES_SPARK

# Credenciais padrão do MinIO para desenvolvimento local (iguais às do docker-compose).
USUARIO_DEV_PADRAO = "minioadmin"
SENHA_DEV_PADRAO = "minioadmin"

_VERDADEIROS = {"1", "true", "sim", "yes", "on"}


def _como_bool(valor: str | None, padrao: bool = False) -> bool:
    if valor is None or valor.strip() == "":
        return padrao
    return valor.strip().lower() in _VERDADEIROS


@dataclass(frozen=True)
class ConfigLakehouse:
    """Parâmetros de conexão do Spark com o cluster e com o armazenamento S3 (MinIO)."""

    master: str = "local[*]"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = USUARIO_DEV_PADRAO
    s3_secret_key: str = SENHA_DEV_PADRAO
    bucket: str = "lakehouse"
    executor_memory: str = "1g"
    driver_host: str | None = None
    jars_preinstalados: bool = False

    @property
    def ssl(self) -> bool:
        return self.s3_endpoint.lower().startswith("https://")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ConfigLakehouse:
        """Lê a configuração de um mapeamento de variáveis de ambiente (ex.: `os.environ`)."""

        def ler(nome: str, padrao: str) -> str:
            valor = env.get(nome, "").strip()
            return valor or padrao

        return cls(
            master=ler("SPARK_MASTER_URL", cls.master),
            s3_endpoint=ler("S3_ENDPOINT", cls.s3_endpoint),
            s3_access_key=ler("S3_ACCESS_KEY", ler("MINIO_ROOT_USER", USUARIO_DEV_PADRAO)),
            s3_secret_key=ler("S3_SECRET_KEY", ler("MINIO_ROOT_PASSWORD", SENHA_DEV_PADRAO)),
            bucket=ler("LAKEHOUSE_BUCKET", cls.bucket),
            executor_memory=ler("SPARK_EXECUTOR_MEMORY", cls.executor_memory),
            driver_host=env.get("SPARK_DRIVER_HOST", "").strip() or None,
            jars_preinstalados=_como_bool(env.get("LAKEHOUSE_JARS_PREINSTALADOS")),
        )

    def caminho(self, *partes: str) -> str:
        """Monta um caminho `s3a://<bucket>/<partes>` dentro do lakehouse."""
        sufixo = "/".join(p.strip("/") for p in partes if p.strip("/"))
        base = f"s3a://{self.bucket}"
        return f"{base}/{sufixo}" if sufixo else base


def montar_conf(cfg: ConfigLakehouse) -> dict[str, str]:
    """Gera as propriedades do Spark para Delta Lake + s3a (função pura)."""
    conf: dict[str, str] = {
        # Delta Lake
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # s3a -> MinIO
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": cfg.s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": cfg.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": cfg.s3_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(cfg.ssl).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        # Execução
        "spark.sql.adaptive.enabled": "true",
        "spark.executor.memory": cfg.executor_memory,
    }
    if cfg.driver_host:
        conf["spark.driver.host"] = cfg.driver_host
    if not cfg.jars_preinstalados:
        conf["spark.jars.packages"] = ",".join(PACOTES_SPARK)
    return conf


def get_spark(app_name: str, cfg: ConfigLakehouse | None = None) -> SparkSession:
    """Cria (ou reaproveita) a SparkSession configurada para o lakehouse no MinIO."""
    cfg = cfg or ConfigLakehouse.from_env(os.environ)
    builder = SparkSession.builder.appName(app_name).master(cfg.master)
    for chave, valor in montar_conf(cfg).items():
        builder = builder.config(chave, valor)
    return builder.getOrCreate()
