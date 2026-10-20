"""RNF-02 / RNF-05: SparkSession with Delta Lake and s3a pointing to MinIO.

The configuration is built by pure functions (`LakehouseConfig.from_env` and `build_conf`),
testable without Docker; `get_spark` only applies that configuration to the Spark builder.

Environment variables (defaults meant for local development, never for production):

| Variable                      | Default                 | Purpose                               |
|-------------------------------|-------------------------|---------------------------------------|
| `SPARK_MASTER_URL`            | `local[*]`              | Spark master                          |
| `S3_ENDPOINT`                 | `http://localhost:9000` | S3 endpoint (MinIO)                   |
| `S3_ACCESS_KEY`               | `MINIO_ROOT_USER`       | MinIO (dev) credential                |
| `S3_SECRET_KEY`               | `MINIO_ROOT_PASSWORD`   | MinIO (dev) credential                |
| `LAKEHOUSE_BUCKET`            | `lakehouse`             | bucket for bronze/silver/gold         |
| `SPARK_EXECUTOR_MEMORY`       | `1g`                    | memory of each executor               |
| `SPARK_DRIVER_HOST`           | (unset)                 | driver host/IP as seen by the cluster |
| `LAKEHOUSE_JARS_PREINSTALLED` | `false`                 | `true` inside the project Spark image |
| `SPARK_EVENT_LOG_DIR`         | (unset)                 | enables Spark event logs (benchmark)  |
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import SparkSession

from cnpj_lakehouse.versions import SPARK_PACKAGES

# Default MinIO credentials for local development (same as in docker-compose).
DEFAULT_DEV_USER = "minioadmin"
DEFAULT_DEV_PASSWORD = "minioadmin"

_TRUTHY = {"1", "true", "yes", "on"}


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in _TRUTHY


@dataclass(frozen=True)
class LakehouseConfig:
    """Connection settings between Spark, the cluster and the S3 storage (MinIO)."""

    master: str = "local[*]"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = DEFAULT_DEV_USER
    s3_secret_key: str = DEFAULT_DEV_PASSWORD
    bucket: str = "lakehouse"
    executor_memory: str = "1g"
    driver_host: str | None = None
    jars_preinstalled: bool = False
    # Directory (URI, e.g. ``file:/opt/app/build/benchmark/eventlogs``) for Spark event logs; set
    # only by the benchmark (task 6.1), so the Spark UI can be rebuilt by a History Server.
    event_log_dir: str | None = None

    @property
    def ssl(self) -> bool:
        return self.s3_endpoint.lower().startswith("https://")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> LakehouseConfig:
        """Reads the configuration from a mapping of environment variables (e.g. `os.environ`)."""

        def read(name: str, default: str) -> str:
            value = env.get(name, "").strip()
            return value or default

        return cls(
            master=read("SPARK_MASTER_URL", cls.master),
            s3_endpoint=read("S3_ENDPOINT", cls.s3_endpoint),
            s3_access_key=read("S3_ACCESS_KEY", read("MINIO_ROOT_USER", DEFAULT_DEV_USER)),
            s3_secret_key=read("S3_SECRET_KEY", read("MINIO_ROOT_PASSWORD", DEFAULT_DEV_PASSWORD)),
            bucket=read("LAKEHOUSE_BUCKET", cls.bucket),
            executor_memory=read("SPARK_EXECUTOR_MEMORY", cls.executor_memory),
            driver_host=env.get("SPARK_DRIVER_HOST", "").strip() or None,
            jars_preinstalled=_as_bool(env.get("LAKEHOUSE_JARS_PREINSTALLED")),
            event_log_dir=env.get("SPARK_EVENT_LOG_DIR", "").strip() or None,
        )

    def path(self, *parts: str) -> str:
        """Builds an `s3a://<bucket>/<parts>` path inside the lakehouse."""
        suffix = "/".join(p.strip("/") for p in parts if p.strip("/"))
        base = f"s3a://{self.bucket}"
        return f"{base}/{suffix}" if suffix else base


def build_conf(cfg: LakehouseConfig) -> dict[str, str]:
    """Builds the Spark properties for Delta Lake + s3a (pure function)."""
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
        # Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.executor.memory": cfg.executor_memory,
    }
    if cfg.driver_host:
        conf["spark.driver.host"] = cfg.driver_host
    if cfg.event_log_dir:
        conf["spark.eventLog.enabled"] = "true"
        conf["spark.eventLog.dir"] = cfg.event_log_dir
        conf["spark.eventLog.compress"] = "false"
    if not cfg.jars_preinstalled:
        conf["spark.jars.packages"] = ",".join(SPARK_PACKAGES)
    return conf


def get_spark(app_name: str, cfg: LakehouseConfig | None = None) -> SparkSession:
    """Creates (or reuses) the SparkSession configured for the lakehouse on MinIO."""
    cfg = cfg or LakehouseConfig.from_env(os.environ)
    builder = SparkSession.builder.appName(app_name).master(cfg.master)
    for key, value in build_conf(cfg).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
