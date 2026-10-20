"""SparkSession configuration tests (no Docker and no JVM started)."""

from __future__ import annotations

from typing import Any

import pytest

from cnpj_lakehouse import spark as spark_module
from cnpj_lakehouse.spark import LakehouseConfig, build_conf, get_spark
from cnpj_lakehouse.versions import SPARK_PACKAGES


def test_from_env_empty_uses_development_defaults() -> None:
    cfg = LakehouseConfig.from_env({})
    assert cfg == LakehouseConfig()
    assert cfg.master == "local[*]"
    assert cfg.s3_endpoint == "http://localhost:9000"
    assert cfg.bucket == "lakehouse"
    assert cfg.jars_preinstalled is False
    assert cfg.driver_host is None


def test_from_env_reads_variables() -> None:
    cfg = LakehouseConfig.from_env(
        {
            "SPARK_MASTER_URL": "spark://spark-master:7077",
            "S3_ENDPOINT": "http://minio:9000",
            "S3_ACCESS_KEY": "user",
            "S3_SECRET_KEY": "dev-password",
            "LAKEHOUSE_BUCKET": "other",
            "SPARK_EXECUTOR_MEMORY": "2g",
            "SPARK_DRIVER_HOST": "172.18.0.5",
            "LAKEHOUSE_JARS_PREINSTALLED": "true",
        }
    )
    assert cfg.master == "spark://spark-master:7077"
    assert cfg.s3_endpoint == "http://minio:9000"
    assert (cfg.s3_access_key, cfg.s3_secret_key) == ("user", "dev-password")
    assert cfg.bucket == "other"
    assert cfg.executor_memory == "2g"
    assert cfg.driver_host == "172.18.0.5"
    assert cfg.jars_preinstalled is True


def test_from_env_falls_back_to_minio_credentials() -> None:
    cfg = LakehouseConfig.from_env({"MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p", "X": ""})
    assert (cfg.s3_access_key, cfg.s3_secret_key) == ("u", "p")


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1", True), ("TRUE", True), (" yes ", True), ("false", False), ("", False), ("0", False)],
)
def test_jars_preinstalled_parses_booleans(value: str, expected: bool) -> None:
    cfg = LakehouseConfig.from_env({"LAKEHOUSE_JARS_PREINSTALLED": value})
    assert cfg.jars_preinstalled is expected


def test_path_builds_s3a_uri() -> None:
    cfg = LakehouseConfig(bucket="lakehouse")
    assert cfg.path() == "s3a://lakehouse"
    assert cfg.path("bronze", "/empresas/") == "s3a://lakehouse/bronze/empresas"
    assert cfg.path("", "silver") == "s3a://lakehouse/silver"


def test_build_conf_sets_delta_and_s3a() -> None:
    cfg = LakehouseConfig(s3_endpoint="http://minio:9000", s3_access_key="a", s3_secret_key="b")
    conf = build_conf(cfg)
    assert conf["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert conf["spark.sql.catalog.spark_catalog"].endswith("DeltaCatalog")
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "a"
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "b"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.executor.memory"] == "1g"
    assert "spark.driver.host" not in conf


def test_build_conf_https_enables_ssl() -> None:
    conf = build_conf(LakehouseConfig(s3_endpoint="https://s3.example"))
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"


def test_build_conf_packages_only_when_jars_not_preinstalled() -> None:
    assert build_conf(LakehouseConfig())["spark.jars.packages"] == ",".join(SPARK_PACKAGES)
    assert "spark.jars.packages" not in build_conf(LakehouseConfig(jars_preinstalled=True))


def test_build_conf_driver_host() -> None:
    conf = build_conf(LakehouseConfig(driver_host="10.0.0.2"))
    assert conf["spark.driver.host"] == "10.0.0.2"


def test_event_log_only_when_dir_is_set() -> None:
    assert "spark.eventLog.enabled" not in build_conf(LakehouseConfig.from_env({}))
    cfg = LakehouseConfig.from_env({"SPARK_EVENT_LOG_DIR": " file:/tmp/events "})
    conf = build_conf(cfg)
    assert conf["spark.eventLog.enabled"] == "true"
    assert conf["spark.eventLog.dir"] == "file:/tmp/events"
    assert conf["spark.eventLog.compress"] == "false"


class _FakeBuilder:
    def __init__(self) -> None:
        self.calls: dict[str, Any] = {"config": {}}

    def appName(self, name: str) -> _FakeBuilder:  # noqa: N802 (PySpark API)
        self.calls["app"] = name
        return self

    def master(self, master: str) -> _FakeBuilder:
        self.calls["master"] = master
        return self

    def config(self, key: str, value: str) -> _FakeBuilder:
        self.calls["config"][key] = value
        return self

    def getOrCreate(self) -> str:  # noqa: N802 (PySpark API)
        return "session"


def test_get_spark_applies_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = _FakeBuilder()

    class _FakeSparkSession:
        pass

    _FakeSparkSession.builder = builder  # type: ignore[attr-defined]
    monkeypatch.setattr(spark_module, "SparkSession", _FakeSparkSession)
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://m:7077")
    monkeypatch.setenv("LAKEHOUSE_JARS_PREINSTALLED", "true")

    assert get_spark("test") == "session"  # type: ignore[comparison-overlap]
    assert builder.calls["app"] == "test"
    assert builder.calls["master"] == "spark://m:7077"
    assert builder.calls["config"] == build_conf(
        LakehouseConfig(master="spark://m:7077", jars_preinstalled=True)
    )
