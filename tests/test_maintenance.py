"""RF-07 maintenance tests (Delta): OPTIMIZE reduces the files, VACUUM keeps time travel.

Same Delta-enabled local session as `test_silver_merge.py` (marker ``delta``, own pytest run).
Tiny synthetic tables (``spark.range``), no CNPJ data.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test_silver_merge import pytestmark, spark

from cnpj_lakehouse import maintenance as mt
from cnpj_lakehouse.fingerprint import table_fingerprint

__all__ = ["pytestmark", "spark"]  # module-scoped Delta session and markers, reused


def small_files(spark: SparkSession, path: str, files: int = 5, partitioned: bool = False) -> None:
    df = spark.range(200).withColumn("p", (F.col("id") % 2).cast("int")).repartition(files)
    writer = df.write.format("delta").mode("overwrite")
    if partitioned:
        writer = writer.partitionBy("p")
    writer.save(path)


def test_optimize_compacts_and_keeps_the_content(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "t").as_posix()
    small_files(spark, path)
    before = table_fingerprint(spark.read.format("delta").load(path))
    result = mt.optimize(spark, path)
    assert result["operation"] == "compaction"
    assert (result["before"]["num_files"], result["after"]["num_files"]) == (5, 1)
    assert (result["files_removed"], result["files_added"]) == (5, 1)
    assert result["bytes_removed"] > 0 and result["bytes_added"] > 0
    assert table_fingerprint(spark.read.format("delta").load(path)) == before
    # the old files stay on storage for time travel until VACUUM (ADR 013)
    assert mt.storage_files(spark, path)["files"] == 6
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 200
    # a second run finds nothing to compact
    assert mt.optimize(spark, path)["files_removed"] == 0


def test_optimize_partition_filter_and_zorder(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "p").as_posix()
    small_files(spark, path, files=4, partitioned=True)
    files = mt.table_files(spark, path)
    assert set(files["partitions"]) == {"p=0", "p=1"}
    assert files["files_per_partition_max"] == 4
    result = mt.optimize(spark, path, where="p = 0")
    assert result["after"]["partitions"] == {"p=0": 1, "p=1": 4}
    assert result["partitions_optimized"] == 1
    zorder = mt.optimize(spark, path, zorder=["id"])
    assert zorder["operation"] == "ZORDER BY (id)"
    assert zorder["after"]["partitions"] == {"p=0": 1, "p=1": 1}
    assert spark.read.format("delta").load(path).count() == 200


def test_vacuum_respects_the_retention(spark: SparkSession, tmp_path: Path) -> None:
    path = (tmp_path / "v").as_posix()
    small_files(spark, path)
    small_files(spark, path, files=3)  # version 1 replaces every file of version 0
    with pytest.raises(ValueError, match="ADR 013"):
        mt.vacuum(spark, path, retention_hours=0)
    result = mt.vacuum(spark, path)
    assert result["retention_hours"] == mt.DEFAULT_RETENTION_HOURS == 840
    # every replaced file is younger than 35 days: nothing deleted, version 0 still readable
    assert result["files_deleted"] == 0 and result["storage_after"]["files"] == 8
    assert result["live_files"] == 3
    assert spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 200


def test_cli_optimize_then_vacuum(spark: SparkSession, tmp_path: Path) -> None:
    base = (tmp_path / "silver").as_posix()
    small_files(spark, f"{base}/empresas")
    summary = tmp_path / "optimize.json"
    argv = ["optimize", "--base", base, "--tables", "empresas", "--summary", str(summary)]
    assert mt.main(argv, spark=spark) == 0
    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["command"] == "optimize" and payload["layer"] == "silver"
    assert payload["tables"][0]["table"] == "empresas"
    assert payload["tables"][0]["after"]["num_files"] == 1
    assert mt.main(["vacuum", "--base", base, "--tables", "empresas"], spark=spark) == 0
    with pytest.raises(ValueError):
        mt.main(["vacuum", "--base", base, "--tables", "empresas", "--retention-hours", "1"], spark)
