"""Environment smoke test (RNF-02): writes and reads a Delta table on MinIO via s3a.

Run inside the `spark-client` container by `make smoke`, with the driver attached to the
cluster (`spark://spark-master:7077`). Exits with a non-zero code if anything fails.
"""

from __future__ import annotations

import os
import platform
import sys
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Row

from cnpj_lakehouse.spark import LakehouseConfig, get_spark

ROWS = [
    Row(cnpj_basico="00000000", uf="DF", qtd=1),
    Row(cnpj_basico="11111111", uf="SP", qtd=2),
    Row(cnpj_basico="22222222", uf="RJ", qtd=3),
]


def _executor_python_version(_: object) -> str:
    return platform.python_version()


def main() -> int:
    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("smoke-delta-minio", cfg)
    try:
        sc = spark.sparkContext
        print(f"[smoke] Spark {spark.version} | master {sc.master}")
        print(f"[smoke] driver Python {platform.python_version()}")

        if not cfg.master.startswith("local"):
            versions = sorted(
                set(sc.parallelize(range(4), 2).map(_executor_python_version).collect())
            )
            print(f"[smoke] executor Python {versions}")
            if versions != [platform.python_version()]:
                print("[smoke] FAILED: Python version differs between driver and executors")
                return 1

        path = cfg.path("_smoke", f"delta_{uuid.uuid4().hex[:8]}")
        df = spark.createDataFrame(ROWS)
        df.write.format("delta").mode("overwrite").save(path)
        print(f"[smoke] wrote {df.count()} rows to {path}")

        read_back = spark.read.format("delta").load(path)
        expected = sorted((r.cnpj_basico, r.uf, r.qtd) for r in ROWS)
        actual = sorted((r.cnpj_basico, r.uf, r.qtd) for r in read_back.collect())
        version = DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]
        print(f"[smoke] read {len(actual)} rows (Delta version {version})")
        if actual != expected:
            print(f"[smoke] FAILED: expected {expected}, got {actual}")
            return 1
        if not DeltaTable.isDeltaTable(spark, path):
            print("[smoke] FAILED: path is not a Delta table")
            return 1

        print("[smoke] OK: Delta table written to and read from MinIO via s3a")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
