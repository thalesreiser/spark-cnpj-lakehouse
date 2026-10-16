"""Smoke test do ambiente (RNF-02): grava e lê uma tabela Delta no MinIO via s3a.

Executado dentro do container `spark-client` por `make smoke`, com o driver ligado ao
cluster (`spark://spark-master:7077`). Termina com código != 0 se algo falhar.
"""

from __future__ import annotations

import os
import platform
import sys
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Row

from cnpj_lakehouse.spark import ConfigLakehouse, get_spark

LINHAS = [
    Row(cnpj_basico="00000000", uf="DF", qtd=1),
    Row(cnpj_basico="11111111", uf="SP", qtd=2),
    Row(cnpj_basico="22222222", uf="RJ", qtd=3),
]


def _versao_python_executor(_: object) -> str:
    return platform.python_version()


def main() -> int:
    cfg = ConfigLakehouse.from_env(os.environ)
    spark = get_spark("smoke-delta-minio", cfg)
    try:
        sc = spark.sparkContext
        print(f"[smoke] Spark {spark.version} | master {sc.master}")
        print(f"[smoke] Python driver {platform.python_version()}")

        if not cfg.master.startswith("local"):
            versoes = sorted(
                set(sc.parallelize(range(4), 2).map(_versao_python_executor).collect())
            )
            print(f"[smoke] Python executores {versoes}")
            if versoes != [platform.python_version()]:
                print("[smoke] FALHA: versão do Python difere entre driver e executores")
                return 1

        caminho = cfg.caminho("_smoke", f"delta_{uuid.uuid4().hex[:8]}")
        df = spark.createDataFrame(LINHAS)
        df.write.format("delta").mode("overwrite").save(caminho)
        print(f"[smoke] gravado {df.count()} linhas em {caminho}")

        lido = spark.read.format("delta").load(caminho)
        esperado = sorted((r.cnpj_basico, r.uf, r.qtd) for r in LINHAS)
        obtido = sorted((r.cnpj_basico, r.uf, r.qtd) for r in lido.collect())
        versao = DeltaTable.forPath(spark, caminho).history(1).collect()[0]["version"]
        print(f"[smoke] lido {len(obtido)} linhas (versão Delta {versao})")
        if obtido != esperado:
            print(f"[smoke] FALHA: esperado {esperado}, obtido {obtido}")
            return 1
        if not DeltaTable.isDeltaTable(spark, caminho):
            print("[smoke] FALHA: caminho não é uma tabela Delta")
            return 1

        print("[smoke] OK: tabela Delta gravada e lida no MinIO via s3a")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
