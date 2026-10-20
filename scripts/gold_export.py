"""Task 5.3: exports small roll-ups of the gold tables as CSV for the business questions notebook.

Run inside the `spark-client` container after `make gold` (manual `Gold analysis` workflow).
It reads the three gold tables (RF-05, ADR 012) from ``s3a://<bucket>/gold/`` and writes, under
``--out``, only aggregates (counts by year/UF, CNAE, size, status x sector) and metadata, never
rows, names or CNPJs (RST-01/LGPD):

* ``q1_ano_uf.csv``: establishments opened per year and UF (all CNAEs);
* ``q1_cnae.csv``: the ``--top`` main CNAEs by establishments opened in the last five full
  years before the reference month (with their all-time total);
* ``q2_porte.csv``: ``gold_distribuicao_porte`` as is (a handful of rows);
* ``q2_uf.csv``: establishments and active establishments per UF (all years);
* ``q3_situacao_setor.csv``: ``gold_situacao_setor`` as is (status x CNAE section/division);
* ``export.json``: reference month, row counts of each gold table and of each file.

    python scripts/gold_export.py --competencia 2026-09 --out build/gold-export
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cnpj_lakehouse.gold import CNAE_UF_ANO, DISTRIBUICAO_PORTE, GOLD_LAYER, SITUACAO_SETOR
from cnpj_lakehouse.spark import LakehouseConfig, get_spark


def write_csv(df: DataFrame, path: Path) -> int:
    """Collects a small aggregate and writes it as CSV; returns the number of rows."""
    rows = df.collect()
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(df.columns)
        writer.writerows([list(r) for r in rows])
    return len(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competencia", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--top", type=int, default=30, help="main CNAEs kept in q1_cnae.csv")
    args = parser.parse_args(argv)
    year = int(args.competencia[:4])
    recent = (year - 5, year - 1)

    cfg = LakehouseConfig.from_env(os.environ)
    spark = get_spark("gold-export", cfg)
    gold = {
        name: spark.read.format("delta").load(cfg.path(GOLD_LAYER, name))
        for name in (CNAE_UF_ANO, DISTRIBUICAO_PORTE, SITUACAO_SETOR)
    }
    cnae_uf_ano = gold[CNAE_UF_ANO]
    est = F.sum("estabelecimentos").alias("estabelecimentos")
    ativos = F.sum("estabelecimentos_ativos").alias("estabelecimentos_ativos")
    in_recent = F.col("ano_inicio_atividade").between(*recent)
    exports: dict[str, DataFrame] = {
        "q1_ano_uf.csv": cnae_uf_ano.groupBy("ano_inicio_atividade", "uf")
        .agg(est, ativos)
        .orderBy("ano_inicio_atividade", "uf"),
        "q1_cnae.csv": cnae_uf_ano.groupBy("cnae_fiscal_principal", "cnae_descricao")
        .agg(
            est,
            F.sum(F.when(in_recent, F.col("estabelecimentos")).otherwise(0)).alias(
                f"estabelecimentos_{recent[0]}_{recent[1]}"
            ),
        )
        .orderBy(F.desc(f"estabelecimentos_{recent[0]}_{recent[1]}"), "cnae_fiscal_principal")
        .limit(args.top),
        "q2_porte.csv": gold[DISTRIBUICAO_PORTE].orderBy("porte_empresa"),
        "q2_uf.csv": cnae_uf_ano.groupBy("uf").agg(est, ativos).orderBy("uf"),
        "q3_situacao_setor.csv": gold[SITUACAO_SETOR].orderBy(
            "situacao_cadastral", "cnae_secao", "cnae_divisao"
        ),
    }
    args.out.mkdir(parents=True, exist_ok=True)
    files = {name: write_csv(df, args.out / name) for name, df in exports.items()}
    meta: dict[str, Any] = {
        "competencia": args.competencia,
        "gold_rows": {name: df.count() for name, df in gold.items()},
        "files": files,
        "recent_years": list(recent),
    }
    (args.out / "export.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(meta, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
