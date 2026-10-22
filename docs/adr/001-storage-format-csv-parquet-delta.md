# 001 - Storage format: CSV vs Parquet vs Delta

- **Status:** accepted
- **Date:** 2026-10-22

## Context

The Receita Federal publishes the CNPJ register as zipped CSV files (`;`-separated, Latin-1, no header). The lakehouse keeps three layers (bronze, silver, gold) and needs, besides fast reads: replacing one month of bronze without touching the others ([ADR 002](002-partitioning-by-month-and-state.md)), a `MERGE` by business key in silver ([ADR 011](011-silver-current-state-merge.md)), history and time travel (RF-06), and atomic writes, since a failed job must not leave a half-written table that the next layer reads.

Small measurement (task 6.2, `scripts/format_compare.py`, `Benchmark` run [39787450949](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39787450949)): one month of bronze `estabelecimentos` (25% sample of `Estabelecimentos1`, 1,189,564 rows, every column a string, ADR 008) written to MinIO in each format by the project cluster (1 worker, 2 cores), then three queries, median of 3:

| Format | Write | Size | Files | Full count | Count by `uf` | Distinct companies with an active establishment |
|---|---:|---:|---:|---:|---:|---:|
| CSV (header, uncompressed) | 7.0 s | 189.9 MiB | 3 | 0.7 s | 1.2 s | 1.5 s |
| Parquet (snappy) | 5.7 s | 83.5 MiB | 3 | 0.2 s | 0.5 s | 0.5 s |
| Delta (Parquet + log) | 7.4 s | 83.6 MiB | 4 | 0.3 s | 0.5 s | 0.7 s |

## Decision

1. **Delta Lake for every layer** (bronze, silver, gold), written and read through `cnpj_lakehouse.spark` (Delta 3.x on Spark 3.5).
2. **CSV only as the source**: the downloaded files stay in `data/raw/` and are read once by bronze with an explicit all-string schema.

## Alternatives considered

- **CSV in the lake:** 2.3x the size of Parquet and 2–3x slower to query in the measurement (no compression, no column pruning: every query parses every column), no types, no atomic replace of a month.
- **Plain Parquet:** the same storage and query cost as Delta (Delta's data files are Parquet; its log adds 0.1 MiB and 0.1–0.2 s of log replay to a query and 1.7 s of commit and statistics to the write), but no `MERGE`, no `replaceWhere`, no time travel and no atomic commit: a failed overwrite leaves a mix of old and new files.
- **Apache Iceberg / Hudi:** similar table features, but Delta has the most direct support in open-source Spark 3.5 and is what the project targets.

## Consequences

- Silver and gold get `MERGE`, `RESTORE`, `VACUUM` and `versionAsOf` (RF-04, RF-06); the benchmark uses `RESTORE` to repeat the `MERGE` on the same table.
- Every read replays the Delta log; with many commits this cost grows and checkpoints (every 10 commits by default) and the snapshot partitions setting (`cnpj_lakehouse.spark.TUNING_CONF`) keep it small.
- The data files are Parquet, so they stay readable by any Parquet reader (without the transaction guarantees).
