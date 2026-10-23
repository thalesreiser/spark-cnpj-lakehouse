# 002 - Partitioning by reference month and state (UF)

- **Status:** accepted (bronze by reference month; no partitioning by state (UF) in silver or gold, decided on 2026-10-22, see the note below)
- **Date:** 2026-10-17

## Context

The Receita Federal publishes the whole CNPJ register every month (a *competencia*, `YYYY-MM`): each release is a full snapshot, not a delta. The pipeline must keep several releases side by side (history, time travel in epic 4) and re-ingest one month without touching the others (idempotent re-runs, RF-02).

Partitioning by state (UF) was also planned, because most business questions filter or group by UF. But the UF only exists in `estabelecimentos` (column `uf`); `empresas`, `socios`, `simples` and the lookup tables have no state, so a bronze-wide UF partition is impossible without joins, which bronze must not do (it keeps the source as received, ADR 008).

## Decision

1. Every bronze table (`s3a://<bucket>/bronze/<table>/`) is partitioned by a **`competencia`** string column (`YYYY-MM`), filled by the ingestion from the release being loaded (`cnpj_lakehouse.bronze.add_metadata`).
2. Re-running a month **replaces its partition**: Delta `mode("overwrite")` with `replaceWhere "competencia = '<YYYY-MM>'"` (`cnpj_lakehouse.bronze.write_bronze`). Other months are untouched, rows are never duplicated, and Delta rejects any row of another month written by mistake.
3. No UF partition in bronze. Partitioning silver/gold by UF (e.g. `estabelecimentos` and the gold aggregations) will be decided and measured in epic 6 (optimization and benchmark), together with the target file size (ADR 004).

## Alternatives considered

- **`competencia` + `uf` in bronze:** not available for most tables; would need joins in bronze.
- **No partitioning, `competencia` as a plain column:** re-running a month would need a `DELETE` + append or a `MERGE`, and every query on one month would scan all of them.
- **Dynamic partition overwrite (`partitionOverwriteMode=dynamic`):** also idempotent, but it silently accepts rows of any month; `replaceWhere` states the replaced partition explicitly. Dynamic overwrite is still used for Parquet in the unit tests, which run without the Delta jars.
- **Date-typed partition (`2026-09-01`):** the domain name and format of the source is `YYYY-MM`; a string keeps the folder names readable (`competencia=2026-09`).

## Consequences

- One folder per month per table; a month can be reloaded, compared with another (row counts per partition) or dropped on its own.
- The bronze tables are not partitioned by UF: a query on one state reads the whole month. This is acceptable for bronze, which is read once per month by silver.
- With a single partition per month, the number and size of files per partition depends on the input splits; compaction (`OPTIMIZE`) and file size are tuned in epic 6.

## Note (2026-10-22, task 6.2): no UF partitioning in silver or gold

Decided while tuning the main job ([benchmark](../benchmark.md)):

- **Silver** holds the current state of each business key and is loaded by a `MERGE` on that key ([ADR 011](011-silver-current-state-merge.md)). The key does not contain the UF and an establishment can move to another state, so a UF partition would not prune the `MERGE` (Delta prunes only on conditions over the partition column) and an update could move a row between partitions. It would also split each table into 27 partitions: silver `estabelecimentos` of the benchmark (167 MiB) would become 27 partitions of about 6 MiB, far below the target file size ([ADR 004](004-target-file-size.md)). Silver stays unpartitioned; reads filtered by UF rely on the Delta file statistics (data skipping), which `OPTIMIZE ... ZORDER BY (uf)` can sharpen (task 6.3).
- **Gold** tables are a few KB to about 1 MiB in total; one partition per UF would only create 27 tiny files per table.
- **Bronze** keeps the partition by `competencia` (decisions 1-3 above, unchanged).

## Note (2026-10-23, task 7.2): Z-order not adopted

The note above mentions `OPTIMIZE ... ZORDER BY (uf)` as a way to sharpen data skipping in silver. Task 6.3 measured it and [ADR 013](013-table-maintenance-and-vacuum-retention.md) did not adopt it (no files skipped at this volume, full rewrite after every monthly `MERGE`); silver stays unpartitioned and compacted only. The bronze compaction and file size mentioned in the consequences were done in task 6.3 ([ADR 004](004-target-file-size.md)).
