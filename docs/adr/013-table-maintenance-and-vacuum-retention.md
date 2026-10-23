# 013 - Table maintenance: compaction, Z-order and VACUUM retention

- **Status:** accepted
- **Date:** 2026-10-22

## Context

Task 6.3 (RF-07) adds `OPTIMIZE` and `VACUUM` to the large bronze and silver tables (`empresas`, `estabelecimentos`). [ADR 004](004-target-file-size.md) fixed the target file size (~128 MiB, reached by compaction). Three things were still open:

- whether to Z-order any table, and by which columns;
- how long `VACUUM` keeps the files that older versions reference. Silver is the current state updated by a monthly `MERGE` ([ADR 011](011-silver-current-state-merge.md)) and time travel (`versionAsOf N-1`, RF-06, task 4.3) reads the previous load. A monthly `MERGE` of ~1–3% of the keys, spread over the whole table, rewrites almost every file, so each old version that is kept costs about one table of storage;
- Delta's defaults: `delta.deletedFileRetentionDuration` = 7 days (`VACUUM` refuses less unless `spark.databricks.delta.retentionDurationCheck.enabled` is turned off) and `delta.logRetentionDuration` = 30 days (log entries older than that are cleaned at checkpoints).

## Decision

1. **Compaction, no Z-order by default.** `OPTIMIZE` runs as compaction (`executeCompaction`, `maxFileSize` = `minFileSize` = 128 MiB, so a compacted table is not rewritten again). `ZORDER_BY` in `cnpj_lakehouse/maintenance.py` is empty; `--zorder` stays available. Z-order was measured on silver `estabelecimentos` by `uf` (see [benchmark](../benchmark.md#optimize-and-vacuum-task-63)) and is not adopted:
   - the main job (gold) reads the whole table, and the analytical filters by UF or CNAE are answered by the gold tables, which are already aggregated by those columns;
   - Delta Lake OSS Z-order is not incremental: it rewrites every file in scope each time, and after each monthly `MERGE` (which rewrites most files) the clustering would have to be rebuilt, i.e. one more full rewrite of the table per month;
   - measured (run 39864257683): before Z-order none of the 13 files could be skipped for `uf = 'SP'`, and Z-order collapsed the 2 compacted files into 1, so nothing was left to skip; the read workload went from 2.89 to 2.58 s, explained by the file count, not by skipping.
2. **`VACUUM` retention: 840 h (35 days) by default, never below 168 h.** 35 days is one monthly load cycle plus a margin: the files of the previous load stay readable with `versionAsOf` / `timestampAsOf` until the next load has been checked. `cnpj_lakehouse.maintenance.vacuum` raises below 168 h (Delta's default and safety floor: a shorter retention can delete files that a running job or a concurrent reader still needs), and Delta's retention check is never disabled in pipeline code. The only place that turns it off is the benchmark harness (`scripts/benchmark.py reset_silver`), which undoes its own `MERGE` on throwaway `benchmark/` tables between repetitions.
3. **Order:** monthly load → checks → `OPTIMIZE` (bronze: `--where "competencia = '<month>'"`, only the new partition; silver: the whole table) → `VACUUM`. `OPTIMIZE` commits a version with `dataChange = false`: same rows, and the earlier versions stay readable.

## Alternatives considered

- **Z-order silver `estabelecimentos` by `uf` / `cnae_fiscal_principal`:** better skipping for filtered reads on silver; rejected for the cost and the read pattern above. Revisit if silver gets direct filtered consumers (e.g. an API by UF) at full volume.
- **Partitioning by UF instead of Z-order:** rejected in [ADR 002](002-partitioning-by-month-and-state.md) (no pruning for the `MERGE`, 27 small partitions).
- **Retention 168 h (Delta's default):** a week after a load the previous month's files would be deleted, and `versionAsOf N-1` (the time travel demo) would fail for most of the table.
- **Longer retention (e.g. 13 months, one version per month):** a year of history by time travel, but ~13x the storage of silver; bronze already keeps every month as a partition, so older states can be rebuilt from it.
- **`VACUUM RETAIN 0 HOURS`:** minimal storage, no time travel, and unsafe with concurrent readers.

## Consequences

- Storage: silver keeps about two versions' worth of files (current + previous month); the benchmark records live vs on-storage files.
- `delta.logRetentionDuration` (30 days by default) is shorter than the 35-day file retention; log cleanup happens only at checkpoints (every 10 commits), which a monthly table with 2–3 commits per month reaches every few months, but a table that is reached later could lose `versionAsOf` of a version older than 30 days. Setting `delta.logRetentionDuration = 'interval 60 days'` on the silver tables is a follow-up, not done here.
- The maintenance commands are not yet part of a scheduled pipeline (`make optimize` / `make vacuum`, `python -m cnpj_lakehouse.maintenance`); orchestration is out of scope of epic 6.
