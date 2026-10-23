# 004 - Target file size

- **Status:** accepted
- **Date:** 2026-10-22

## Context

The number and size of the files of a Delta table drive both sides of its cost: many small files mean one task, one S3 request and one footer read per file (and a long transaction log), while a few very large files limit the read parallelism and make every rewrite of a `MERGE` heavier (Delta rewrites whole files). The usual target for Parquet under Delta is 128 MiB to 1 GiB per file.

The 6.1 baseline ([benchmark](../benchmark.md)) on the sampled release (25% of `Empresas1` and `Estabelecimentos1`) found silver `empresas` in 5 files of 20.4 MiB and silver `estabelecimentos` in 13 files of 12.9 MiB, a bronze `empresas` partition of 12 files of a few MiB, and gold tables of a few KB (1 or 2 files). A full release is about 40 times larger (17 GB of `estabelecimentos` unzipped). The cluster has one worker with 2 cores.

There are two ways to steer file size: at write time (`repartition(n)` / `coalesce(n)` before the write, `maxRecordsPerFile`, the number of shuffle partitions) or after the write, by compaction (`OPTIMIZE`, task 6.3).

## Decision

1. **Target: about 128 MiB per file** (64–256 MiB acceptable) for the large bronze and silver tables. 128 MiB rather than 1 GiB because the lake is small (one month of a full release is a few GB) and a `MERGE` rewrites whole files: smaller files keep the rewrite of a monthly update proportional to the keys that changed.
2. **Reached by compaction, not at write time:** `OPTIMIZE` with `spark.databricks.delta.optimize.maxFileSize = 128m` (task 6.3). The writes do **not** `repartition(n)` or `coalesce(n)` to a fixed number of files:
   - `coalesce(n)` lowers the parallelism of the whole stage that feeds the write (the typing, row hash and window of silver would run on n tasks), which is what made the baseline `MERGE` stage spill;
   - `repartition(n)` adds a full shuffle of the table only to shape the files, and a fixed n is wrong at either the sample or the full volume.
   The number of files written follows the input splits (bronze) or the shuffle partitions coalesced by AQE (silver `MERGE`, gold), each capped at `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MiB of shuffle data, the default; with `coalescePartitions.parallelismFirst`, also the default, AQE keeps at least one partition per core).
3. **Gold and lookup tables:** tiny (KBs); one or two files each, no action.

## Alternatives considered

- **`repartition(n)` before each write:** predictable file count, but one more shuffle and a number that has to change with the volume.
- **`coalesce(1)` for small tables:** one file, but one task does the work; only harmless for the gold tables, which already end in one or two files.
- **`maxRecordsPerFile`:** caps the file size but never merges small files.
- **Auto compaction / optimized writes (`delta.autoOptimize.*`):** available in Delta Lake 3.x, but they add work to every write of the monthly job; an explicit `OPTIMIZE` after the load keeps the cost visible and measurable.

## Consequences

- At the sample volume the silver files stay below the target after a load (a few files of tens of MiB); `OPTIMIZE` (6.3) compacts them and its effect on the read time is measured there.
- No write path has a hard-coded partition count; scaling to the full release changes only the AQE-coalesced partition count.
- The bronze partitions with many small files are compacted by the same `OPTIMIZE` (6.3).

## Note (2026-10-22, task 6.3)

Measured in the [benchmark](../benchmark.md#optimize-and-vacuum-task-63) (run 39864257683, 25% sample): `OPTIMIZE` with `maxFileSize` = `minFileSize` = 128 MiB took the bronze partitions from 3–12 and 8–9 files to **1 file per partition**, silver `empresas` from 5 files to 1 and silver `estabelecimentos` from 13 files (12.9 MiB) to 2 (84 MiB), with identical fingerprints; the silver read workload went from 4.37 s to 2.89 s (−34%). At this volume every partition is smaller than 128 MiB, so the target acts as the cap of a bin and no file reaches it; at the full release (~40x) the same setting yields files of about 128 MiB. Z-order and the `VACUUM` retention are decided in [ADR 013](013-table-maintenance-and-vacuum-retention.md).
