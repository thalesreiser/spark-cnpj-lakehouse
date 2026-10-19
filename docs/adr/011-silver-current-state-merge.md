# 011 - Silver as the current state per business key, loaded by Delta MERGE

- **Status:** accepted
- **Date:** 2026-10-19

## Context

RF-04 (Must): when a new reference month (`competencia`) arrives, a re-run must update the changed records and insert the new ones, without duplicates. Until task 3.2 the silver job wrote each month as its own partition with `replaceWhere` (same rule as bronze, ADR 002): idempotent, but every month was a full copy, nothing was "updated", and the current record of a key was only unique *within* a month.

Facts that shape the decision:

- Each Receita Federal release is a full snapshot (ADR 002), so the history of every month is already kept in bronze, partitioned by `competencia`.
- Most keys do not change from one month to the next; rewriting all of them every month costs I/O and hides what actually changed.
- Runs may load a subset of the release (CI loads `lookups` + `Empresas1`, one of ten files), so a key missing from a run does not mean the company left the register.
- Time travel (task 4.3) relies on Delta versions.

## Decision

1. **Write strategy.** Each silver table `s3a://<bucket>/silver/<table>/` holds the **current state of every business key across months** (one row per key, keys as in the [data dictionary](../data-dictionary.md#silver-keys-and-current-record)). One run deduplicates the month's bronze rows as before (RF-03) and merges them into the table (`cnpj_lakehouse.silver.merge_silver`, the I/O layer; the conditions are pure functions):
   - first run: the table is created from the month (Delta version 0);
   - later runs: one Delta `MERGE` joined on the business key with null-safe equality (`<=>`, so the optional parts of the `socios` natural key match like the deduplication groups);
   - `whenMatchedUpdateAll` **only when something changed** and the month is not older: `NOT (t._row_hash <=> s._row_hash) AND s.competencia >= t.competencia`;
   - `whenNotMatchedInsertAll` for new keys.
2. **Change detection.** A `_row_hash` column (SHA-256 of the JSON of the business columns, nulls included; bronze metadata `competencia`, `_ingest_ts`, `_source_file` left out) is computed in the deduplication and **stored in the table**. The same record loaded from another month or ingestion has the same hash, so an unchanged key is not rewritten and keeps the `competencia` / `_ingest_ts` / `_source_file` of its **last change**. `_row_hash` is also the last tie-breaker of the current-record ordering (rows that still tie have the same content), so the choice does not depend on the month either.
3. **Absent keys are kept** (no `whenNotMatchedBySource` delete). Closed companies stay in the source with a registration status, and partial runs (a subset of the files) would otherwise delete most of the table.
4. **Partitioning.** The silver tables are **not partitioned**. A `competencia` partition would now mean "month of the last change": skewed, and a MERGE would move rows between partitions without pruning. UF partitioning and file size stay with epic 6 (ADR 002, ADR 004).
5. **Metrics.** The Delta commit metrics (`numSourceRows`, `numTargetRowsInserted`, `numTargetRowsUpdated`; `WRITE`'s `numOutputRows` on creation) go to the run summary as `operation`, `table_version`, `rows_inserted`, `rows_updated`, `rows_unchanged` (source rows minus inserted and updated) and `table_rows`. If Delta skips an empty commit, the run reports 0 inserted and 0 updated.
6. **Re-runs.** Re-running the same month changes nothing (0 inserted, 0 updated); re-running an older month never overwrites a newer record.

## Alternatives considered

- **Keep one partition per month (`replaceWhere`):** idempotent but not incremental; consumers would have to pick the latest month per key; storage grows by a full snapshot per month on top of bronze.
- **`whenMatchedUpdateAll` without a condition:** rewrites every matched key every month, so the metrics would not show what changed and each version would be a full rewrite.
- **Compare every column in the condition** instead of a stored hash: a long, table-specific condition, with null handling per column; the hash is one column and one comparison.
- **Delete keys absent from the month:** correct only for complete releases; wrong for subset runs, and the source already flags closed companies.
- **A `last_seen` month on every key:** would update every key every month (same cost as no condition).
- **SCD type 2 (one row per version of a key):** history already exists in bronze and in the Delta versions; can be added in gold if needed.

## Consequences

- One Delta version per loaded month (plus re-runs that changed something): `VERSION AS OF` / `TIMESTAMP AS OF` give the silver state after each load (task 4.3). Version 0 is the first month loaded. `VACUUM` retention bounds how far back this goes.
- `competencia` in silver is the month of the last change of the key, not the month of the run; a query "as of month M" uses time travel, or bronze.
- Loading months out of order is safe for existing keys (an older month never wins); keys first seen in an older month are inserted.
- Silver tables written with the old layout (partitioned by `competencia`, no `_row_hash`) must be dropped before the first MERGE run; CI always starts from an empty MinIO.
- The MERGE needs Delta: the unit tests that exercise it run in their own pytest process with the Delta jars (`tests/test_silver_merge.py`, marker `delta`); the CI `bronze` job proves it on real data (a re-run merges nothing; a synthetic next month derived from the same bronze, `scripts/silver_next_month.py`, gives the expected updated and inserted counts with no duplicated key).
