# 003 - Broadcast join for lookup tables

- **Status:** accepted
- **Date:** 2026-10-19

## Context

The gold aggregations (RF-05) attach descriptions to codes of the large tables: the CNAE of each establishment (`cnaes`, 1,359 rows in 2026-09), and the documented domains of company size, registration status and CNAE sections (4 to 87 rows, kept in code because the source has no file for them). `estabelecimentos` has tens of millions of rows in a full release (17 GB unzipped). A sort-merge join would shuffle the large side by the lookup key only to match it with a few kilobytes of data. AQE (enabled, `spark.sql.adaptive.enabled`) can turn a join into a broadcast at runtime, but only after a shuffle stage has produced statistics, and only below `spark.sql.autoBroadcastJoinThreshold` as estimated from the files, so the plan would depend on table statistics.

## Decision

1. Every join with a lookup or domain table is written with an explicit `F.broadcast(...)` hint on the small side (`cnpj_lakehouse/gold.py`): the lookup is sent once to every executor and the large side is joined in place, with no shuffle for the join.
2. The lookups are joined **left**, so a code missing from the lookup keeps its row with a null description, and are unique per code (silver deduplicates them by `codigo`), so a join never multiplies rows. The gold sum checks would catch a duplicate (ADR 012).
3. The join between the two large tables (`estabelecimentos` aggregated per `cnpj_basico` with `empresas`) is **not** broadcast: both sides grow with the release and a broadcast could exhaust the driver memory. Its strategy is left to Spark and AQE and will be measured in epic 6.
4. The tests check the plan: the optimized plan holds the hint (`strategy=broadcast`) and the physical plan a `BroadcastHashJoin` (`tests/test_gold.py`).

## Alternatives considered

- **Rely on `autoBroadcastJoinThreshold` and AQE only:** usually the same result, but implicit and dependent on statistics; the hint documents the intent and keeps the plan stable.
- **Join the descriptions after the aggregation:** cheaper still (both sides small), but the lookup join would no longer show the pattern for later joins on row-level data (e.g. municipalities in a map), and the difference is negligible against the aggregation shuffle. Can be revisited in the epic 6 benchmark.
- **Map the codes with a Python dict and a UDF or `create_map`:** works for the static domains but not for `cnaes`, which changes with each release, and a UDF is opaque to the optimizer.

## Consequences

- No shuffle of the establishments for the lookup joins; the only shuffles of the gold job are the aggregations and the companies x establishments join.
- A lookup must stay small: if a "lookup" ever grows to hundreds of MB the hint becomes a risk for executor memory and must be removed.
- The before/after effect is part of the epic 6 benchmark (RF-08).

## Note (2026-10-22, task 6.2): measured, unchanged

The event logs of the 6.1 baseline confirm the plan: every lookup and domain join of gold runs as a `BroadcastHashJoin` and the companies x establishments join (and the anti join that counts establishments without a company) as a `SortMergeJoin`. Gold is 14% of the main job (64 of 460 s) and its time goes mostly to Delta log replay and job overhead, not to joins, so the decision stands: explicit broadcast for lookups, Spark and AQE (`spark.sql.adaptive.autoBroadcastJoinThreshold`, skew join enabled in `cnpj_lakehouse.spark.TUNING_CONF`) for the large join.

