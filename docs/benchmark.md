# Benchmark (RF-08, RNF-01)

Baseline of the main job before any tuning (task 6.1). The tuning itself is task 6.2 and the before × after comparison is task 6.4. Runs execute on GitHub Actions runners ([ADR 006](adr/006-ci-as-execution-environment.md)): every measurement is repeated 3 times, the median is reported and the runner specification is recorded.

> **Status: PENDING.** The method, the helper and the `Benchmark` workflow are in place; the numbers are filled in after the first manual run of the workflow. GitHub only dispatches a `workflow_dispatch` workflow whose file is on the default branch, so that run happens after this change is merged.

## Main job and reference volume

- **Main job: the monthly incremental load, silver `MERGE` + gold.** It runs every month in production and holds the widest operations of the pipeline: the deduplication window, the row hash and the `MERGE` by business key in silver ([ADR 011](adr/011-silver-current-state-merge.md)), then the joins and aggregations of gold ([ADR 012](adr/012-gold-aggregations-and-counting-rules.md)). Bronze is a plain CSV → Delta copy and is left out.
- **Volume:** release `2026-09`, files `lookups` (whole), `Empresas1` and `Estabelecimentos1` (~440 MB zipped), with a deterministic 25% sample of the two large tables in bronze (`SPARK_SAMPLE_FRACTION=0.25`, fixed seed, RNF-03). The next month (`2026-10`) is synthetic and derived from the same bronze by `scripts/silver_next_month.py` (about 1% of the keys updated, 1% inserted, 1% absent), so the `MERGE` has real inserts and updates. `Estabelecimentos1` is included because without it gold builds only the size table and the job has no join worth measuring.
- **Why the sample:** a trial run with every row (≈ 4.5 M companies + 4.8 M establishments) took about 25 min for the initial silver load and 27 min for the `MERGE` on a 2-CPU runner, and the shuffle files filled the 14 GB disk during the second repetition. The sample keeps the same plan and the same operations at a size that fits 3 repetitions in the runner and in the Actions minutes. The fraction is a workflow input (`sample_fraction`), so a full-volume run stays possible.

## Environment

| Item | Value |
|---|---|
| Runner | GitHub-hosted `ubuntu-latest` (private repository: 2 vCPU, 8 GB RAM, 14 GB SSD); exact image, CPU model and memory recorded by the workflow in the job summary and in `benchmark.json` |
| Stack | Docker Compose of the project ([ADR 005](adr/005-docker-environment-images.md)): 1 Spark master, 1 worker (2 cores, 2 GB), driver in `spark-client`, MinIO (s3a) |
| Versions | Spark 3.5.3, Delta Lake 3.2.1, Python 3.11, Java 17 (`cnpj_lakehouse/versions.py`) |
| Key configs | `spark.executor.memory=1g`, `spark.sql.adaptive.enabled=true`, everything else at the Spark defaults (e.g. `spark.sql.shuffle.partitions=200`, `spark.sql.autoBroadcastJoinThreshold=10MB`); the properties actually set are read from the event log |

## Method

1. `Benchmark` workflow (`.github/workflows/benchmark.yml`, `workflow_dispatch` only): frees disk, downloads the files, brings up the stack, loads bronze (`2026-09`) and the synthetic next month (`2026-10`).
2. `scripts/benchmark.py run` (also `make benchmark`), inside the `spark-client` container. Each step is its own Spark application with **event logs enabled** through `SPARK_EVENT_LOG_DIR` (`cnpj_lakehouse/spark.py`), so the pipeline code is unchanged:
   - setup, once: `cnpj_lakehouse.silver` for `2026-09` creates the silver tables (reported, not repeated);
   - main job, 3 times: `cnpj_lakehouse.silver` for `2026-10` (`MERGE` of `empresas` and `estabelecimentos`), then `cnpj_lakehouse.gold`. Before repetitions 2 and 3 the merged tables are restored to version 0 (Delta `RESTORE`) and the files of the undone `MERGE` are vacuumed, so every repetition merges the same month into the same table.
3. `cnpj_lakehouse.benchmark` parses each event log (no `SparkListener` and no live UI needed): wall time, jobs, stages, tasks, shuffle read/write bytes, memory and disk spill, input/output bytes, executor and GC time, task-duration skew (max / median) per stage and the longest stages. It reports the median of the 3 repetitions per step and for the main job (the sum of its two steps per repetition).
4. Files: `DESCRIBE DETAIL` (`numFiles`, `sizeInBytes`) for every bronze, silver and gold table, and live files per partition value for the partitioned tables (bronze by `competencia`).
5. **Spark UI evidence:** `scripts/benchmark.py ui` starts a Spark History Server over the event logs and saves, for the median repetition, the REST API JSON (jobs, stages, executors, task-duration quantiles of the 3 longest stages) and the server-rendered jobs, stages and SQL pages. The workflow uploads `benchmark.json`, `benchmark.md`, the event logs and these pages as the `benchmark-<competencia>` artifact (metadata only, never rows, RST-01). To browse the full UI, download the artifact and run a History Server locally with `spark.history.fs.logDirectory` pointing at `eventlogs/`.

## Baseline

PENDING: filled after the first manual run of the `Benchmark` workflow (median of 3; the full sheet is `benchmark.md` in the artifact).

| Metric (median of 3) | silver `MERGE` | gold | Main job |
|---|---|---|---|
| Wall time | | | |
| Shuffle read / write | | | |
| Spill (memory / disk) | | | |
| Tasks | | | |

| Table | Files | Size | Avg file size | Files per partition |
|---|---|---|---|---|
| bronze/empresas | | | | |
| bronze/estabelecimentos | | | | |
| silver/empresas | | | | |
| silver/estabelecimentos | | | | |
| gold/gold_empresas_cnae_uf_ano | | | | |
| gold/gold_distribuicao_porte | | | | |
| gold/gold_situacao_setor | | | | |

## Observations

PENDING: to be written from the measured run (skew, small files, wide shuffles, partitions). Points to check, seen in the logs of the trial runs:

- whole-stage code generation is disabled for some silver plans ("Code grows beyond 64 KB", the row hash over all business columns), so those stages fall back to interpreted execution;
- the heaviest silver stages run with only 2–3 tasks (few large input splits for 2 cores), with memory and disk spill in the `MERGE` write;
- the quality counts of silver are a separate full pass over the month;
- gold and the Delta log reads use many tiny tasks (default shuffle partitions), which also produces high skew ratios on stages that last seconds.

## After tuning (task 6.2)

To be filled by task 6.2; the before × after table is task 6.4.

| Metric | Baseline | After tuning | Change |
|---|---|---|---|
| Main job runtime | | | |
| Shuffle read/write | | | |
| Spill (memory/disk) | | | |
| Files per partition | | | |
