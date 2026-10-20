# Benchmark (RF-08, RNF-01)

Baseline of the main job before any tuning (task 6.1). The tuning itself is task 6.2 and the before × after comparison is task 6.4. Runs execute on GitHub Actions runners ([ADR 006](adr/006-ci-as-execution-environment.md)): every measurement is repeated 3 times, the median is reported and the runner specification is recorded.

> **Status: measured.** Baseline from the `Benchmark` run [39447843930](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39447843930) on `main` (2026-10-20, Spark 3.5.3, median of 3 repetitions). The full sheet, the event logs and the Spark UI pages are in its `benchmark-2026-09` artifact.

## Main job and reference volume

- **Main job: the monthly incremental load, silver `MERGE` + gold.** It runs every month in production and holds the widest operations of the pipeline: the deduplication window, the row hash and the `MERGE` by business key in silver ([ADR 011](adr/011-silver-current-state-merge.md)), then the joins and aggregations of gold ([ADR 012](adr/012-gold-aggregations-and-counting-rules.md)). Bronze is a plain CSV → Delta copy and is left out.
- **Volume:** release `2026-09`, files `lookups` (whole), `Empresas1` and `Estabelecimentos1` (~440 MB zipped), with a deterministic 25% sample of the two large tables in bronze (`SPARK_SAMPLE_FRACTION=0.25`, fixed seed, RNF-03). The next month (`2026-10`) is synthetic and derived from the same bronze by `scripts/silver_next_month.py` (about 1% of the keys updated, 1% inserted, 1% absent), so the `MERGE` has real inserts and updates. `Estabelecimentos1` is included because without it gold builds only the size table and the job has no join worth measuring.
- **Why the sample:** a trial run with every row (≈ 4.5 M companies + 4.8 M establishments) took about 25 min for the initial silver load and 27 min for the `MERGE` on a 2-CPU runner, and the shuffle files filled the 14 GB disk during the second repetition. The sample keeps the same plan and the same operations at a size that fits 3 repetitions in the runner and in the Actions minutes. The fraction is a workflow input (`sample_fraction`), so a full-volume run stays possible.

## Environment

| Item | Value |
|---|---|
| Runner | GitHub-hosted `ubuntu-latest` (image `ubuntu24 20261018.339.1`). The repository is public since 2026-10-20, so the runner has **4 vCPU and 15.6 GiB RAM** (the trial runs below were on the 2 vCPU / 8 GB private-repository runner); recorded in the job summary and in `benchmark.json` |
| Stack | Docker Compose of the project ([ADR 005](adr/005-docker-environment-images.md)): 1 Spark master, 1 worker (2 cores, 2 GB), driver in `spark-client`, MinIO (s3a) |
| Versions | **Baseline measured on Spark 3.5.3**, Delta Lake 3.2.1, Python 3.11, Java 17 (`cnpj_lakehouse/versions.py`). The project moves to Spark 3.5.8 (security fix, same data: the silver fingerprints of CI are identical on both versions); task 6.2 measures on 3.5.8, so a small part of any difference may come from the patch release |
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

Median of 3 repetitions (median repetition: #3). The main job is the sum of its two steps per repetition; the setup (initial silver load of `2026-09`, run once) is shown for reference.

| Metric (median of 3) | silver `MERGE` | gold | Main job | Setup: initial silver (1 run) |
|---|---:|---:|---:|---:|
| Wall time | 395.1 s | 63.6 s | **459.7 s** | 372.6 s |
| Jobs / stages / tasks | 66 / 66 / 817 | 85 / 82 / 1,087 | 151 / 148 / 1,904 | 152 / 152 / 2,139 |
| Shuffle read / write | 1.5 / 1.5 GiB | 145.7 / 145.7 MiB | 1.6 / 1.6 GiB | 486.6 / 486.6 MiB |
| Spill (memory / disk) | 840.0 / 364.0 MiB | 0 / 0 | 840.0 / 364.0 MiB | 400.0 / 189.6 MiB |
| Input / output | 1.4 GiB / 269.1 MiB | 93.7 / 1.3 MiB | 1.4 GiB / 270.4 MiB | 127.7 / 264.3 MiB |
| Executor run time / GC | 662.0 / 18.6 s | 62.4 / 1.3 s | 724.4 / 19.9 s | 619.5 / 23.8 s |
| Max task skew (max / median) | 15.32x | 18.76x | 18.76x | 14.36x |

Main job per repetition: 454.2 s, 461.1 s, 459.7 s (spread under 2%); shuffle write 1.6 GiB in all three; disk spill 184.7, 364.0 and 364.0 MiB.

| Table | Files | Size | Avg file size | Files per partition |
|---|---:|---:|---:|---|
| bronze/empresas | 15 | 58.7 MiB | 3.9 MiB | 3–12 (2 partitions) |
| bronze/estabelecimentos | 17 | 165.5 MiB | 9.7 MiB | 8–9 (2 partitions) |
| silver/empresas | 5 | 102.0 MiB | 20.4 MiB | 5 (not partitioned) |
| silver/estabelecimentos | 13 | 167.1 MiB | 12.9 MiB | 13 (not partitioned) |
| gold/gold_empresas_cnae_uf_ano | 2 | 1.3 MiB | 679.7 KiB | 2 |
| gold/gold_distribuicao_porte | 1 | 2.2 KiB | 2.2 KiB | 1 |
| gold/gold_situacao_setor | 1 | 8.1 KiB | 8.1 KiB | 1 |

Lookup tables (bronze and silver `cnaes`, `motivos`, `municipios`, `naturezas`, `paises`, `qualificacoes`) are one file each, 3 KiB to 432 KiB.

## Observations

- **The `MERGE` dominates: 86% of the main job (395 of 460 s).** Its two longest stages are a Delta write stage of 164.5 s with only **3 tasks**, which writes 353 MiB of shuffle and spills 185 MiB to disk, and the `first()` at `silver.py:326` (110.2 s, 3 tasks), an action that forces a full pass over the month before the write. Together they are 70% of the `MERGE` (275 of 395 s).
- **Too few tasks for the heavy stages, too many for the light ones.** The heaviest silver stages run with 2–3 tasks on a 4-core runner (few large input splits; the worker has 2 cores), so most of the CPU is idle while they spill. Gold does the opposite: 1,087 tasks for 64 s of work (default `spark.sql.shuffle.partitions=200` on tiny aggregations), which is where the 18.8x skew ratio comes from — stages of a few seconds where one task is slower, not real data skew.
- **Spill in the `MERGE` write** (840 MiB memory, 364 MiB disk) with `spark.executor.memory=1g`: the executor memory and the number of partitions of the write are the first levers.
- **Code generation** is disabled for the row-hash plans ("Code grows beyond 64 KB" in the trial logs), so those stages run interpreted.
- **Files:** silver is written as 5 and 13 files of 13–20 MiB, below the usual 128 MiB–1 GiB target, and bronze `empresas` has a partition with 12 files of a few MiB (small files, task 6.3 `OPTIMIZE`). Gold tables are tiny, so their file count does not matter.
- Candidate changes for task 6.2: drop or cache the `first()` pass, repartition the `MERGE` source to match the cores, enable AQE coalescing with a lower initial `shuffle.partitions` for gold, raise the executor memory within the worker's 2 GB, and `OPTIMIZE` bronze (6.3).

## After tuning (task 6.2)

To be filled by task 6.2; the before × after table is task 6.4.

| Metric | Baseline | After tuning | Change |
|---|---|---|---|
| Main job runtime | | | |
| Shuffle read/write | | | |
| Spill (memory/disk) | | | |
| Files per partition | | | |
