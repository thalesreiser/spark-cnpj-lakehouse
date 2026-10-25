# 05 - Validation

How each requirement of [02](02-analysis-and-prioritization.md) was checked against the scenarios of [03](03-specification.md). Run IDs link to GitHub Actions (`https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/<id>`). Every CI job and artifact holds counts and metrics only (RST-01).

## Requirements review checklist
- [x] Each requirement is singular, verifiable, unambiguous and traceable (one ID, one Gherkin scenario in 03, one row in [06](06-traceability.md))
- [x] Every "Must" RF has an executable acceptance criterion (a test file and a CI step or workflow run)
- [x] Numeric targets (RNF) were measured and recorded (RNF-01 in [benchmark](benchmark.md); coverage ≥ 80% enforced by `make test`)

## Evidence sources

| Source | What it proves | Reference |
|---|---|---|
| `CI` › `lint-test` | ruff, black, mypy strict; `make test` (unit + Delta tests on the synthetic release, ≥ 80% coverage) | every PR; `main` run [39974158684](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39974158684) |
| `CI` › `smoke` | Docker Compose stack up; Delta write/read on MinIO through the cluster | same run |
| `CI` › `bronze` | Real data (`lookups` + `Empresas1` of 2026-09, from the Actions cache, SHA-256 verified): bronze, counts, silver, re-run, synthetic next month, time travel, quality, gold, reconcile | same run |
| `Download` › `monthly-check` | Live download of the latest month from the portal, `lookups Empresas1 Estabelecimentos1` unsampled, full pipeline | run [39873234968](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39873234968) (2026-10-22) |
| `Benchmark` | Before × after on one runner, Spark UI | run [39975706499](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39975706499) (final); history: [39447843930](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39447843930) (6.1), [39787450949](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39787450949) (6.2, formats), [39864257683](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39864257683) (6.3) |
| `Gold analysis` | Gold on real establishments, notebook 02 executed | run [39433114188](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39433114188) (2026-10-20) |
| `Fixture privacy` | Synthetic CNPJ bases absent from the real registry | scheduled run [39410110093](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/runs/39410110093) (2026-10-20) and PRs touching fixtures |

## Acceptance criteria execution results

| ID | Criterion (02) | How it was validated | Evidence | Result | Date |
|---|---|---|---|---|---|
| RF-01 | Files in `data/raw/<month>/` with a recorded checksum | Integration test against a local HTTP server (files, manifest SHA-256, idempotent re-run, resume with/without `Range`, retries); offline `--verify-only` on every CI run; live download | `tests/test_download.py`, `tests/test_latest_competencia.py`; CI `bronze` step "Verify every file against the manifest"; `monthly-check` run 39873234968 | ✅ pass | 2026-10-22 |
| RF-02 | Every row in bronze plus `_ingest_ts` and `_source_file` | Source × target row count on the synthetic release and on real data; re-run replaces the partition | `tests/test_bronze.py`, `tests/test_bronze_counts.py`; CI `bronze` steps "make bronze again" and "make bronze-check" (`scripts/bronze_check.py`: source lines == bronze rows); `monthly-check` (also on `Estabelecimentos1`) | ✅ pass | 2026-10-23 |
| RF-03 | No duplicated business key; 100% of valid dates typed | Unit and chispa tests of dedup and normalization; `silver-check` and `quality` on real data | `tests/test_silver.py`, `tests/test_silver_dedup.py`, `tests/test_normalize.py`, `tests/test_quality.py`; CI `bronze` steps "make silver-check" (0 duplicated keys, date columns typed) and "make quality" (report in the job summary) | ✅ pass | 2026-10-23 |
| RF-04 | New month updates and inserts without duplicates | Delta `MERGE` tests; same month twice → same fingerprint, 0 inserted / 0 updated; synthetic next month → expected counts | `tests/test_silver_merge.py`, `tests/test_silver_idempotency.py`, `tests/test_fingerprint.py`; CI `bronze` steps "make silver again" and "make silver next month" | ✅ pass | 2026-10-23 |
| RF-05 | Gold totals match silver | Sum checks in every gold run; independent reconciliation by total, UF, status, size and grain; deliberately broken gold fails | `tests/test_gold.py`, `tests/test_gold_delta.py`, `tests/test_reconcile.py`; CI `bronze` steps "make gold" and "make reconcile" (size table on real data); `monthly-check` 39873234968 and `Gold analysis` 39433114188 (establishment tables on real data, 4,753,435 establishments) | ✅ pass | 2026-10-22 |
| RF-06 | Reproduce a query on version N-1 | Aggregate recorded after 2026-09, compared with `versionAsOf` N-1 and `timestampAsOf` after the synthetic 2026-10 | `tests/test_time_travel.py`; CI `bronze` step "make time-travel"; `notebooks/01-time-travel.ipynb` | ✅ pass | 2026-10-23 |
| RF-07 | Files per partition drop; read time before and after | `OPTIMIZE` + `VACUUM` in the `Benchmark` workflow with fingerprints before/after | `tests/test_maintenance.py`; `Benchmark` 39864257683 and 39975706499: files per partition 3–12 → 1, silver reads 4.37 → 2.89 s (−34%), identical fingerprints ([benchmark](benchmark.md#optimize-and-vacuum-task-63)) | ✅ pass | 2026-10-23 |
| RF-08 | `docs/benchmark.md` with comparison table and Spark UI evidence | Review of [benchmark.md](benchmark.md): same-runner table (time, shuffle, spill, tasks, files), Spark UI screenshots in `docs/img/benchmark`, event logs in the artifact | `Benchmark` 39975706499; `tests/test_benchmark.py` | ✅ pass | 2026-10-23 |
| RF-09 | `pytest` runs without the full dataset | Tests use one local SparkSession, in-memory DataFrames and `tests/fixtures`; coverage gate | CI `lint-test` (`make test`, fails below 80% for the package and the transformation modules) | ✅ pass | 2026-10-23 |
| RF-10 | Candidate pairs with a score and a sample review | Not executed | — | ⬜ not implemented (Could) | — |
| RNF-01 | ≥ 30% reduction of the main job after tuning | Pre-tuning commit `e1dcb45` and current code measured in the same job, same runner (AMD EPYC 7763), same data, median of 3; 11 silver/gold fingerprints asserted identical | `Benchmark` 39975706499: **481.9 s → 240.6 s (−50.1%)**. The earlier 459.7 s → 169.8 s (−63.1%, task 6.2) compared two runner CPU models and is kept only as history | ✅ pass (target 30%) | 2026-10-23 |
| RNF-02 | Docker Compose environment without paid cloud | Stack started and Delta round trip on MinIO on every PR | CI `smoke` (`scripts/smoke_delta.py`), `tests/test_spark.py`, `tests/test_smoke.py` | ✅ pass | 2026-10-23 |
| RNF-03 | Sampling parameter for limited RAM; full run when resources allow | Deterministic sampling tested; benchmark at 0.25; unsampled (1.0) run of `lookups Empresas1 Estabelecimentos1` on a 16 GB runner | `tests/test_bronze.py` (sampling); `Benchmark` (`SPARK_SAMPLE_FRACTION=0.25`); `monthly-check` 39873234968 (`1.0`) | ✅ pass; the whole release (all ten files of each table) was not run: it exceeds the runner disk (ADR 006) | 2026-10-22 |
| RNF-04 | Pure, testable transformations | Review: `normalize.py` is `Column -> Column`; bronze/silver/gold steps are DataFrame functions, jobs only orchestrate; each tested in isolation | `tests/test_normalize.py`, `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py` | ✅ pass | 2026-10-23 |
| RNF-05 | Pinned Spark, Delta and Python; `make up && make run` | Version consistency test; image built in CI | `tests/test_versions.py`; CI `smoke` | ✅ versions pinned and checked; `make run COMPETENCIA=2026-09` runs the whole month in one command (`cnpj_lakehouse.pipeline`, `tests/test_pipeline.py`), proven by the CI `bronze` job step `make run` (`build/pipeline-2026-09.json`: 8 steps passed, download skipped after the cached download) | 2026-10-24 |
| RN-01 | Establishment key = base + order + DV | Dedup tests on the three-part key; `silver-check` on real data | `tests/test_silver.py`, `tests/test_silver_dedup.py`; CI `bronze` "make silver-check" | ✅ pass | 2026-10-23 |
| RST-01 | No re-identification, no raw data in the repository | Fixtures synthetic, bases verified absent from the registry; artifacts and summaries hold counts only; `data/` ignored by Git | `Fixture privacy` 39410110093, `tests/test_check_fixture_cnpjs.py`; review of the workflow artifacts and notebooks (aggregates only) | ✅ pass | 2026-10-20 |

## Success metrics of [01](01-vision-and-scope.md)

| Metric | Target | Measured | Result |
|---|---|---|---|
| Main job runtime reduction | ≥ 30% | −50.1% on one runner (run 39975706499) | ✅ |
| Test coverage of the transformations | ≥ 80% | Enforced by `make test` in CI `lint-test` | ✅ |
| Pipeline reproducible with a single command | one command | `make up && make run COMPETENCIA=...`: download, manifest check, bronze, bronze-check, silver, silver-check, quality, gold, reconcile; stops at the first failure; run summary in `build/pipeline-<COMPETENCIA>.json` and `.md` (CI `bronze`) | ✅ |
