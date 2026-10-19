# spark-cnpj-lakehouse

[![CI](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/thalesreiser/spark-cnpj-lakehouse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD4)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> A layered lakehouse (bronze → silver → gold) built with **Apache Spark** and **Delta Lake** on top of the Brazilian Federal Revenue Service (Receita Federal) CNPJ open data, with incremental loads, auditable history and a documented benchmark of the performance gain obtained through tuning.

**Status:** 🚧 in progress · epic 2 of 7 (bronze ingestion)

---

## Why this project exists

The Receita Federal CNPJ registry is one of the largest public datasets in Brazil: tens of millions of companies and establishments, plus partners and lookup tables (CNAE activity codes, municipalities, legal natures), published as compressed, untyped CSV files. It is a realistic case for distributed processing: a volume that does not fit comfortably in a single machine's memory, dirty data, composite keys and periodic updates.

The goal is not just "running Spark", but showing the full cycle of a data engineer:

- **Ingest** with an explicit schema and lineage metadata
- **Clean and model** in layers with testable rules
- **Update incrementally** without duplicates (MERGE)
- **Measure, optimize and prove** the gain with numbers from the Spark UI

## Technical highlights

| Topic | What is demonstrated |
|---|---|
| **Delta Lake** | ACID transactions, incremental `MERGE`, *time travel* (querying version N-1), schema evolution, `OPTIMIZE` and `VACUUM` |
| **Spark tuning** | AQE, *broadcast join* of lookup tables, partitioning by reference month and state, file size control, `repartition` vs `coalesce` |
| **Honest benchmark** | Runtime, *shuffle*, *spill* and number of files before × after, with Spark UI evidence. Target: **at least 30% reduction** in the main job |
| **Data quality** | Uniqueness, null, domain and silver × gold reconciliation checks |
| **Software engineering** | Transformations as pure functions, `chispa` tests, CI, pre-commit, ADRs |
| **Portability** | Everything runs with Docker Compose (Spark + MinIO with an S3 API), with no cloud cost |

## Architecture

```mermaid
flowchart LR
  RF[Receita Federal<br/>zipped CSVs] -->|download.py<br/>+ checksum| RAW[(data/raw/&lt;reference month&gt;)]
  RAW -->|explicit schema<br/>_ingest_ts, _source_file| B[(Bronze<br/>Delta)]
  B -->|cleaning, typing,<br/>deduplication| S[(Silver<br/>Delta)]
  S -.->|incremental MERGE| S
  S -->|aggregations| G[(Gold<br/>Delta)]
  G --> N[Notebook / Spark SQL]

  subgraph Docker Compose
    SP[Spark master + workers]
    M[MinIO<br/>S3 storage]
  end
```

| Layer | Content | Guarantee |
|---|---|---|
| **Bronze** | CSVs as received, with an explicit schema + `_ingest_ts` and `_source_file` | Source row count = target row count |
| **Silver** | Typed, normalized data, deduplicated on the business key | No duplicated CNPJ; 100% of valid dates typed as `date` |
| **Gold** | Companies by CNAE, state, year of opening, size and registration status | Totals reconcile with silver |

### Did you know? The anatomy of a CNPJ

A CNPJ (the Brazilian company tax ID) has 14 digits split into three parts, and that is how the establishment business key is built (rule **RN-01**):

```
12.345.678 / 0001 - 95
└── base ──┘ └order┘ └check┘
  company    head     check
             office   digits
             or branch
```

The first 8 digits identify the **company**; order `0001` is usually the head office and the others are branches. That is why the dataset comes split into "companies" (by base CNPJ) and "establishments" (by full CNPJ), and the join between them is one of the tuning points of the project.

### Questions the gold layer answers

- How many companies were opened per year in each state and sector (CNAE)?
- How are active companies distributed by size in each region?
- Which sectors have the highest share of closed or inactive companies?

## Stack

| Layer | Technology | Why |
|---|---|---|
| Processing | Apache Spark 3.5 (PySpark), AQE enabled | Industry standard for large volumes |
| Table format | Delta Lake | ACID, MERGE, time travel, schema evolution |
| Storage | MinIO (S3 API, maintained `pgsty/minio` fork) | The same code later runs on AWS, at no cost |
| Orchestration | Makefile + scripts | Simple and reproducible (Airflow is covered in [P1](https://github.com/thalesreiser/aws-fundos-pipeline)) |
| Tests | pytest + chispa | DataFrame comparison |
| Code quality | ruff, black, mypy, pre-commit, gitleaks | Standard shared by the 3 projects |
| CI | GitHub Actions | Lint + tests + Docker environment smoke test; also the execution environment for heavy runs ([ADR 006](docs/adr/006-ci-as-execution-environment.md)) |

## How to run

### Where things run

| Where | What |
|---|---|
| **Your machine** | Editing, `make lint`, `make test` (unit tests with small DataFrames; needs Python 3.11 and Java 17, no Docker) |
| **CI on every pull request** | Lint, type-check, tests; the `smoke` job, which builds the image, brings up Spark + MinIO and writes/reads a Delta table; and the `bronze` job, which ingests a real release subset into bronze and reconciles the row counts |
| **Manual workflows** | Heavy runs: the `Download` workflow (a reference month from the real source, manifest as an artifact); pipeline on a sample and benchmark as the pipeline grows |

The reasons are in [ADR 006](docs/adr/006-ci-as-execution-environment.md). The whole stack can still run locally if you have Docker:

### Run the Docker stack locally (optional)

**Prerequisites:** Docker Desktop (Windows/macOS) or Docker Engine with the Compose v2 plugin (Linux); GNU make (on Windows: `winget install ezwinports.make`); about 8 GB of RAM for the defaults (1 worker with 2 cores and 2 GB, 1 GB executors; adjust `SPARK_WORKER_MEMORY`/`SPARK_WORKER_CORES` in `.env`).

```bash
cp .env.example .env   # optional: MinIO dev credentials and Spark resources
make up                # builds the Spark image, starts MinIO + Spark master/worker and creates the bucket
make smoke             # writes and reads a Delta table on MinIO (s3a://lakehouse/_smoke/...)
make down              # stops everything (make down V=1 also deletes the MinIO volume)
```

`make up` waits for the *healthchecks* (MinIO alive, master up and worker registered) and creates the `lakehouse` bucket with `mc`. The first `make up` takes a few minutes to build the image. Other targets: `make ps` (service status) and `make logs`.

| Service | Address |
|---|---|
| Spark master (UI) | http://localhost:8080 |
| Spark worker (UI) | http://localhost:8081 |
| Spark master (cluster) | `spark://localhost:7077` |
| MinIO S3 API | http://localhost:9000 |
| MinIO console | http://localhost:9001 (user/password from `.env`; dev default `minioadmin`) |

Spark applications run in the `spark-client` service, which uses the same image as the executors (Python 3.11, Spark 3.5.3, Delta 3.2.1, hadoop-aws 3.3.4). The reasons are in [ADR 005](docs/adr/005-docker-environment-images.md). In the code, the session comes from `cnpj_lakehouse.spark.get_spark("app-name")`, configured through environment variables (`S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `LAKEHOUSE_BUCKET`, `SPARK_MASTER_URL`).

> The MinIO credentials in `.env.example` and in CI are for local development only. `.env` is never committed.

### Pipeline

```bash
make download COMPETENCIA=2026-09                            # lookup tables only (safe default, a few KB)
make download COMPETENCIA=2026-09 FILES="lookups Empresas1"  # a subset
make download COMPETENCIA=2026-09 FILES=all                  # the whole release (~7.8 GB zipped, ~29 GB unzipped)
make run                                                      # bronze → silver → gold
```

`COMPETENCIA` is the reference month (YYYY-MM) of the Receita Federal release, kept with its domain name. `make download` (RF-01) fetches the zips into `data/raw/<COMPETENCIA>/`, unzips them there and writes `manifest.json` with the source URL, size, SHA-256, download time and extracted files of each zip. It retries transient errors with backoff, resumes partial downloads with HTTP `Range`, and skips files that still match the manifest, so re-runs are idempotent. `FILES` accepts groups (`empresas`, `estabelecimentos`, `socios`, `simples`, `lookups`, `all`) and file names (`Empresas0`, `Cnaes`, ...). It defaults to `lookups`, so downloading the whole release always requires an explicit `FILES=all` (a protection for machines and runners with little disk). The same options exist on the CLI:

```bash
python -m cnpj_lakehouse.download --competencia 2026-09 --files lookups [--dest data/raw] [--base-url URL]
```

The source moved to a Nextcloud share in 2026; the current URL, layout, volumes and server behaviour are in [`docs/data-source.md`](docs/data-source.md) ([ADR 007](docs/adr/007-cnpj-source-nextcloud-webdav.md)). If it moves again, set `CNPJ_BASE_URL` (or `--base-url`). On GitHub, the manual **Download** workflow (`Actions → Download → Run workflow`, inputs `competencia` and `files`) downloads a release on a runner and publishes only `manifest.json` and a file listing as an artifact, never the raw data.

#### Bronze ingestion (RF-02)

```bash
make up
make bronze COMPETENCIA=2026-09                        # every table downloaded for the month
make bronze COMPETENCIA=2026-09 TABLES="empresas cnaes" # a subset
make bronze-check COMPETENCIA=2026-09                  # source line counts == bronze row counts
```

`make bronze` runs `python -m cnpj_lakehouse.bronze` in the `spark-client` container (the Spark worker mounts the repository read-only, so executors read `data/raw/` at the same path). For each table listed in `manifest.json` it:

1. **checks the layout:** counts the `;`-separated fields of every raw line, ignoring `;` inside quoted values (doubled quotes honoured). With a fixed schema Spark would silently drop extra fields and null missing ones, so any line with a different field count fails the run and the log shows the field-count histogram and examples (`--max-bad-lines N` tolerates N lines; [ADR 008](docs/adr/008-bronze-all-strings-typed-in-silver.md));
2. reads the CSVs with the explicit bronze schema (every column a string, no `inferSchema`) and adds `_ingest_ts` (one UTC timestamp per run), `_source_file` (path of the source CSV) and `competencia`;
3. samples the rows when `SPARK_SAMPLE_FRACTION` < 1 (fixed seed, so the sample is reproducible; RNF-03);
4. writes Delta to `s3a://<bucket>/bronze/<table>/`, partitioned by `competencia`; re-running a month replaces its partition (`replaceWhere`), never duplicates it ([ADR 002](docs/adr/002-partitioning-by-month-and-state.md));
5. logs rows read and written (and whether sampling was applied) and writes a summary to `build/bronze-<COMPETENCIA>.json`.

On every pull request the CI `bronze` job downloads `lookups` + `Empresas1` of 2026-09, ingests them, re-runs two tables and checks with `scripts/bronze_check.py` that every bronze partition has exactly as many rows as its source CSVs have lines, with `_ingest_ts` and `_source_file` always filled. It publishes only the counts (job summary and artifact).

#### Silver: types and deduplication (RF-03)

```bash
make silver COMPETENCIA=2026-09                         # every bronze table with rows for the month
make silver COMPETENCIA=2026-09 TABLES="empresas cnaes"  # a subset
make silver-check COMPETENCIA=2026-09                   # no duplicated business key, dates typed
```

`make silver` runs `python -m cnpj_lakehouse.silver` in the `spark-client` container. For each table it reads the month's bronze partition, applies the documented types (CNPJ incl. alphanumeric, CNAE, dates, money; [ADR 009](docs/adr/009-alphanumeric-cnpj-and-invalid-values.md)), discards rows without a business key, keeps **one current record per business key** with a window function (RN-01 base + order + DV for `estabelecimentos`, `cnpj_basico` for `empresas` and `simples`, a natural key for `socios`, `codigo` for the lookups; a later release, then the most recent relevant date, then `_ingest_ts`, with a deterministic final tie-breaker) and writes Delta to `s3a://<bucket>/silver/<table>/`, partitioned by `competencia` (re-runs replace the month). Establishments get `cnpj` and a `cnpj_valido` flag; invalid CNPJs are kept flagged by default (`--invalid-cnpj drop` discards them; [ADR 010](docs/adr/010-invalid-and-all-zero-cnpj-in-silver.md), proposed). Rows in and out, duplicates removed, rows without key, invalid CNPJs and date parse failures are logged and written to `build/silver-<COMPETENCIA>.json`. The keys and ordering per table are in the [data dictionary](docs/data-dictionary.md#silver-keys-and-current-record).

The CI `bronze` job then runs `make silver` on the same 2026-09 subset and `scripts/silver_check.py`, which fails if any business key appears twice, if a date column is not typed `date` or if a valid source date was lost.

For development: `make install` (dependencies + pre-commit hooks), `make lint` and `make test` (fails below 80% coverage, RF-09). On Windows without `HADOOP_HOME`/winutils the Spark write tests are skipped, so coverage can fall below the threshold: run `pytest` (no threshold) or `make test COVERAGE_MIN=0` locally and rely on CI (Linux) for the enforced run. Limited memory? Set `SPARK_SAMPLE_FRACTION=0.1` in `.env` to process 10% of the data (RNF-03).

## Repository layout

```
├── src/cnpj_lakehouse/   # download, schemas, bronze, silver, gold, pipeline, Spark session and versions
├── tests/                # unit tests (small DataFrames) and fixtures
├── notebooks/            # time travel and business questions
├── infra/                # docker-compose (Spark + MinIO) and the Spark image
├── scripts/              # environment smoke test
├── docs/                 # requirements (01–06), benchmark and ADRs
└── .github/workflows/    # CI and the manual Download workflow
```

## Roadmap

| Epic | Deliverable | Hours | Status |
|---|---|---|---|
| 1. Foundations | Repository, Docker environment, vision and requirements | 6 h | ✅ |
| 2. Bronze ingestion | Parameterized download, schemas, Delta with metadata | 8 h | 🚧 |
| 3. Silver and quality | Normalization, deduplication, tests, quality checks | 10 h | 🚧 |
| 4. Incremental and history | MERGE, idempotency, time travel | 7 h | ⬜ |
| 5. Gold and analysis | Aggregations, reconciliation, notebook | 5 h | ⬜ |
| 6. Optimization and benchmark | Baseline, tuning, OPTIMIZE/VACUUM, `benchmark.md` | 9 h | ⬜ |
| 7. Publication | CI, diagrams, ADRs, traceability, final README | 5 h | ⬜ |

## Measured results

_To be published at the end of epic 6 in [`docs/benchmark.md`](docs/benchmark.md), including the actual result even if the 30% target is not reached._

| Metric | Baseline | After tuning | Change |
|---|---|---|---|
| Main job runtime | — | — | — |
| Shuffle | — | — | — |
| Spill | — | — | — |
| Files per partition | — | — | — |

## Documentation

The project follows a requirements engineering process inspired by ISO/IEC/IEEE 29148:

| Document | Content |
|---|---|
| [01 - Vision and scope](docs/01-vision-and-scope.md) | Problem, goal, scope and success metrics |
| [02 - Analysis and prioritization](docs/02-analysis-and-prioritization.md) | Requirements (MoSCoW), rules, constraints and risks |
| [03 - Specification](docs/03-specification.md) | User stories in Gherkin and data model |
| [04 - Architecture](docs/04-architecture.md) | Simplified C4 |
| [05 - Validation](docs/05-validation.md) | Acceptance criteria execution |
| [06 - Traceability](docs/06-traceability.md) | Requirement → component → test |
| [Data source](docs/data-source.md) | Where the CNPJ data lives, server behaviour and release layout |
| [Data dictionary](docs/data-dictionary.md) | Tables, columns, target types and CSV format of the CNPJ open data |
| [ADRs](docs/adr/) | Architecture decisions with alternatives and consequences |

## Data and LGPD

The data is public and downloaded straight from the official source at run time: **no raw data is committed** to this repository. Partner CPFs (individual taxpayer IDs) already come partially masked by the Receita Federal, and using the project to try to re-identify people is forbidden. Details in [`USAGE-POLICY.md`](USAGE-POLICY.md).

## Related projects

This is the first of three data engineering projects developed in sequence:

| # | Project | Focus |
|---|---|---|
| **P2** | **spark-cnpj-lakehouse** (this one) | Distributed processing with Spark and Delta Lake |
| P1 | [aws-fundos-pipeline](https://github.com/thalesreiser/aws-fundos-pipeline) | AWS pipeline with Terraform, Airflow and CI/CD |
| P3 | [dbt-dw-api](https://github.com/thalesreiser/dbt-dw-api) | Data warehouse with dbt, data quality and a REST API |

## License

Code under the [MIT](LICENSE) license: it may be used, copied and modified, including commercially, as long as the copyright notice of **Thales Reiser Pedreira** is kept. Rules about data and security are in [`USAGE-POLICY.md`](USAGE-POLICY.md).

## Author

**Thales Reiser Pedreira**, Data and Software Engineer · Python, SQL, AWS and Big Data.
[GitHub](https://github.com/thalesreiser) · [LinkedIn](https://www.linkedin.com/in/thalesreiserpedreira)
