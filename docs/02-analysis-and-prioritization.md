# 02 - Analysis and prioritization (MoSCoW)

## Functional requirements
| ID | Requirement | Prio. | Acceptance criterion | Verification |
|---|---|---|---|---|
| RF-01 | Download and unzip the files reproducibly, parameterized by reference month | M | Given a reference month, when I run `make download`, then the files are in `data/raw/<reference-month>/` with a recorded checksum | Integration test |
| RF-02 | Ingest the CSV files into the bronze layer (Delta) with an explicit schema and load metadata | M | Given a CSV, then the bronze table has every row plus the `_ingest_ts` and `_source_file` columns | Source × target row count |
| RF-03 | Build the typed, deduplicated and normalized silver layer (CNPJ, CNAE, dates) | M | Then there is no duplicated CNPJ on the business key and 100% of valid dates are typed as `date` | Unit tests and quality checks |
| RF-04 | Apply an incremental load with MERGE when a new reference month arrives | M | Given a new reference month, when I re-run, then changed records are updated and new ones inserted without duplicates | Idempotency test |
| RF-05 | Produce gold tables: companies by CNAE/state/year, company size, registration status | M | Then gold totals match silver (reconciliation) | Reconciliation test |
| RF-06 | Demonstrate time travel by querying the previous version of a table | S | Then I can reproduce the result of a query on version N-1 | Documented notebook |
| RF-07 | Compact small files and clean up old versions (OPTIMIZE / VACUUM) | S | Then the number of files per partition drops and read time is measured before and after | Benchmark |
| RF-08 | Record a before/after benchmark of the optimizations (time, shuffle, spill, number of files) | M | Then `docs/benchmark.md` exists with a comparison table and Spark UI evidence | Review |
| RF-09 | Unit tests of the transformations with small DataFrames | M | Then `pytest` runs without accessing the full dataset | CI |
| RF-10 | Link partners with the same name by similarity (simplified record linkage) | C | Then candidate pairs are generated with a score and a sample review | Manual sample |

## Non-functional requirements, rules and constraints
| ID | Type | Description |
|---|---|---|
| RNF-01 | Performance | At least 30% reduction in the main job runtime after tuning (target) |
| RNF-02 | Portability | Complete environment via Docker Compose (Spark master and workers, MinIO), with no paid cloud dependency |
| RNF-03 | Scalability | Sampling parameter to run with 16 GB of RAM, and a full run when resources allow |
| RNF-04 | Maintainability | Transformations as pure, testable functions, separated from orchestration |
| RNF-05 | Reproducibility | Pinned Spark, Delta and Python versions; `make up && make run` |
| RN-01 | Business rule | The establishment business key is base CNPJ + branch order + check digits |
| RST-01 | Constraint (LGPD) | Public data, with partner CPFs partially masked by the source. Re-identifying people is forbidden, and raw data is never republished in the repository |

## Risks
| ID | Risk | Prob. | Impact | Mitigation |
|---|---|---|---|---|
| R-01 | Scope grows and the project is never finished | High | High | Strict MoSCoW; "Could" only after all "Must" items |
| R-03 | Lack of time | High | High | Incremental delivery: every epic produces something publishable |
| R-04 | Open data changes format or URL | Medium | Medium | Parameterized download and a versioned sample for tests |
| R-P2-01 | Data volume exceeds the available memory or disk | — | — | Parameterized sampling; process per reference month and per partition; heavy runs in CI (ADR 006) |
| R-P2-02 | The 30% gain is not reached | — | — | Record the actual result and the diagnosis |
