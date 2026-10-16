# 06 - Traceability matrix

| Requirement | Component | Test | Status |
|---|---|---|---|
| RF-01 | `cnpj_lakehouse/download.py` | | ⬜ |
| RF-02 | `cnpj_lakehouse/bronze.py` | | ⬜ |
| RF-03 | `cnpj_lakehouse/silver.py` | | ⬜ |
| RF-04 | `cnpj_lakehouse/silver.py` | | ⬜ |
| RF-05 | `cnpj_lakehouse/gold.py` | | ⬜ |
| RF-06 | `notebooks/` | | ⬜ |
| RF-07 | | | ⬜ |
| RF-08 | `docs/benchmark.md` | | ⬜ |
| RF-09 | `tests/` | | ⬜ |
| RF-10 | | | ⬜ |
| RNF-01 | `docs/benchmark.md` | | ⬜ |
| RNF-02 | `infra/docker-compose.yml`, `infra/spark/Dockerfile`, `cnpj_lakehouse/spark.py`, `Makefile` (`up`/`down`/`smoke`) | `scripts/smoke_delta.py` (CI `smoke` job), `tests/test_spark.py` | ✅ |
| RNF-05 | `cnpj_lakehouse/versions.py`, `pyproject.toml`, `infra/spark/Dockerfile` | `tests/test_versions.py`, CI `smoke` job | ✅ |

## Change log
| Date | Change | Reason | ADR |
|---|---|---|---|
| 2026-10-16 | Project Spark image (Python 3.11) instead of `bitnami/spark`; MinIO from the `pgsty/minio` fork; driver in a container | Official Bitnami and MinIO images unavailable on Docker Hub; `apache/spark` ships Python 3.10 | [005](adr/005-docker-environment-images.md) |
| 2026-10-16 | Repository documentation and code translated to English | Git/GitHub artifacts are written in English | — |
| 2026-10-16 | GitHub Actions is the execution environment for the Docker stack and heavy runs | The development laptop lacks the disk and RAM to run the stack and datasets locally | [006](adr/006-ci-as-execution-environment.md) |
