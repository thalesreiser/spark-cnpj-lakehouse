# 06 - Traceability matrix

| Requirement | Component | Test | Status |
|---|---|---|---|
| RF-01 | `cnpj_lakehouse/download.py`, `Makefile` (`download`), `.github/workflows/download.yml` | `tests/test_download.py` (integration with a local HTTP server: files, manifest SHA-256, idempotency, resume with/without `Range`, retries), `Download` workflow (real source) | ✅ |
| RF-02 | `cnpj_lakehouse/bronze.py`, `cnpj_lakehouse/schemas.py` (explicit bronze schemas and CSV read options, [data dictionary](data-dictionary.md)) | `tests/test_schemas.py` (layout, bronze types, file mapping, local Spark read of a Latin-1 CSV) | 🚧 schemas done (task 2.2); ingestion pending |
| RF-03 | `cnpj_lakehouse/silver.py`, target types in `cnpj_lakehouse/schemas.py` | `tests/test_schemas.py` (target types) | ⬜ |
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
| 2026-10-16 | CNPJ data downloaded from the Receita Federal Nextcloud share through public WebDAV; base URL configurable | The source moved again (R-04): the old `dadosabertos.rfb.gov.br` and `arquivos.receitafederal.gov.br/dados/cnpj/...` URLs are gone | [007](adr/007-cnpj-source-nextcloud-webdav.md) |
| 2026-10-16 | Bronze reads every CSV column as a nullable string; the documented target types (date, decimal, array) are applied in silver; one column spec per table drives both | Keep bronze lossless (no rows or values lost to cast errors) while documenting types for RF-03 | [008](adr/008-bronze-all-strings-typed-in-silver.md) |
