# 06 - Matriz de rastreabilidade

| Requisito | Componente | Teste | Status |
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
| RNF-02 | `infra/docker-compose.yml`, `infra/spark/Dockerfile`, `cnpj_lakehouse/spark.py`, `Makefile` (`up`/`down`/`smoke`) | `scripts/smoke_delta.py` (job `smoke` do CI), `tests/test_spark.py` | ✅ |
| RNF-05 | `cnpj_lakehouse/versoes.py`, `pyproject.toml`, `infra/spark/Dockerfile` | `tests/test_versoes.py`, job `smoke` do CI | ✅ |

## Controle de mudanças
| Data | Mudança | Motivo | ADR |
|---|---|---|---|
| 2026-10-16 | Imagem Spark própria (Python 3.11) no lugar de `bitnami/spark`; MinIO pelo fork `pgsty/minio`; driver em container | Bitnami e MinIO oficiais indisponíveis no Docker Hub; `apache/spark` usa Python 3.10 | [005](adr/005-imagens-do-ambiente-docker.md) |
