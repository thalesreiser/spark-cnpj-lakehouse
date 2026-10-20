# 005 - Docker environment images and driver execution

- **Status:** accepted
- **Date:** 2026-10-16

## Context

RNF-02 requires the complete environment (Spark master and workers + MinIO) via Docker Compose, with no paid cloud, and RNF-05 requires pinned Spark, Delta and Python versions. The initial draft used `bitnami/spark:3.5.3` and `minio/minio`, but while implementing task 1.2 we checked the registries (Docker Hub API, on 2026-10-16):

- `bitnami/spark` no longer exists in the public catalog (Bitnami moved the old images to `bitnamilegacy/*`, frozen and no longer updated).
- `minio/minio` and `minio/mc` are no longer available on Docker Hub nor publicly on quay.io (MinIO stopped distributing community edition images).
- The official `apache/spark:3.5.x-python3` images use Ubuntu 22.04 with **Python 3.10**, while the project pins **Python 3.11** (`pyproject.toml`).

In addition, PySpark requires the driver and the executors to run the same Python *major.minor* version, and the Delta and `hadoop-aws` jars must match the Hadoop version bundled with Spark (3.3.4 for Spark 3.5). The development machine has 7.7 GB of RAM (the plan assumed 16 GB).

## Decision

1. **Project Spark image** (`infra/spark/Dockerfile`): `python:3.11.17-slim-bookworm` + OpenJDK 17 + `pip install .` (which installs `pyspark==3.5.3` and `delta-spark==3.2.1`, pinned in `pyproject.toml`). The `delta-spark_2.12` and `delta-storage` 3.2.1, `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262 jars are downloaded from Maven Central at build time and verified by SHA-1; the build fails if the bundled Hadoop is not the expected one. The same image runs the **master**, the **worker** and the **driver**.
2. **Driver in a container** (`spark-client`, `client` profile), on the compose network, with the repository mounted at `/opt/app` and `--master spark://spark-master:7077`. It advertises the container IP as `spark.driver.host` so the executors can connect back to it. `make smoke` (and, in later tasks, the pipeline) runs through it.
3. **MinIO from the maintained community fork `pgsty/minio`** (with `pgsty/mc` to create the bucket), with exact `RELEASE.*` tags. It is the same server/client binary (same CLI and S3 API). The images can be swapped without touching the code through the `MINIO_IMAGE` and `MC_IMAGE` variables in `.env`.
4. **Versions in a single place:** `src/cnpj_lakehouse/versions.py`. The `tests/test_versions.py` test keeps `pyproject.toml`, the `Dockerfile` and `docker-compose.yml` consistent with it.
5. **Modest default resources:** 1 worker with 2 cores and 2 GB, 1 GB executors, adjustable in `.env`.

## Alternatives considered

- **Official `apache/spark:3.5.x-python3`:** maintained by Apache, but ships Python 3.10; using it would mean downgrading the project to 3.10 or installing another Python in the image, losing the benefit of a ready-made image.
- **`bitnamilegacy/spark` and `bitnamilegacy/minio`:** they exist, but have been frozen since 2025, with no security fixes.
- **Driver on the (Windows) host with a local `pyspark`:** would require Python 3.11 + Java 17 installed and, even then, a host ↔ containers round-trip network for the executors, which is fragile on Docker Desktop.
- **Other S3-compatible stores (SeaweedFS, RustFS, Garage):** viable, but they depart from the MinIO specified in RNF-02 and in the plan.
- **`spark.jars.packages` at run time:** would download the jars on every session (Ivy), depending on the network; kept only as a fallback for runs outside the image (`LAKEHOUSE_JARS_PREINSTALLED=false`).

## Consequences

- The driver and the executors use exactly the same Python, Spark, Delta and Hadoop, removing a whole class of version mismatch errors.
- The first `make up` builds the image (a few minutes and about 1 GB); later runs use the cache.
- Upgrading Spark/Delta requires changing `versions.py` and `pyproject.toml` together (the test flags any mismatch) and rebuilding the image.
- We depend on a community fork of MinIO; if it stops being maintained, point `MINIO_IMAGE`/`MC_IMAGE` to another compatible image.
- The worker's fixed hostname (`spark-worker`) limits the environment to one worker; scaling out requires adjusting the compose file.

## Update 2026-10-20: PySpark 3.5.8

PySpark was bumped from 3.5.3 to 3.5.8 to fix CVE-2026-32773 (and CVE-2025-54920), which affect `pyspark` before 3.5.8. The image design is unchanged: Spark still comes from `pip install .` and the image tag follows `SPARK_VERSION` (`cnpj-lakehouse/spark:3.5.8`). PySpark 3.5.8 still bundles Hadoop 3.3.4, so `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262 stay. `delta-spark` 3.2.1 declares `pyspark>=3.5.3,<3.6.0` and targets Spark 3.5.x, so it stays too.

Update 2026-10-20 (later): patch bump to PySpark 3.5.9 (Dependabot, same Hadoop 3.3.4 / Scala 2.12 line; the image build checks the bundled Hadoop version).
