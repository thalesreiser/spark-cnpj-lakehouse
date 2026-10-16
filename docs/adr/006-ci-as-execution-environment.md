# 006 - GitHub Actions as the execution environment

- **Status:** accepted
- **Date:** 2026-10-16

## Context

The development laptop has about 10 GB of free disk and 7.7 GB of RAM. That is not enough to run Docker Desktop, the Spark + MinIO stack and the CNPJ dataset (several GB compressed, considerably more once unzipped), and the plan assumed 16 GB of RAM (RNF-03). GitHub Codespaces was evaluated and dropped in favour of a simpler setup with no extra environment to manage.

GitHub-hosted runners (`ubuntu-latest`, checked on 2026-10-16) offer, for **private** repositories, 2 CPUs, 8 GB of RAM and 14 GB of SSD, with 2,000 minutes per month on the Free plan (usage is blocked, not billed, when the quota runs out and no payment method is on file). For **public** repositories they offer 4 CPUs, 16 GB of RAM and 14 GB of SSD, with unlimited minutes.

## Decision

Split the work between the laptop and CI:

1. **Laptop:** editing, `make lint`, `make test` and unit tests with small DataFrames (Python 3.11 and Java 17 are installed locally; Spark in local mode with tiny inputs fits in memory). No Docker is required locally.
2. **CI on every pull request:** lint, type-check, unit tests and the `smoke` job, which builds the Spark image, brings up the whole Docker Compose stack and writes/reads a Delta table on MinIO.
3. **Manually triggered workflows** (`workflow_dispatch`) for heavy runs, added as the pipeline grows: downloading a reference month, running bronze → silver → gold on a sample (`SPARK_SAMPLE_FRACTION`), and the benchmark. Reports and Spark event logs are uploaded as artifacts.

## Alternatives considered

- **Docker Desktop on the laptop:** requires administrator rights, WSL 2 and 3–4 GB of disk before any data, on a machine with 10 GB free and 7.7 GB of RAM.
- **GitHub Codespaces:** a full cloud dev environment, but adds another environment to maintain and its own quota; dropped in favour of CI only.
- **A cloud VM (e.g. EC2):** flexible, but has a running cost and adds operational work.

## Consequences

- The environment is always proven from a clean machine, which strengthens the reproducibility claim (RNF-05).
- The feedback loop for integration issues is slower (a CI run instead of a local command).
- The full dataset does not fit on a 14 GB runner: runs use sampling (RNF-03); preinstalled runner tools that the project does not need can be removed to free disk space.
- Runner performance varies between runs, so the benchmark (epic 6) repeats each measurement 3 times, reports the median and records the runner specification. Spark UI evidence comes from event logs uploaded as artifacts and opened in a local Spark History Server.
- Making the repository public (planned for publication) doubles the runner size and removes the minutes limit.
- The README keeps the instructions to run the stack locally with Docker for anyone who has the resources.
