# COMPETENCIA = reference month of the Receita Federal release (YYYY-MM).
COMPETENCIA ?= $(shell date +%Y-%m)
# FILES = optional subset for `make download`: groups (empresas, estabelecimentos, socios,
# simples, lookups, all) and/or file names (e.g. FILES="lookups Empresas0").
# Default: lookups (a few KB); the whole release (~7.8 GB zipped) needs FILES=all explicitly.
FILES ?=
# TABLES = optional subset for `make bronze` (e.g. TABLES="empresas cnaes"); default: every table
# downloaded for COMPETENCIA (listed in data/raw/<COMPETENCIA>/manifest.json). `make silver` uses
# it too (default: every bronze table with rows for COMPETENCIA).
TABLES ?=
# Use the root .env if present (the compose file lives in infra/ and would not see it).
ENV_FILE := $(if $(wildcard .env),--env-file .env,)
COMPOSE = docker compose $(ENV_FILE) -f infra/docker-compose.yml
# Commands run in the driver (spark-client container), on the same image as the executors.
SPARK_RUN = $(COMPOSE) --profile client run --rm spark-client

.PHONY: install up down smoke logs ps download bronze bronze-check silver silver-check quality gold reconcile run lint test fmt

install:
	pip install -e ".[dev]" && pre-commit install

up:  ## RNF-02: starts MinIO (+ bucket) and the Spark master/worker, waits for healthchecks
	$(COMPOSE) up -d --build --wait --wait-timeout 300 minio spark-master spark-worker
	$(COMPOSE) run --rm minio-init

down:  ## stops the environment (keeps the MinIO volume; `make down V=1` deletes it)
	$(COMPOSE) --profile client down --remove-orphans $(if $(V),--volumes,)

smoke:  ## RNF-02: writes and reads a Delta table on MinIO (s3a) through the Spark cluster
	$(SPARK_RUN) python scripts/smoke_delta.py

logs:
	$(COMPOSE) logs --no-color --tail=200

ps:
	$(COMPOSE) ps -a

download:  ## RF-01: downloads, unzips and checksums a release into data/raw/<COMPETENCIA>/
	python -m cnpj_lakehouse.download --competencia $(COMPETENCIA) $(if $(strip $(FILES)),--files $(FILES),)

bronze:  ## RF-02: ingests data/raw/<COMPETENCIA>/ into s3a://<bucket>/bronze/<table>/ (Delta)
	$(SPARK_RUN) python -m cnpj_lakehouse.bronze --competencia $(COMPETENCIA) \
		$(if $(strip $(TABLES)),--tables $(TABLES),) --summary build/bronze-$(COMPETENCIA).json

bronze-check:  ## RF-02 proof: source line counts == bronze row counts, metadata columns filled
	$(SPARK_RUN) python scripts/bronze_check.py --competencia $(COMPETENCIA) \
		$(if $(strip $(TABLES)),--tables $(TABLES),) --out build/bronze-check

silver:  ## RF-03: bronze -> typed, deduplicated s3a://<bucket>/silver/<table>/ (Delta)
	$(SPARK_RUN) python -m cnpj_lakehouse.silver --competencia $(COMPETENCIA) 		$(if $(strip $(TABLES)),--tables $(TABLES),) --summary build/silver-$(COMPETENCIA).json

silver-check:  ## RF-03 proof: no duplicated business key, date columns typed, counts reconciled
	$(SPARK_RUN) python scripts/silver_check.py --competencia $(COMPETENCIA) 		--summary build/silver-$(COMPETENCIA).json --out build/silver-check

quality:  ## RF-03: quality checks of silver (nulls, domains, uniqueness); fails on `error` rules
	$(SPARK_RUN) python -m cnpj_lakehouse.quality --competencia $(COMPETENCIA) \
		$(if $(strip $(TABLES)),--tables $(TABLES),) --report build/quality-$(COMPETENCIA)

gold:  ## RF-05: silver current state -> s3a://<bucket>/gold/<table>/ (Delta overwrite); sums reconciled
	$(SPARK_RUN) python -m cnpj_lakehouse.gold --competencia $(COMPETENCIA) --summary build/gold-$(COMPETENCIA).json

reconcile:  ## RF-05 proof: gold totals (total, by UF, status, size, gold grain) == silver recomputed
	$(SPARK_RUN) python -m cnpj_lakehouse.reconcile --competencia $(COMPETENCIA) --out build/reconcile

run:
	python -m cnpj_lakehouse.pipeline --competencia $(COMPETENCIA)

lint:
	ruff check src tests scripts infra && black --check src tests scripts infra && mypy src scripts infra

fmt:
	ruff check --fix src tests scripts infra && black src tests scripts infra

# RF-09: the suite fails below 80% line coverage, both for the whole package and for the
# transformation modules alone (CI runs `make test` on Linux, where no test is skipped). The Delta
# tests need a fresh JVM, so they run in a second pytest whose coverage is appended (--cov-append).
COVERAGE_MIN ?= 80
TRANSFORMS = */cnpj_lakehouse/normalize.py,*/cnpj_lakehouse/bronze.py,*/cnpj_lakehouse/silver.py,*/cnpj_lakehouse/schemas.py,*/cnpj_lakehouse/gold.py,*/cnpj_lakehouse/reconcile.py

test:
	pytest -m "not integration and not delta" --cov-fail-under=$(COVERAGE_MIN)
	pytest -m "delta and not integration" --cov-append
	coverage report --include="$(TRANSFORMS)" --fail-under=$(COVERAGE_MIN)

.PHONY: time-travel
# RF-06: DESCRIBE HISTORY, versionAsOf N-1 vs N and timestampAsOf on silver/<TABLE> (default
# empresas); TT_ARGS e.g. "--snapshot build/x.json" or "--expected build/x.json" (ADR 011).
TABLE ?= empresas
time-travel:  ## RF-06 proof: version N-1 reproduces the recorded result (aggregates only)
	$(SPARK_RUN) python -m cnpj_lakehouse.time_travel --table $(TABLE) --out build/time-travel $(TT_ARGS)

.PHONY: benchmark
# RF-08 / RNF-01 (task 6.1): runs the main job (silver MERGE + gold) N times with Spark event logs
# and records the median time, shuffle, spill and files in build/benchmark (docs/benchmark.md).
# Needs `make bronze` and scripts/silver_next_month.py first; BENCH_ARGS e.g.
# "--next 2026-10 --merge-tables empresas --runs 3" (the Benchmark workflow runs it end to end).
benchmark:  ## RF-08 baseline: main job N times, median metrics and event logs in build/benchmark
	$(SPARK_RUN) python scripts/benchmark.py run --competencia $(COMPETENCIA) $(BENCH_ARGS)
