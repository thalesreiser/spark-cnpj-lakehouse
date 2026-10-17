# COMPETENCIA = reference month of the Receita Federal release (YYYY-MM).
COMPETENCIA ?= $(shell date +%Y-%m)
# FILES = optional subset for `make download`: groups (empresas, estabelecimentos, socios,
# simples, lookups, all) and/or file names (e.g. FILES="lookups Empresas0").
# Default: lookups (a few KB); the whole release (~7.8 GB zipped) needs FILES=all explicitly.
FILES ?=
# TABLES = optional subset for `make bronze` (e.g. TABLES="empresas cnaes"); default: every table
# downloaded for COMPETENCIA (listed in data/raw/<COMPETENCIA>/manifest.json).
TABLES ?=
# Use the root .env if present (the compose file lives in infra/ and would not see it).
ENV_FILE := $(if $(wildcard .env),--env-file .env,)
COMPOSE = docker compose $(ENV_FILE) -f infra/docker-compose.yml
# Commands run in the driver (spark-client container), on the same image as the executors.
SPARK_RUN = $(COMPOSE) --profile client run --rm spark-client

.PHONY: install up down smoke logs ps download bronze bronze-check run lint test fmt

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

run:
	python -m cnpj_lakehouse.pipeline --competencia $(COMPETENCIA)

lint:
	ruff check src tests scripts infra && black --check src tests scripts infra && mypy src

fmt:
	ruff check --fix src tests scripts infra && black src tests scripts infra

test:
	pytest -m "not integration"
