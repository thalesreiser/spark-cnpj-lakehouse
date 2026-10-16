COMPETENCIA ?= $(shell date +%Y-%m)
# Usa o .env da raiz, se existir (o compose fica em infra/ e não o enxergaria sozinho).
ENV_FILE := $(if $(wildcard .env),--env-file .env,)
COMPOSE = docker compose $(ENV_FILE) -f infra/docker-compose.yml
# Comandos executados no driver (container spark-client), na mesma imagem dos executores.
SPARK_RUN = $(COMPOSE) --profile cliente run --rm spark-client

.PHONY: install up down smoke logs ps download run lint test fmt

install:
	pip install -e ".[dev]" && pre-commit install

up:  ## RNF-02: sobe MinIO (+ bucket) e Spark master/worker e espera os healthchecks
	$(COMPOSE) up -d --build --wait --wait-timeout 300 minio spark-master spark-worker
	$(COMPOSE) run --rm minio-init

down:  ## derruba o ambiente (mantém o volume do MinIO; use `make down V=1` para apagar)
	$(COMPOSE) --profile cliente down --remove-orphans $(if $(V),--volumes,)

smoke:  ## RNF-02: grava e lê uma tabela Delta no MinIO (s3a) pelo cluster Spark
	$(SPARK_RUN) python scripts/smoke_delta.py

logs:
	$(COMPOSE) logs --no-color --tail=200

ps:
	$(COMPOSE) ps -a

download:  ## RF-01
	python -m cnpj_lakehouse.download --competencia $(COMPETENCIA)

run:
	python -m cnpj_lakehouse.pipeline --competencia $(COMPETENCIA)

lint:
	ruff check src tests scripts infra && black --check src tests scripts infra && mypy src

fmt:
	ruff check --fix src tests scripts infra && black src tests scripts infra

test:
	pytest -m "not integration"
