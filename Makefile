COMPETENCIA ?= $(shell date +%Y-%m)
COMPOSE = docker compose -f infra/docker-compose.yml

.PHONY: install up down download run lint test fmt

install:
	pip install -e ".[dev]" && pre-commit install

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

download:  ## RF-01
	python -m cnpj_lakehouse.download --competencia $(COMPETENCIA)

run:
	python -m cnpj_lakehouse.pipeline --competencia $(COMPETENCIA)

lint:
	ruff check src tests && black --check src tests && mypy src

fmt:
	ruff check --fix src tests && black src tests

test:
	pytest -m "not integration"
