#!/usr/bin/env bash
# Runs once when the codespace (or local dev container) is created.
set -euo pipefail

command -v make >/dev/null || { sudo apt-get update -qq && sudo apt-get install -y -qq make; }

python -m pip install --quiet --upgrade pip
python -m pip install --quiet -e ".[dev]"
pre-commit install

# Local-development settings only (MinIO dev credentials, Spark resources). Never committed.
[ -f .env ] || cp .env.example .env

echo "Dev container ready. Next: make up && make smoke"
