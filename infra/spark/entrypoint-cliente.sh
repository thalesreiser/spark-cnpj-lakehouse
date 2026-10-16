#!/usr/bin/env bash
# Entrypoint do serviço spark-client (driver). Os executores precisam alcançar o driver pela
# rede do compose; o hostname de um container criado por `docker compose run` não é resolvível
# pelos outros containers, então anunciamos o IP do container.
set -euo pipefail
export SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-$(hostname -i | awk '{print $1}')}"
exec "$@"
