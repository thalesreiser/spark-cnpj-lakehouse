#!/usr/bin/env bash
# Entrypoint of the spark-client service (driver). Executors must reach the driver over the
# compose network; the hostname of a container created by `docker compose run` cannot be
# resolved by the other containers, so we advertise the container IP instead.
set -euo pipefail
export SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-$(hostname -i | awk '{print $1}')}"
exec "$@"
