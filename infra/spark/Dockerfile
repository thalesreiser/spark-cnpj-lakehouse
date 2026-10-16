# RNF-02 / RNF-05: single image for the Spark master, worker and client (driver).
# Python 3.11 base + Java 17 + PySpark from pip, so the driver and executors use exactly the
# same Python, Spark, Delta and hadoop-aws versions (see docs/adr/005).
ARG PYTHON_IMAGE=python:3.11.17-slim-bookworm
FROM ${PYTHON_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    # architecture-independent JAVA_HOME (amd64/arm64)
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_WORKER_DIR=/tmp/spark-work \
    LAKEHOUSE_JARS_PREINSTALLED=true \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /opt/app

# Dependencies pinned in pyproject.toml (pyspark, delta-spark, ...).
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Delta Lake and s3a jars (versions in src/cnpj_lakehouse/versions.py), with SHA-1 verification.
COPY infra/spark/download_jars.py /tmp/download_jars.py
RUN python /tmp/download_jars.py "${SPARK_HOME}/jars" && rm /tmp/download_jars.py

CMD ["bash"]
