# RNF-02 / RNF-05: imagem única para Spark master, worker e cliente (driver).
# Base Python 3.11 + Java 17 + PySpark do pip, para que driver e executores usem exatamente
# as mesmas versões de Python, Spark, Delta e hadoop-aws (ver docs/adr/005).
ARG PYTHON_IMAGE=python:3.11.17-slim-bookworm
FROM ${PYTHON_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    # JAVA_HOME independente da arquitetura (amd64/arm64)
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_WORKER_DIR=/tmp/spark-work \
    LAKEHOUSE_JARS_PREINSTALADOS=true \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /opt/app

# Dependências fixadas no pyproject.toml (pyspark, delta-spark, ...).
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

# Jars de Delta Lake e s3a (versões em src/cnpj_lakehouse/versoes.py), com verificação SHA-1.
COPY infra/spark/baixar_jars.py /tmp/baixar_jars.py
RUN python /tmp/baixar_jars.py "${SPARK_HOME}/jars" && rm /tmp/baixar_jars.py

CMD ["bash"]
