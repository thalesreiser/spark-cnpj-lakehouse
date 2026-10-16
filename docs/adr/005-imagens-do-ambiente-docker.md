# 005 - Imagens do ambiente Docker e execução do driver

- **Status:** aceito
- **Data:** 2026-10-16

## Contexto

O RNF-02 pede o ambiente completo (Spark master e workers + MinIO) via Docker Compose, sem nuvem paga, e o RNF-05 pede versões fixadas de Spark, Delta e Python. O esboço inicial usava `bitnami/spark:3.5.3` e `minio/minio`, mas ao implementar a tarefa 1.2 verificamos no registro (API do Docker Hub, em 2026-10-16):

- `bitnami/spark` não existe mais no catálogo público (a Bitnami moveu as imagens antigas para `bitnamilegacy/*`, congeladas e sem atualizações).
- `minio/minio` e `minio/mc` não estão mais disponíveis no Docker Hub nem publicamente no quay.io (o MinIO deixou de distribuir imagens da edição comunitária).
- As imagens oficiais `apache/spark:3.5.x-python3` usam Ubuntu 22.04 com **Python 3.10**, enquanto o projeto fixa **Python 3.11** (`pyproject.toml`).

Além disso, o PySpark exige que driver e executores rodem a mesma versão *major.minor* do Python, e os jars de Delta e `hadoop-aws` precisam bater com o Hadoop embarcado no Spark (3.3.4 no Spark 3.5). A máquina de desenvolvimento tem 7,7 GB de RAM (o plano supunha 16 GB).

## Decisão

1. **Imagem Spark própria** (`infra/spark/Dockerfile`): `python:3.11.17-slim-bookworm` + OpenJDK 17 + `pip install .` (que instala `pyspark==3.5.3` e `delta-spark==3.2.1` fixados no `pyproject.toml`). Os jars `delta-spark_2.12`, `delta-storage` 3.2.1, `hadoop-aws` 3.3.4 e `aws-java-sdk-bundle` 1.12.262 são baixados do Maven Central no build e conferidos por SHA-1; o build falha se o Hadoop embarcado não for o esperado. A mesma imagem roda o **master**, o **worker** e o **driver**.
2. **Driver em container** (`spark-client`, perfil `cliente`), na rede do compose, com o repositório montado em `/opt/app` e `--master spark://spark-master:7077`. Ele anuncia o IP do container como `spark.driver.host` para os executores conseguirem se conectar. `make smoke` (e, nas próximas tarefas, o pipeline) roda por ele.
3. **MinIO pelo fork comunitário mantido `pgsty/minio`** (com `pgsty/mc` para criar o bucket), com tags `RELEASE.*` exatas. É binariamente o mesmo servidor/cliente (mesma CLI e API S3). As imagens podem ser trocadas sem mexer no código pelas variáveis `MINIO_IMAGE` e `MC_IMAGE` no `.env`.
4. **Versões em um só lugar:** `src/cnpj_lakehouse/versoes.py`. O teste `tests/test_versoes.py` garante que `pyproject.toml`, o `Dockerfile` e o `docker-compose.yml` continuam coerentes com ele.
5. **Recursos modestos por padrão:** 1 worker com 2 cores e 2 GB, executores de 1 GB, ajustáveis no `.env`.

## Alternativas consideradas

- **`apache/spark:3.5.x-python3` oficial:** mantida pela Apache, mas traz Python 3.10; usá-la obrigaria rebaixar o projeto para 3.10 ou instalar outro Python na imagem, perdendo a vantagem de usar a imagem pronta.
- **`bitnamilegacy/spark` e `bitnamilegacy/minio`:** existem, mas estão congeladas desde 2025 e sem correções de segurança.
- **Driver no host (Windows) com `pyspark` local:** exigiria Python 3.11 + Java 17 instalados e, ainda assim, rede de ida e volta host ↔ containers para os executores, o que é frágil no Docker Desktop.
- **Outros S3 compatíveis (SeaweedFS, RustFS, Garage):** viáveis, mas fogem do MinIO previsto no RNF-02 e no plano.
- **`spark.jars.packages` em tempo de execução:** baixaria os jars a cada sessão (Ivy), dependente de rede; mantido apenas como fallback para execuções fora da imagem (`LAKEHOUSE_JARS_PREINSTALADOS=false`).

## Consequências

- Driver e executores usam exatamente o mesmo Python, Spark, Delta e Hadoop, eliminando a classe de erros de incompatibilidade de versões.
- O primeiro `make up` constrói a imagem (alguns minutos e ~1 GB); os seguintes usam o cache.
- Atualizar Spark/Delta exige mudar `versoes.py` e `pyproject.toml` juntos (o teste aponta divergências) e reconstruir a imagem.
- Dependemos de um fork comunitário do MinIO; se ele parar de ser mantido, basta apontar `MINIO_IMAGE`/`MC_IMAGE` para outra imagem compatível.
- O hostname fixo do worker (`spark-worker`) limita o ambiente a um worker; escalar exige ajustar o compose.
