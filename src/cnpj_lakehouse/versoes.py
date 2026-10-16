"""RNF-05: fonte única das versões fixadas do ambiente.

As versões Python de `pyspark` e `delta-spark` também aparecem no `pyproject.toml` e a versão
do Python na imagem `infra/spark/Dockerfile`; o teste `tests/test_versoes.py` garante que tudo
continua consistente com este módulo.
"""

from __future__ import annotations

from dataclasses import dataclass

PYTHON_VERSION = "3.11"
SPARK_VERSION = "3.5.3"
SCALA_BINARY_VERSION = "2.12"
DELTA_VERSION = "3.2.1"
# Versão do Hadoop embarcada no PySpark 3.5.x (jars hadoop-client-*-3.3.4).
HADOOP_VERSION = "3.3.4"
# Versão do AWS SDK v1 exigida pelo hadoop-aws 3.3.4 (propriedade aws-java-sdk.version).
AWS_SDK_BUNDLE_VERSION = "1.12.262"

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"


@dataclass(frozen=True)
class ArtefatoMaven:
    """Coordenada Maven de um jar necessário ao Spark (Delta Lake e s3a)."""

    grupo: str
    artefato: str
    versao: str

    @property
    def coordenada(self) -> str:
        return f"{self.grupo}:{self.artefato}:{self.versao}"

    @property
    def nome_jar(self) -> str:
        return f"{self.artefato}-{self.versao}.jar"

    @property
    def url(self) -> str:
        caminho_grupo = self.grupo.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{caminho_grupo}/{self.artefato}/{self.versao}/{self.nome_jar}"


# Jars embarcados na imagem Spark (lista completa, sem resolução transitiva).
JARS: tuple[ArtefatoMaven, ...] = (
    ArtefatoMaven("io.delta", f"delta-spark_{SCALA_BINARY_VERSION}", DELTA_VERSION),
    ArtefatoMaven("io.delta", "delta-storage", DELTA_VERSION),
    ArtefatoMaven("org.apache.hadoop", "hadoop-aws", HADOOP_VERSION),
    ArtefatoMaven("com.amazonaws", "aws-java-sdk-bundle", AWS_SDK_BUNDLE_VERSION),
)

# Pacotes para `spark.jars.packages` quando os jars não estão pré-instalados (execução fora
# do container). O Ivy resolve as dependências transitivas (delta-storage, aws-java-sdk-bundle).
PACOTES_SPARK: tuple[str, ...] = (JARS[0].coordenada, JARS[2].coordenada)
