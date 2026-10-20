"""RNF-05: single source of truth for the pinned environment versions.

The Python versions of `pyspark` and `delta-spark` also appear in `pyproject.toml`, and the
Python version in the `infra/spark/Dockerfile` image; `tests/test_versions.py` keeps all of
them consistent with this module.
"""

from __future__ import annotations

from dataclasses import dataclass

PYTHON_VERSION = "3.11"
SPARK_VERSION = "3.5.8"
SCALA_BINARY_VERSION = "2.12"
DELTA_VERSION = "3.2.1"
# Hadoop version bundled with PySpark 3.5.x (hadoop-client-*-3.3.4 jars).
HADOOP_VERSION = "3.3.4"
# AWS SDK v1 version required by hadoop-aws 3.3.4 (aws-java-sdk.version property).
AWS_SDK_BUNDLE_VERSION = "1.12.262"

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"


@dataclass(frozen=True)
class MavenArtifact:
    """Maven coordinates of a jar required by Spark (Delta Lake and s3a)."""

    group: str
    artifact: str
    version: str

    @property
    def coordinate(self) -> str:
        return f"{self.group}:{self.artifact}:{self.version}"

    @property
    def jar_name(self) -> str:
        return f"{self.artifact}-{self.version}.jar"

    @property
    def url(self) -> str:
        group_path = self.group.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{group_path}/{self.artifact}/{self.version}/{self.jar_name}"


# Jars baked into the Spark image (complete list, no transitive resolution).
JARS: tuple[MavenArtifact, ...] = (
    MavenArtifact("io.delta", f"delta-spark_{SCALA_BINARY_VERSION}", DELTA_VERSION),
    MavenArtifact("io.delta", "delta-storage", DELTA_VERSION),
    MavenArtifact("org.apache.hadoop", "hadoop-aws", HADOOP_VERSION),
    MavenArtifact("com.amazonaws", "aws-java-sdk-bundle", AWS_SDK_BUNDLE_VERSION),
)

# Packages for `spark.jars.packages` when the jars are not preinstalled (running outside the
# container). Ivy resolves the transitive dependencies (delta-storage, aws-java-sdk-bundle).
SPARK_PACKAGES: tuple[str, ...] = (JARS[0].coordinate, JARS[2].coordinate)
