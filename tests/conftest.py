"""Shared test fixtures (RF-09).

One local SparkSession is started for the whole test run and reused by every Spark test module:
starting the JVM once instead of once per module keeps the suite fast. Tests never read the real
dataset: they build in-memory DataFrames or use the synthetic release in ``tests/fixtures``.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Local SparkSession shared by the whole run (no Delta packages, no network).

    Two cores and a few shuffle partitions, so window and aggregation tests really exercise more
    than one partition; UTC session time zone so timestamps do not depend on the machine. Tests
    that change a runtime conf must restore it (see ``test_normalize.ansi``).
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cnpj-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()
