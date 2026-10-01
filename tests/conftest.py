import pathlib
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src" / "pipeline"))


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("a2c-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.ansi.enabled", "true")   # serverless runs with ANSI on
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
