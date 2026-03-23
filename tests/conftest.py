import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests"""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ipl-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
