import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ETLTest")
        .config("spark.sql.catalogImplementation", "in-memory")  # or "hive" or "unity" depending on your setup
        .config("spark.sql.catalog.assignment", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_raw_tables_exist(spark):
    tables = spark.sql("SHOW TABLES IN assignment.raw").select("tableName").rdd.flatMap(lambda x: x).collect()
    for expected in ["airports", "countries", "runways"]:
        assert expected in tables, f"{expected} table is missing from assignment.raw"


def test_curated_views_exist(spark):
    views = spark.sql("SHOW TABLES IN assignment.curated").select("tableName").rdd.flatMap(lambda x: x).collect()
    assert "airport_runway_country" in views
    assert "airport_counts_per_country" in views

