import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ETLTest")
        .getOrCreate()
    )

def test_raw_tables_exist(spark):
    ##Debugging only
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    print(f"Available catalogs: {catalogs}")
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    print(f"Current catalog: {current_catalog}")

    spark.sql("USE CATALOG assignment")
    tables = spark.sql("SHOW TABLES IN raw").select("tableName").rdd.flatMap(lambda x: x).collect()
    for expected in ["airports", "countries", "runways"]:
        assert expected in tables, f"{expected} table is missing from assignment.raw"


def test_curated_views_exist(spark):
    views = spark.sql("SHOW TABLES IN curated").select("tableName").rdd.flatMap(lambda x: x).collect()
    assert "airport_runway_country" in views
    assert "airport_counts_per_country" in views

