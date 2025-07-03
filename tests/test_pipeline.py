import pytest
from pyspark.sql import SparkSession
from databricks import sql
import os

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("ETLTest")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def databricks_client():
    conn = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )
    yield conn
    conn.close()

def test_raw_tables_exist_spark(spark):

    tables_df = spark.sql("SHOW TABLES IN assignment.raw")
    tables = tables_df.select("tableName").rdd.flatMap(lambda x: x).collect()
    
    for expected in ["airports", "countries", "runways"]:
        assert expected in tables, f"{expected} table is missing from assignment.raw"

def test_curated_views_exist_spark(spark):
    views_df = spark.sql("SHOW TABLES IN assignment.curated")
    views = views_df.select("tableName").rdd.flatMap(lambda x: x).collect()
    
    assert "airport_runway_country" in views
    assert "airport_counts_per_country" in views

def test_raw_tables_exist_sql_connector(databricks_client):
    cursor = databricks_client.cursor()
    cursor.execute("SHOW TABLES IN assignment.raw")
    tables = [row[1] for row in cursor.fetchall()]  # tableName is in column 1
    
    for expected in ["airports", "countries", "runways"]:
        assert expected in tables, f"{expected} table is missing from assignment.raw"

def test_curated_views_exist_sql_connector(databricks_client):
    cursor = databricks_client.cursor()
    cursor.execute("SHOW TABLES IN assignment.curated")
    views = [row[1] for row in cursor.fetchall()]
    
    assert "airport_runway_country" in views
    assert "airport_counts_per_country" in views
