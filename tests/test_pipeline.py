import pytest
from pyspark.sql import SparkSession
from databricks import sql
import os

print("Before fixtures")

@pytest.fixture(scope="session")
def databricks_client():
    conn = sql.connect(
        server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path = os.environ["DATABRICKS_HTTP_PATH"],
        access_token = os.environ["DATABRICKS_TOKEN"]
    )
    print("Connected")
    yield conn
    conn.close()

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
