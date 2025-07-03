import pytest
from pyspark.sql import SparkSession
from databricks import sql
import os

print("Before fixtures")

@pytest.fixture(scope="session")
def databricks_client():
    print("HOST:", os.getenv("DATABRICKS_HOST"))
    print("HTTP PATH:", os.getenv("DATABRICKS_HTTP_PATH"))
    print("TOKEN:", "set" if os.getenv("DATABRICKS_TOKEN") else "NOT SET")
    conn = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
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
