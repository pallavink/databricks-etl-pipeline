# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS assignment;
# MAGIC use catalog assignment;
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS curated;

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest CSV file data to raw tables

# COMMAND ----------

# Define URLs
airports_url = "https://sacodeassessment.blob.core.windows.net/public/airports.csv"
countries_url = "https://sacodeassessment.blob.core.windows.net/public/countries.csv"
runways_url = "https://sacodeassessment.blob.core.windows.net/public/runways.csv"

# Read CSVs directly from URL
df_airports = spark.read.option("header", True).csv(airports_url)
df_countries = spark.read.option("header", True).csv(countries_url)
df_runways = spark.read.option("header", True).csv(runways_url)

# Write to Delta tables in your chosen catalog.schema (assignment.raw)
df_airports.write.mode("overwrite").saveAsTable("assignment.raw.airports")
df_countries.write.mode("overwrite").saveAsTable("assignment.raw.countries")
df_runways.write.mode("overwrite").saveAsTable("assignment.raw.runways")


# COMMAND ----------

# MAGIC %md
# MAGIC Transform data

# COMMAND ----------


# Create a joined view to simplify queries
spark.sql("""
CREATE OR REPLACE VIEW assignment.curated.airport_runway_country AS
SELECT
  c.name AS country_name,
  a.name AS airport_name,
  r.length_ft,
  r.width_ft
FROM assignment.raw.runways r
JOIN assignment.raw.airports a ON r.airport_ref = a.id
JOIN assignment.raw.countries c ON a.iso_country = c.code
WHERE r.length_ft IS NOT NULL
""")

# Airport counts per country
spark.sql("""
CREATE OR REPLACE VIEW assignment.curated.airport_counts_per_country AS
SELECT c.name AS country_name, COUNT(a.id) AS airport_count
FROM assignment.raw.airports a
JOIN assignment.raw.countries c ON a.iso_country = c.code
GROUP BY c.name
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Answer Queries 
# MAGIC - For each country with airports, list the details (width, length, airport name) of its longest and shortest runways.
# MAGIC - Calculate the number of airports per country, identifying the top 3 and bottom 10 countries.

# COMMAND ----------


# Longest runway per country
spark.sql("""
SELECT country_name, airport_name, length_ft, width_ft
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY length_ft DESC) AS rn
  FROM assignment.curated.airport_runway_country
) WHERE rn = 1
""")

# Shortest runway per country
spark.sql("""
SELECT country_name, airport_name, length_ft, width_ft
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY length_ft ASC) AS rn
  FROM assignment.curated.airport_runway_country
) WHERE rn = 1
""")


# Top 3 countries that have the most airports
spark.sql("""
SELECT *
FROM assignment.curated.airport_counts_per_country
ORDER BY airport_count DESC
LIMIT 3;
""")


# Bottom 10 countries that have least number of airports
spark.sql("""
SELECT *
FROM assignment.curated.airport_counts_per_country
WHERE airport_count > 0
ORDER BY airport_count ASC
LIMIT 10;
""")


print("ETL pipeline completed successfully with SQL transformations.")