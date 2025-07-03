# Databricks ETL Pipeline Assignment

## Overview

This project implements a data pipeline to ingest, transform, and analyze airport-related datasets in a Databricks data warehouse environment. The pipeline is automated to run daily using Databricks Jobs, with CI/CD configured via GitHub Actions.

---

## Project Structure

- **notebooks/etl_pipeline.py** Main Databricks notebook that:
  - Creates catalog and schemas
  - Ingests CSV files from public URLs into raw Delta tables
  - Transforms data with SQL views in curated schemas
  - Answers business questions via SQL queries

- **.databricks/config/job-config.json** Job configuration to create and schedule the Databricks job with a new cluster.

- **requirements.txt** Python dependencies for testing and deployment (`pytest`, `pyspark`, `databricks-cli`).

- **tests/test_pipeline.py** Pytest-based test suite verifying existence of expected raw tables and curated views.

- **.github/workflows/databricks-etl.yml** GitHub Actions workflow automating CI/CD:
  - Installs dependencies
  - Configures Databricks CLI with secrets
  - Deploys notebook to workspace
  - Creates or updates Databricks job
  - Runs ETL job and tests


## Setup Instructions

1. **Databricks Workspace**
   - Use a Databricks workspace (Community Edition or paid).
   - Verify supported Spark version and node types for job clusters.
   - Generate a personal access token for CLI authentication.

2. **GitHub Secrets**
   - Add the following repository secrets in GitHub:
     - `DATABRICKS_HOST` Your Databricks workspace URL.
     - `DATABRICKS_TOKEN` Your Databricks personal access token.

3. **Local Testing**
   - Install dependencies locally:
     ```bash
     pip install -r requirements.txt
     ```
   - Run Pytest to verify tests:
     ```bash
     pytest tests/
     ```

4. **Pipeline Execution**
   - The ETL pipeline ingests data from public CSV URLs.
   - Writes raw data into Delta tables under `assignment.raw` schema.
   - Creates SQL views in `assignment.curated` schema to enable business queries.
   - Answers key questions on runways and airport counts by country.

5. **Automation**
   - The pipeline is scheduled to run daily at 06:00 UTC using Databricks Job and the schedule is taken care at the cicd end while making the job using job-config.json
   - GitHub Actions workflow triggers on every push to `main`, deploying code and running tests automatically.


## Assumptions & Notes

- CSV data schemas are assumed consistent with source URLs.
- `new_cluster` is used in the job config for Community Edition compatibility.
- Basic validation tests check for table and view existence.
- Production-ready enhancements could include data quality checks, more detailed tests, and error handling.


## Challenges

- Limited access to cluster IDs in free Databricks tiers required use of new cluster creation for jobs.
- CLI setup and authentication sometimes require careful environment configuration.
- Scheduling and CI/CD integration requires coordination between Databricks and GitHub workflows.


## Improvement Ideas

- Add incremental data ingestion with change data capture.
- Implement more comprehensive unit and integration tests.
- Use parameterized notebooks for flexible environments.


## RBAC setup
Assumed groups
    - data-engineers-group	- ETL developers with full access to assignment catalog and schemas.
    - data-analysts-group	- Business/data analysts with (read-only access) i.e. SELECT on assignment.curated views only.

## Use of Language Models

No large language models (LLMs) were used directly in the development of this pipeline. Assistance was provided by AI tools to speed up development.
