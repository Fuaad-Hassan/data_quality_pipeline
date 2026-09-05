# Config-Driven Data Pipeline

A lightweight ETL pipeline that ingests data from external APIs and local CSV file drops, validates it against a configurable YAML schema, and routes the records into a PostgreSQL database.

Rather than failing the entire pipeline when it encounters bad data, this system isolates invalid records into a quarantine table for review, allowing the clean data to continue to production.

## How It Works

The process is broken down into four independent steps:

1. **Ingestion:** Supports omni-channel inputs. It fetches JSON payloads from external APIs or scans the `00_dropzone/` directory for raw CSV files, normalizes the data, and saves the standardized output locally as Parquet files.
2. **Validation:** Reads the column constraints defined in `config/schema_contracts.yaml`. It uses Polars to filter the incoming Parquet file, splitting the records into `clean` and `quarantine` datasets without using hardcoded Python rules.
3. **Storage Routing:**
* **Clean Data:** Inserted into a strictly typed PostgreSQL table (`prod_demographics`).
* **Quarantined Data:** Because corrupt records will violate SQL data types, the entire failed row is serialized into a `JSONB` object and stored in a separate table (`dead_letter_logs`) alongside the reason it failed.

4. **Reporting:** Queries the database and generates a basic Seaborn chart showing the ratio of passed vs. failed records for the day's ingestion.

## Tools Used

* **Polars & PyArrow:** For memory-efficient data evaluation and reading/writing Parquet files.
* **PostgreSQL (Dockerized):** Handles both structured relational data and unstructured JSONB data.
* **PyYAML & python-dotenv:** Keeps validation rules and database credentials outside of the core codebase.
* **Seaborn:** Generates daily visual health checks.

## Project Structure and Orchestration

Currently, the pipeline is managed by a single script (`local_execution_controller.py`). However, the modules inside `src/` are built to be entirely independent.

Instead of passing large dataframes in memory between steps, the functions only pass the file paths of the generated Parquet files. This makes it straightforward to migrate this project to an orchestrator like Apache Airflow in the future, as the existing functions can be mapped directly to Airflow tasks without rewriting the underlying logic.

## Setup and Execution

**1. Configure the environment**
Create a `.env` file in the root directory with your credentials:

```env
API_KEY=your_api_key_here
DB_USER=admin
DB_PASSWORD=your_secure_password
DB_NAME=analytics_db

```

**2. Start the database**
The Docker setup will automatically provision Postgres and execute the table creation scripts on boot.

```bash
docker compose --env-file .env -f infrastructure/docker-compose.yml up -d

```

**3. Run the pipeline**
Set up your virtual environment, install the dependencies, and execute the controller.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python local_execution_controller.py

```

After the script finishes, you can check the `data/04_audit_reports/` folder for the generated dashboard, or connect to the Postgres container to view the loaded records.