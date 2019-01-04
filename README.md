# Real-World Databricks Project (Medallion Architecture)

This project turns the course transcripts into a runnable Databricks implementation using the Medallion Architecture (Bronze → Silver → Gold).

## Overview
- Bronze: Ingest raw files (CSV, TSV, XML, fixed-width) into Delta tables with technical columns.
- Silver: Standardize schemas, clean values, and conform dimensions.
- Gold: Build KPIs and answer business questions on 2019 data.

## Datasets (expected)
Upload these four files into DBFS (e.g., `dbfs:/FileStore/world_bank_demo/`):
- Youth & Adult Literacy Rate (TSV)
- Children Out of School (CSV)
- Government Education Expenditure (XML)
- World Population (fixed-width)

Note: File names may differ. Set them in `notebooks/01_bronze_ingestion.py`.

## Quick Start (Databricks Community Edition)
1. Import this folder into your Databricks workspace (or clone via Repos).
2. Open `notebooks/01_bronze_ingestion.py` and set `raw_base_path` and file names.
3. Attach a cluster (DBR 12.2+ recommended). Install Maven library `com.databricks:spark-xml_2.12:0.16.0`.
4. Run `01_bronze_ingestion.py` (creates database and Bronze Delta tables).
5. Run `02_silver_transformations.py` (cleans and standardizes to Silver).
6. Run `03_gold_kpis.py` (joins and computes KPIs and answers questions).

### Optional: Generate sample data locally
Run the generator to create four files under `real_world_databricks_project/data/raw`:

```bash
python real_world_databricks_project/scripts/generate_sample_data.py
```

Then upload the generated files to your Databricks `raw_base_path` (e.g., `dbfs:/FileStore/world_bank_demo/`).

## Run Everything Locally (no Databricks)
- Requires Java 8+ and Python 3.10+.
- Creates a local Delta Lake in `real_world_databricks_project/_delta_lake`.

```powershell
python -m venv .venv; `
    .\.venv\Scripts\Activate.ps1; `
    pip install -U pip; `
    pip install -r real_world_databricks_project\requirements.txt; `
    python real_world_databricks_project\scripts\generate_sample_data.py; `
    python real_world_databricks_project\run_local_pipeline.py
```

This will output answers for the three KPIs to the console and create Delta tables under a local metastore.

### Visualize KPIs locally
After running the local pipeline:

```powershell
python real_world_databricks_project\scripts\visualize_kpis.py
```

Images are saved to `real_world_databricks_project/outputs/`.

## Configuration
Project-level defaults live in `configs/config.yaml`. You can override in notebooks if preferred.

## Workflows (optional)
`workflows/job.json` defines a Job with tasks for each notebook and a job cluster. Import into a paid Databricks workspace (CE does not support Jobs).

## Notes
- XML support relies on the Spark-XML datasource (install via Maven coordinate above).
- Fixed-width parsing is implemented with substring slicing using configured column specs.
- A small list of aggregate regions is filtered out to keep only countries.


