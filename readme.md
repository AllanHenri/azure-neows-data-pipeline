# Azure NeoWs Data Pipeline

Cloud-based data engineering project that ingests NASA Near-Earth Object Web Service (NeoWs) data into Azure Data Lake Storage Gen2, downloads the raw file for local processing, transforms nested JSON with PySpark, and generates analytical datasets in Parquet format.

## Overview

This project was built to demonstrate an end-to-end data engineering workflow using:

- a real public API
- cloud storage on Azure
- nested JSON processing
- data transformation with PySpark
- analytical output generation in Parquet

The pipeline consumes data from the **NASA NeoWs API**, stores the raw JSON in **Azure Data Lake Storage Gen2**, downloads the file locally, flattens the nested structure with **PySpark**, and creates two output layers:

- **processed**: flattened asteroid-level dataset
- **curated**: aggregated analytical metrics

---

## Architecture

```mermaid
flowchart LR
    A[NASA NeoWs API] --> B[extract_to_adls.py]
    B --> C[Azure Data Lake Storage Gen2 - raw]
    C --> D[download_from_adls.py]
    D --> E[transform.py with PySpark]
    E --> F[Processed Parquet]
    E --> G[Curated Metrics Parquet]

azure-neows-data-pipeline/
├── data/
│   ├── raw/
│   ├── processed/
│   └── curated/
├── src/
│   ├── config.py
│   ├── extract_to_adls.py
│   ├── download_from_adls.py
│   ├── transform.py
│   ├── list_adls_paths.py
│   ├── jobs/
│   │   └── batch_job.py
│   └── utils/
│       └── spark_session.py
├── .env.example
├── requirements.txt
└── README.md

Pipeline Flow
1. Extract

extract_to_adls.py requests Near-Earth Object data from the NASA NeoWs API and uploads the raw JSON file to Azure Data Lake Storage Gen2.

2. Download

download_from_adls.py downloads the raw JSON file from ADLS to a local path for transformation.

3. Transform

transform.py uses PySpark to:

read the raw JSON file
normalize the near_earth_objects structure
flatten nested asteroid fields
convert relevant fields to analytical types
generate processed and curated datasets
4. Output

The pipeline writes:

data/processed/neo_flattened
data/curated/neo_metrics
Tech Stack
Python
PySpark
Azure Data Lake Storage Gen2
Azure Storage SDK for Python
NASA NeoWs API
Parquet
Example Processed Columns

The processed dataset includes columns such as:

approach_date
id
neo_reference_id
name
nasa_jpl_url
absolute_magnitude_h
diameter_min_meters
diameter_max_meters
diameter_avg_meters
is_potentially_hazardous_asteroid
is_sentry_object
close_approach_date
close_approach_date_full
epoch_date_close_approach
relative_velocity_kph
relative_velocity_kps
miss_distance_km
miss_distance_lunar
orbiting_body
year
month
Example Curated Metrics

The curated dataset contains aggregated metrics by year and month, including:

total_asteroids
total_hazardous
avg_velocity_kph
avg_miss_distance_km
avg_diameter_meters
How to Run
1. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate
2. Install dependencies
pip install -r requirements.txt
3. Create your .env file

Use .env.example as a reference and configure:

NASA API key
Azure Storage connection string
file system / directory / file name
local raw path
processed output path
curated output path
4. Run the full pipeline
python -m src.jobs.batch_job
Environment Variables

Example structure:

NASA_API_KEY=DEMO_KEY
START_DATE=2015-09-07
END_DATE=2015-09-08

AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_FILE_SYSTEM=raw
AZURE_DIRECTORY=nasa/neows/year=2015/month=09/day=08
AZURE_FILE_NAME=neo_feed.json

LOCAL_RAW_PATH=data/raw/neo_feed.json
PROCESSED_OUTPUT=data/processed/neo_flattened
CURATED_OUTPUT=data/curated/neo_metrics
Business Value

This project simulates a real-world cloud data engineering workflow where raw API data is ingested into a data lake, processed into structured datasets, and prepared for downstream analytics and reporting.

It demonstrates practical skills in:

API ingestion
Azure-based storage workflows
nested JSON transformation
PySpark batch processing
layered data architecture
Technical Highlights
Integration with Azure Data Lake Storage Gen2
Extraction from a real public API
Flattening of deeply nested JSON structures
Transformation and aggregation with PySpark
Output generation in Parquet
Separation of raw, processed, and curated layers
Possible Improvements

Future enhancements for this project include:

uploading processed and curated Parquet datasets back to Azure
partitioning outputs by year and month
adding data quality checks
orchestrating the workflow with Azure Data Factory
adding monitoring and logging improvements
integrating with Power BI or another reporting layer
Why This Project Matters

This project was designed to strengthen practical skills in modern data engineering by combining:

cloud storage
API ingestion
Spark processing
data modeling for analytics

It is part of a broader portfolio focused on ETL pipelines, PySpark, orchestration, and cloud-based data workflows.

screenshots
<img width="1606" height="382" alt="image" src="https://github.com/user-attachments/assets/55193862-cc44-4eaf-93e7-9c1268baa2a5" />

<img width="1524" height="608" alt="image" src="https://github.com/user-attachments/assets/f5116d05-e64b-4795-afa4-1e1621681be1" />
<img width="252" height="386" alt="image" src="https://github.com/user-attachments/assets/a0a18a93-9535-448a-9d76-f7a147a578a8" />


