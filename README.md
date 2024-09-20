# NDJSON to BigQuery Pipeline with Airflow

This pipeline downloads a .ndjson file, converts it to CSV, uploads it to Google Cloud Storage, and loads the data into BigQuery.

## Requirements
- Python 3.6+
- Google Cloud SDK
- Airflow

### Install the necessary Python libraries:
```bash
pip install apache-airflow pandas google-cloud-bigquery ndjson requests google-cloud-storage

Setting up Google Cloud

    Create a Google Cloud Storage Bucket: Create a bucket to store the CSV files.

    Create a BigQuery Dataset: Create a dataset in BigQuery named dataset_nexar.

    Create a Service Account: Create a Service Account with permissions for accessing BigQuery and GCS. Download the key JSON file and set the environment variable:
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-file.json"

Initialize the Airflow database:
	airflow db init

Start the webserver and scheduler:
	airflow webserver
	airflow scheduler

Add the DAG:
Clone the repository and copy the DAG to the Airflow directory:
	git clone https://github.com/HaoAnh167/nexar.git
	cp nexar/ndjson_pipeline_dag.py ~/airflow/dags/

```
## File description

### File Name: ndjson_pipeline_dag.py
```bash
Description

Defines an Apache Airflow DAG for an ETL pipeline that:

    Downloads NDJSON files from specified URLs.
    Converts the NDJSON data to CSV format.
    Compresses the CSV file.
    Uploads the compressed CSV file to Google Cloud Storage (GCS).
    Creates a BigQuery table if it does not exist.
    Loads the data from the compressed CSV file in GCS into BigQuery.

Main Components:

Libraries Used:

    airflow: Core library for defining and running workflows.
    google.cloud: Google Cloud library for interacting with BigQuery.
    pandas: Data manipulation library for processing data.
    gzip: Library for compressing files.
    ndjson: Library for handling NDJSON data.
    requests: Library for making HTTP requests.
    concurrent.futures: Library for concurrent execution.
    google.api_core.exceptions: Library for Google Cloud exceptions.

Configuration Variables:

    GCS_BUCKET_NAME: Name of the Google Cloud Storage bucket.
    BQ_DATASET_NAME: Name of the BigQuery dataset.
    BQ_TABLE_NAME: Name of the BigQuery table.
    NDJSON_URLS: List of NDJSON file URLs.
    LOCAL_NDJSON_PATH: Local path for storing downloaded NDJSON files.
    LOCAL_CSV_PATH: Local path for storing converted CSV files.
    GCS_CSV_PATH: Path for the CSV file in Google Cloud Storage.

Python Functions:

    download_ndjson(url): Downloads NDJSON files from the given URL.
    process_ndjson(): Converts NDJSON data to CSV format.
    compress_file(): Compresses the CSV file into gzip format.
    create_bq_table(): Creates a BigQuery dataset and table if they do not exist.
    load_csv_to_bq(): Loads the compressed CSV file from GCS into BigQuery.
    download_ndjson_from_urls(**kwargs): Downloads NDJSON files from multiple URLs using multi-threading.

DAG Tasks:

    start: Dummy task to mark the start of the DAG.
    download_task: Task to download NDJSON data.
    process_task: Task to process and convert NDJSON data to CSV.
    compress_task: Task to compress the CSV file.
    upload_to_gcs: Task to upload the compressed file to GCS.
    create_bq_table_task: Task to create the BigQuery table.
    load_bq_task: Task to load data from GCS into BigQuery.
    end: Dummy task to mark the end of the DAG.
