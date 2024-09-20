from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from google.cloud import bigquery
import pandas as pd
import gzip
import ndjson
import requests
import io
from concurrent.futures import ThreadPoolExecutor
from google.api_core.exceptions import NotFound

# Define necessary variables
GCS_BUCKET_NAME = 'bucket-nexar'  # Google Cloud Storage bucket name
BQ_DATASET_NAME = 'dataset_nexar'  # BigQuery dataset name
BQ_TABLE_NAME = 'table_nexar'      # BigQuery table name
NDJSON_URLS = [  # List of NDJSON file URLs
    'https://raw.githubusercontent.com/HaoAnh167/ndjson_file/main/test1.ndjson',
    'https://raw.githubusercontent.com/HaoAnh167/ndjson_file/main/test2.ndjson'
]
LOCAL_NDJSON_PATH = '/tmp/data.ndjson'  # Local path to store downloaded NDJSON files
LOCAL_CSV_PATH = '/tmp/data.csv'        # Local path to store converted CSV file
GCS_CSV_PATH = 'data.csv.gz'            # Path for the CSV file in Google Cloud Storage

def download_ndjson(url):
    """Download NDJSON file from a URL and save it locally."""
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    with open(LOCAL_NDJSON_PATH, 'wb') as file:
        file.write(response.content)

def process_ndjson():
    """Process NDJSON file and convert it to CSV format."""
    try:
        with open(LOCAL_NDJSON_PATH) as f:
            data = ndjson.load(f)  # Load NDJSON data
    except json.JSONDecodeError as e:
        raise ValueError(f"JSONDecodeError: {e}")
    except Exception as e:
        raise RuntimeError(f"Error processing NDJSON: {e}")

    df = pd.DataFrame(data)  # Convert NDJSON data to DataFrame
    df.to_csv(LOCAL_CSV_PATH, index=False)  # Save DataFrame as CSV file

def compress_file():
    """Compress the CSV file using gzip."""
    with open(LOCAL_CSV_PATH, 'rb') as f_in:
        with gzip.open('/tmp/data.csv.gz', 'wb') as f_out:
            f_out.writelines(f_in)

def create_bq_table():
    """Create BigQuery dataset and table if they do not exist."""
    client = bigquery.Client()
    dataset_ref = client.dataset(BQ_DATASET_NAME)
    
    # Create dataset if it does not exist
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'  # Set the dataset location
        client.create_dataset(dataset)

    table_ref = dataset_ref.table(BQ_TABLE_NAME)
    try:
        client.get_table(table_ref)
    except NotFound:
        # Define schema for the BigQuery table
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("email", "STRING")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

def load_csv_to_bq():
    """Load the compressed CSV file from GCS into BigQuery."""
    client = bigquery.Client()
    dataset_ref = client.dataset(BQ_DATASET_NAME)
    table_ref = dataset_ref.table(BQ_TABLE_NAME)
    uri = f'gs://{GCS_BUCKET_NAME}/{GCS_CSV_PATH}'  # URI for the CSV file in GCS
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("email", "STRING")
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
    )
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

def download_ndjson_from_urls(**kwargs):
    """Download NDJSON files from a list of URLs using concurrent execution."""
    urls = kwargs['params'].get('urls', [])
    with ThreadPoolExecutor() as executor:
        executor.map(download_ndjson, urls)

default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start date for the DAG
    'retries': 1,  # Number of retries for failed tasks
}

with DAG('ndjson_pipeline',
         default_args=default_args,
         schedule_interval='0 0 * * *',  # Schedule to run at 07:00 AM (Vietnam time UTC + 7)
         catchup=False) as dag:

    start = DummyOperator(task_id='start')  # Start of the DAG

    download_task = PythonOperator(
        task_id='download_ndjson',
        python_callable=download_ndjson_from_urls,
        op_kwargs={'params': {'urls': NDJSON_URLS}}  # Pass the list of NDJSON URLs
    )

    process_task = PythonOperator(
        task_id='process_ndjson',
        python_callable=process_ndjson  # Call function to process NDJSON
    )

    compress_task = PythonOperator(
        task_id='compress_file',
        python_callable=compress_file  # Call function to compress CSV file
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/data.csv.gz',  # Source file path
        dst=GCS_CSV_PATH,        # Destination path in GCS
        bucket=GCS_BUCKET_NAME,  # GCS bucket name
        mime_type='application/gzip',  # MIME type for gzip file
        gcp_conn_id='googlecloudplatform'  # Google Cloud connection ID
    )

    create_bq_table_task = PythonOperator(
        task_id='create_bq_table',
        python_callable=create_bq_table  # Call function to create BigQuery table
    )

    load_bq_task = PythonOperator(
        task_id='load_csv_to_bq',
        python_callable=load_csv_to_bq  # Call function to load CSV into BigQuery
    )

    end = DummyOperator(task_id='end')  # End of the DAG

    # Define task dependencies
    start >> download_task >> process_task >> compress_task >> upload_to_gcs >> create_bq_table_task >> load_bq_task >> end
