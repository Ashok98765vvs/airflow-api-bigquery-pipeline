"""
Airflow DAG: api_to_bigquery_pipeline
Description: Extracts data from public API, loads to GCS, then BigQuery
Schedule: Daily at 6:00 AM UTC
Author: Ashok
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from utils.api_extractor import extract_api_data
from utils.bq_loader import upload_to_gcs
from utils.data_quality import run_quality_checks

# Default arguments
default_args = {
    'owner': 'ashok',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='api_to_bigquery_pipeline',
    default_args=default_args,
    description='End-to-end pipeline: API -> GCS -> BigQuery',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['data-engineering', 'bigquery', 'api'],
) as dag:

    # Task 1: Start
    start = EmptyOperator(task_id='start')

    # Task 2: Extract data from API
    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_api_data,
        op_kwargs={
            'api_url': 'https://api.openweathermap.org/data/2.5/forecast',
            'output_path': '/tmp/api_data_{{ ds }}.json',
        },
    )

    # Task 3: Upload raw data to GCS
    upload_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'local_path': '/tmp/api_data_{{ ds }}.json',
            'bucket_name': 'retail-pipeline-raw',
            'gcs_path': 'api_data/{{ ds }}/data.json',
        },
    )

    # Task 4: Load GCS to BigQuery
    load_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery_raw',
        bucket='retail-pipeline-raw',
        source_objects=['api_data/{{ ds }}/data.json'],
        destination_project_dataset_table='my_project.raw_dataset.api_data',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # Task 5: Run SQL transformations in BigQuery
    transform_task = BigQueryInsertJobOperator(
        task_id='run_transformations',
        configuration={
            'query': {
                'query': '{% include "sql/transform_analytics.sql" %}',
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': 'my_project',
                    'datasetId': 'analytics_dataset',
                    'tableId': 'api_analytics',
                },
                'writeDisposition': 'WRITE_TRUNCATE',
            }
        },
    )

    # Task 6: Run data quality checks
    quality_task = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_quality_checks,
        op_kwargs={
            'project': 'my_project',
            'dataset': 'analytics_dataset',
            'table': 'api_analytics',
            'min_row_count': 100,
        },
    )

    # Task 7: End
    end = EmptyOperator(task_id='notify_success')

    # Task dependencies
    start >> extract_task >> upload_gcs_task >> load_bq_task >> transform_task >> quality_task >> end
