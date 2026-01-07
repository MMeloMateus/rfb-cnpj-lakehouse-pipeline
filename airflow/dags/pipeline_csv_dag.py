from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


from cnpj_pipeline.extract.downloader import download_files_for_range
from cnpj_pipeline.extract.decompress import uncompress_zip_file_range

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_csv_dag",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
    default_args=default_args,
    max_active_runs=1,
    params = { "zip_dir": "/opt/project/data/raw",
                "csv_dir": "/opt/project/data/bronze/csv",
                "parquet_dir": "/opt/project/data/bronze/parquet",
                "start_date": "2025-02",
                "end_date": "2025-02",
                "sep":";"
                }
) as dag:
    download = PythonOperator(
        task_id="download",
        python_callable=download_files_for_range,
        op_kwargs={
            "origin_base_path": "{{ params.zip_dir }}",
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
        },
    )

    unzip_csv = PythonOperator(
        task_id="unzip_to_csv",
        python_callable= uncompress_zip_file_range,
        op_kwargs={
            "origin_base_path": "{{ params.zip_dir }}",
            "output_dir": "{{ params.csv_dir }}",
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}",
            "sep": "{{ params.sep }}",
        },
    )

    download >> unzip_csv