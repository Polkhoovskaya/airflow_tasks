from airflow.decorators import dag, task
from datetime import datetime, timedelta

import os
import pandas as pd
import logging

BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')
INCOMING_FOLDER = os.path.join(BASE_DIR, "incoming")

def on_failure_callback(context):
    logging.error(f"FAILED: {context['task_instance'].task_id} - {context['exception']}")

def on_success_callback(context):
    results = context['task_instance'].xcom_pull(task_ids='process_file')
    n = len(results) if results else 0
    logging.info(f"Pipeline complete. Processed {n} files.")

@dag(
    dag_id="dag_dynamic_mapping",
    start_date=datetime(2026, 4, 6),
    catchup=False,
    tags=["dynamic_mapping", "task"],
)

def dag_dynamic_mapping_etl():

    # returns a list of full file paths found in data/incoming
    @task()
    def list_files():
        logging.info("Listing files in incoming folder...")

        if not os.path.exists(INCOMING_FOLDER):
            logging.warning(f"Folder does not exist: {INCOMING_FOLDER}")
            return []

        csv_files = [os.path.join(INCOMING_FOLDER, f) for f in os.listdir(INCOMING_FOLDER) if f.endswith(".csv")]
        logging.info(f"Found {len(csv_files)} CSV files.")
        return csv_files

    @task(on_failure_callback=on_failure_callback)
    def process_file(file_path: str) -> dict:
        logging.info("Processing file...")

        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows from {file_path}")

        logging.info("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}

        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        logging.info("Schema validation passed.")

        logging.info("Calculating CTR and CPC...")
        df["ctr"] = (df["clicks"] / df["impressions"]).fillna(0).replace([float("inf")], 0)
        df["cpc"] = (df["spend"] / df["clicks"]).fillna(0).replace([float("inf")], 0)

        return {
            'file': file_path,
            'rows': len(df),
            'status': 'ok' if len(df) > 0 else "empty"
        }

    @task(on_success_callback=on_success_callback)
    def consolidate_results(results: list):
        logging.info("Consolidating results...")

        total_files = len(results)
        total_rows = sum(result['rows'] for result in results)
        empty_files = [result['file'] for result in results if result['status'] == 'empty']

        logging.info(f"Total files processed: {total_files}" if total_files > 0 else "0 files processed.")
        logging.info(f"Total rows processed: {total_rows}" if total_rows > 0 else "0 rows processed.")
        logging.info(f"Empty files: {empty_files}" if empty_files else "0 empty files.")

        if total_files > 0 and len(empty_files) == total_files:
            raise ValueError("All files are empty!")

    files = list_files()
    # This task is mapped using .expand(file_path=list_files()) = Dynamic Task Mapping
    results = process_file.expand(file_path=files)
    consolidate_results(results)


dag = dag_dynamic_mapping_etl()