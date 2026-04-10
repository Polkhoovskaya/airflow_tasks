from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from common.utils.airflow_callbacks import combined_failure_callback, on_success_callback
from common.validation.schema import validate_columns
from advanced_training_tasks.paths import build_path
from advanced_training_tasks.constants import DEFAULT_OWNER, DEFAULT_RETRIES, DEFAULT_DELAY_MINUTES, DEFAULT_START_DATE
from common.airflow.defaults import get_default_args
from common.airflow.exceptions import EmptyFileError

import os
import pandas as pd
import logging

INCOMING_FOLDER = build_path("incoming")

# retry logic 
default_args = get_default_args(owner=DEFAULT_OWNER, retries=DEFAULT_RETRIES, delay_minutes=DEFAULT_DELAY_MINUTES)

@dag(
    dag_id="dag_dynamic_mapping",
    default_args=default_args,
    start_date=DEFAULT_START_DATE,
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

    @task(on_failure_callback=combined_failure_callback)
    def process_file(file_path: str) -> dict:

        logging.info("Processing file...")

        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows from {file_path}")

        if df.empty:
            raise EmptyFileError(f"File {file_path} is empty") 
        
        logging.info("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}

        validate_columns(df, required_columns)  # This will raise an error if validation fails

        logging.info("Schema validation passed.")

        logging.info("Calculating CTR and CPC...")
        df["ctr"] = (df["clicks"] / df["impressions"]).fillna(0).replace([float("inf")], 0)
        df["cpc"] = (df["spend"] / df["clicks"]).fillna(0).replace([float("inf")], 0)

        # It is not necessary to pass the "empty" status, since we handle this option above using on_failure_callback
        return {
            'file': file_path,
            'rows': len(df),
            'status': 'ok' if len(df) > 0 else "empty"
        }

    # what's commented out can be used as a second option for catching empty files;
    #  on_failure_callback is currently used
    @task(on_success_callback=on_success_callback, trigger_rule=TriggerRule.ALL_DONE)
    def consolidate_results(results: list):
        logging.info("Consolidating results...")

        total_files = len(results)
        total_rows = sum(result['rows'] for result in results)
        # empty_files = [result['file'] for result in results if result['status'] == 'empty']

        logging.info(f"Total files processed: {total_files}" if total_files > 0 else "0 files processed.")
        logging.info(f"Total rows processed: {total_rows}" if total_rows > 0 else "0 rows processed.")
        # logging.info(f"Empty files: {empty_files}" if empty_files else "0 empty files.")

        # if total_files > 0 and len(empty_files) == total_files:
        #     raise ValueError("All files are empty!")

    files = list_files()
    # This task is mapped using .expand(file_path=list_files()) = Dynamic Task Mapping
    results = process_file.expand(file_path=files)
    consolidate_results(results)


dag = dag_dynamic_mapping_etl()