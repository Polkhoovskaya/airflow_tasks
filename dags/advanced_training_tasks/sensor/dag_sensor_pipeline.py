from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import get_current_context

from advanced_training_tasks.paths import build_path
from advanced_training_tasks.constants import DEFAULT_START_DATE

import os
import requests
import pandas as pd
import logging

INCOMING_FOLDER = build_path("incoming")

# Callback for sensor failures
def sensor_failure_callback():
    context = get_current_context()

    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    dag_id = context["dag"].dag_id
    execution_date = context["ds"]

    if exception:
        logging.error(f"Sensor failed: {task_id} | reason: {exception}")
    else:
        logging.error(f"Sensor timed out: {task_id} | DAG: {dag_id} | execution_date: {execution_date}")

@dag(
    dag_id = "sensor_pipeline",
    start_date = DEFAULT_START_DATE,
    schedule = None,
    catchup=False,
    tags=["sensor", "task"]
)

def sensor_pipeline_etl():

    # Check if any .csv file exists in data/incoming/
    # Return the file path via PokeReturnValue
    @task.sensor(poke_interval=20, timeout=600, mode="reschedule", on_failure_callback=sensor_failure_callback)
    def wait_for_file() -> PokeReturnValue:
        logging.info("Waiting for file...")
        files = os.listdir(INCOMING_FOLDER)
        csv_files = [f for f in files if f.endswith(".csv")]
      
        if csv_files:
            latest_file = max([os.path.join(INCOMING_FOLDER, f) for f in files], key=os.path.getmtime)
            logging.info(f"Found new file: {latest_file}.")
            return PokeReturnValue(is_done=True, xcom_value=latest_file)

        logging.info("No new files found yet...")
        return PokeReturnValue(is_done=False)


    # Make an HTTP GET to http://localhost:8888/health
    # Return True only when response status is 200
    @task.sensor(poke_interval=30, timeout=300, mode="reschedule", on_failure_callback=sensor_failure_callback)
    def wait_for_api() -> PokeReturnValue:
        logging.info("Waiting for API response...")
        r = requests.get("http://localhost:8888/health")
        logging.info(f"API response: {r.json()}")
        
        if r.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=True)
        return PokeReturnValue(is_done=False)

    # Runs only after BOTH sensors succeed
    @task()
    def  validate_and_load(csv_path: str):
        logging.info("Validating and loading data...")
        
        df = pd.read_csv(csv_path)
        logging.info("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}
        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        logging.info("Schema validation passed.")
        logging.info(f"Read {len(df)} rows.")
        logging.info("First 3 rows:\n" + df.head(3).to_string())

    csv_path = wait_for_file()
    [csv_path, wait_for_api()] >> validate_and_load(csv_path)

dag = sensor_pipeline_etl()
        