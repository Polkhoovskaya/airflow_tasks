from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
import os
import requests
import pandas as pd
import logging


BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')
INCOMING_FOLDER = os.path.join(BASE_DIR, "incoming")

# Handle sensor timeout gracefully: 
# add on_failure_callback that logs 'Sensor timed out: {task_id}'.
def sensor_failure_callback(context):
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
    start_date = datetime(2026, 4, 2),
    tags=["sensor", "task"]
)

def sensor_pipeline_etl():

    # wait_for_file — use @task.sensor with mode='reschedule', 
    # poke_interval=20, timeout=600. Check if any .csv file exists in data/incoming/.
    # Return the file path via PokeReturnValue.
    @task.sensor(poke_interval=20, timeout=600, mode="reschedule", on_failure_callback=sensor_failure_callback)
    def wait_for_file() -> PokeReturnValue:
        print("Waiting for file...")
        files = os.listdir(INCOMING_FOLDER)
        csv_files = [f for f in files if f.endswith(".csv")]
      
        if csv_files:
            file_path = os.path.join(INCOMING_FOLDER, csv_files[0])
            print(f"Found new file: {file_path}.")
            return PokeReturnValue(is_done=True, xcom_value=file_path)

        print("No new files found yet...")
        return PokeReturnValue(is_done=False)


    # wait_for_api — use @task.sensor with mode='reschedule', 
    # poke_interval=30, timeout=300. Make an HTTP GET to http://localhost:8888/health. 
    # Return True only when response status is 200.
    @task.sensor(poke_interval=30, timeout=300, mode="reschedule", on_failure_callback=sensor_failure_callback)
    def wait_for_api() -> PokeReturnValue:
        print("Waiting for api responce...")
        r = requests.get("http://localhost:8888/health")
        print(r.json())
        
        # Implementation for waiting for API response would go here
        if r.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=True)
        return PokeReturnValue(is_done=False)

    # validate_and_load — runs only after BOTH sensors succeed. 
    # Reads the CSV, validates that columns campaign_id, clicks, impressions, 
    # spend, event_date exist, then logs row count and first 3 rows
    @task()
    def  validate_and_load(csv_path: str):
        print("Validating and loading data...")
        df = pd.read_csv(csv_path)
        print("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}
        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        print("Schema validation passed.")
        print(f"Read {len(df)} rows.")

        logging.info(f"Row count:{len(df)}")

        logging.info("First 3 rows:")
        logging.info("\n" + df.head(3).to_string())

    csv_path = wait_for_file()
    [csv_path, wait_for_api()] >> validate_and_load(csv_path)

dag = sensor_pipeline_etl()
        