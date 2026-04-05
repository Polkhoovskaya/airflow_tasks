# •	What is the difference between backfill and catchup=True? >> 
# Backfill is a manual process where you trigger the execution of past DAG runs that were missed or not executed. 
# Catchup=True is an automatic setting that allows Airflow to automatically execute all past DAG runs that were 
# scheduled but not executed when the DAG is first deployed or when it starts running.


# •	If you had catchup=False and ran backfill, what would happen to the runs between start_date and today? >>
# If you had catchup=False and ran backfill, the runs between start_date and today would not be executed automatically. 
# Backfill would only execute the specific runs that you manually trigger, 
# and it would not fill in the gaps for any runs that were missed due to catchup being set to False. 
# You would need to manually trigger each run that you want to execute during the backfill process.

# •	What happens to dag_consumer when the backfill completes? Does it trigger once or 5 times? >>
# When the backfill completes, dag_consumer will trigger once for each run that was backfilled. 
# If you backfill 5 runs, then dag_consumer will trigger 5 times, once for each of the backfilled runs.

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import pandas as pd
import logging
from airflow.datasets import Dataset

BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')
CSV_PATH = os.path.join(BASE_DIR, "users_activity.csv")

users_dataset = Dataset("file:///tmp/users_summary.json")

@dag(
    dag_id='dag_producer_1',
    start_date=datetime(2026, 4, 1),
    tags=["producer", "task"],
    schedule="@daily",
    catchup=True,
)

def producer_etl():

    @task()
    def validate_csv(ds=None):
        logging.info("Reading CSV file...")
        df = pd.read_csv(CSV_PATH, parse_dates=["event_time"])
        logging.info(f"Read {len(df)} rows.")

        logging.info("Validating schema...")
        required_columns = {"user_id", "event_type", "event_time", "device", "country"}

        missing_columns = required_columns - set(df.columns)
        if missing_columns:
             raise ValueError(f"Missing required columns: {missing_columns}.")

        logging.info("Schema validation passed.")

        path = f"/tmp/validated_{ds}.csv"
        df.to_csv(path, index=False)

        logging.info(f"Validated data saved to {path}")
        return path

    @task()
    def clean_data(input_path: str, ds=None):
        df = pd.read_csv(input_path, parse_dates=["event_time"])

        logging.info("Dropping duplicates...")
        df = df.drop_duplicates()
        logging.info(f"DataFrame now has {len(df)} rows after dropping duplicates.")
        
        path = f"/tmp/cleaned_{ds}.csv"
        df.to_csv(path, index=False)

        logging.info(f"Cleaned data saved to {path}")
        return path

    @task()
    def aggregate_data(input_path: str, ds=None):

        df = pd.read_csv(input_path, parse_dates=["event_time"])

        df["event_date"] = df["event_time"].dt.date

        logging.info("Computing daily summary...")

        summary = (
        df.groupby(["event_date", "country"])
          .agg(
              total_events=("event_type", "count"),
              unique_users=("user_id", "nunique")
          )
          .reset_index()
        )

        path = f"/tmp/aggregated_{ds}.json"
        summary.to_json(path, orient="records", date_format="iso")

        logging.info(f"Aggregated data saved to {path}")
        return path
    

    @task(outlets=[users_dataset])
    def save_summary(input_path: str, ds=None):
        final_path = f"/tmp/users_summary_{ds}.json"

        logging.info("Saving summary to JSON...")
        df = pd.read_json(input_path)
        df.to_json(final_path, orient="records", date_format="iso")
        logging.info(f"Saved summary to {final_path}")
        logging.info("Dataset updated!")


    validated = validate_csv()
    cleaned = clean_data(validated)
    aggregated = aggregate_data(cleaned)
    save_summary(aggregated)

dag = producer_etl()
