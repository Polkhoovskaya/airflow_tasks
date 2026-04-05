from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import pandas as pd
import logging
from airflow.datasets import Dataset


BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')
CSV_PATH = os.path.join(BASE_DIR, "incoming/campaign_data.csv")

@dag(
    dag_id='dag_consumer_1',
    schedule=[Dataset("file:///tmp/users_summary.json")],
    catchup=False,
    tags=["consumer", "task"],
)


def consumer_etl():

    @task()
    def validate_csv(ds=None):
        logging.info("Reading CSV file...")
        df = pd.read_csv(CSV_PATH, parse_dates=["event_date"])
        logging.info(f"Read {len(df)} rows.")

        logging.info("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}

        missing_columns = required_columns - set(df.columns)
        if missing_columns:
             raise ValueError(f"Missing required columns: {missing_columns}.")

        logging.info("Schema validation passed.")

        path = f"/tmp/campaign_validated_{ds}.csv"
        df.to_csv(path, index=False)

        logging.info(f"Validated data saved to {path}")
        return path

    @task()
    def clean_data(input_path: str, ds=None):
        df = pd.read_csv(input_path, parse_dates=["event_date"])

        logging.info("Dropping duplicates...")
        df = df.drop_duplicates()
        logging.info(f"DataFrame now has {len(df)} rows after dropping duplicates.")
        
        path = f"/tmp/campaign_cleaned_{ds}.csv"
        df.to_csv(path, index=False)

        logging.info(f"Cleaned data saved to {path}")
        return path

    @task()
    def produce_combined_report(input_path: str, ds=None, **context):

        # Read JSON form tmp
        ds = context["ds"]
        summary_path = f"/tmp/users_summary_{ds}.json"
        
        logging.info(ds)
        logging.info(f"Reading summary from {summary_path}")
        summary_df = pd.read_json(summary_path, convert_dates=["event_date"])
        logging.info(f"Read summary with {len(summary_df)} rows.")

        # Read cleaned campaign data csv
        logging.info(f"Reading campaign data from {input_path}")
        campaign_df = pd.read_csv(input_path, parse_dates=["event_date"])
        logging.info(f"Read campaign data with {len(campaign_df)} rows.")

        # join on event_date
        logging.info("Joining campaign data with user summary...")
        combined_df = pd.merge(campaign_df, summary_df, on="event_date", how="inner")
        logging.info(f"Combined data has {len(combined_df)} rows.")
        logging.info(combined_df)

        producer_run_timestamp = context["ts"]

        logging.info(f"Processing data produced at: {producer_run_timestamp}")

    validated = validate_csv()
    cleaned = clean_data(validated)
    produce_combined_report(cleaned)


consumer_etl_dag = consumer_etl()
