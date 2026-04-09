from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import get_current_context

from advanced_training_tasks.paths import build_path
from advanced_training_tasks.constants import USERS_SUMMARY_DATASET

import pandas as pd
import logging

CSV_PATH = build_path("incoming", "campaign_data.csv")

@dag(
    dag_id='dag_consumer',
    schedule=[USERS_SUMMARY_DATASET],
    catchup=False,
    tags=["consumer", "task"],
)

def consumer_etl():

    @task()
    def validate_csv() -> str:
        logging.info("Reading CSV file...")
        df = pd.read_csv(CSV_PATH, parse_dates=["event_date"])
        logging.info(f"Read {len(df)} rows.")

        logging.info("Validating schema...")
        required_columns = {'campaign_id', 'clicks', 'impressions', 'spend', 'event_date'}

        missing_columns = required_columns - set(df.columns)
        if missing_columns:
             raise ValueError(f"Missing required columns: {missing_columns}.")
        logging.info("Schema validation passed.")

        run_date = datetime.now().strftime("%Y-%m-%d")
        path = f"/tmp/campaign_validated_{run_date}.csv"
        df.to_csv(path, index=False)
        logging.info(f"Validated data saved to {path}")
        return path

    @task()
    def clean_data(input_path: str) -> str:
        df = pd.read_csv(input_path, parse_dates=["event_date"])
        logging.info("Dropping duplicates...")
        df = df.drop_duplicates()
        logging.info(f"DataFrame now has {len(df)} rows after dropping duplicates.")
        
        run_date = datetime.now().strftime("%Y-%m-%d")
        path = f"/tmp/campaign_cleaned_{run_date}.csv"
        df.to_csv(path, index=False)
        logging.info(f"Cleaned data saved to {path}")
        return path

    @task()
    def produce_combined_report(input_path: str):

        # Read JSON from  tmp
        run_date = datetime.now().strftime("%Y-%m-%d")
        summary_path = f"/tmp/users_summary_{run_date}.json"
        
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
        logging.info("First 3 rows:\n" + combined_df.head(3).to_string())

        context = get_current_context()
        producer_run_timestamp = context.get("asset_trigger_timestamp") or datetime.now().isoformat()

        logging.info(f"Processing data produced at: {producer_run_timestamp}")

    validated = validate_csv()
    cleaned = clean_data(validated)
    produce_combined_report(cleaned)


consumer_etl_dag = consumer_etl()
