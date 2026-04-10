from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common.io.mssql import load_to_mssql
from advanced_training_tasks.paths import build_path
from advanced_training_tasks.constants import USERS_ACTIVITY_TABLE, DEFAULT_OWNER, DEFAULT_RETRIES, DEFAULT_DELAY_MINUTES, DEFAULT_START_DATE
from common.airflow.defaults import get_default_args

import pandas as pd
import logging

# retry logic 
default_args = get_default_args(owner=DEFAULT_OWNER, retries=DEFAULT_RETRIES, delay_minutes=DEFAULT_DELAY_MINUTES)

CSV_PATH = build_path("branching", "users_activity_large.csv")

@dag(
    dag_id="dag_branching_etl", 
    default_args=default_args,
    start_date=DEFAULT_START_DATE,
    schedule="@daily",
    catchup=False,
    tags=["branching", "task"],
)

def dag_branching_etl():

    @task()
    # read_csv — read the file, return the file path (NOT the DataFrame) via XCom
    def read_csv() -> str:
        logging.info("Reading CSV file...")
        df = pd.read_csv(CSV_PATH)
        logging.info(f"Read {len(df)} rows")
        return CSV_PATH
        

    @task.branch()
    # check_volume — read the file, count rows, return the string 'light_process' 
    # or 'heavy_process'
    def check_volume(csv_path: str) -> str:
        logging.info("Counting rows...")

        df = pd.read_csv(csv_path)
        row_count = len(df)

        logging.info(f"Row count: {row_count}")
        
        return 'light_process' if row_count < 24 else 'heavy_process'
    
    @task()
    # light_process — log a summary of row counts 
    # per event_type to the Airflow task log
    def light_process(csv_path: str):
        logging.info("Processing with light process...")

        df = pd.read_csv(csv_path)
        summary = df.groupby("event_type").size()

        for event_type, count in summary.items():
            logging.info(f"event_type={event_type}, count={count}")


    @task()
    # heavy_process — load the data into 
    # MSSQL table users_activity_full using MsSqlHook
    def heavy_process(csv_path: str):
        logging.info("Processing with heavy process...")
        
        df = pd.read_csv(csv_path)

        logging.info("Loading data into SQL Server...")
        load_to_mssql(df, USERS_ACTIVITY_TABLE, conn_id="mssql_local")
        logging.info(f"Data loaded into table {USERS_ACTIVITY_TABLE}.")


    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    # Log: 'Pipeline completed for execution date: {{ ds }}'. 
    # Must use trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS.

    def finalize():
        context = get_current_context()
        ds = context["ds"]

        logging.info(f"Pipeline completed for execution date: {ds}")

    csv_path = read_csv()
    branch = check_volume(csv_path)
    branch >> [light_process(csv_path), heavy_process(csv_path)] >> finalize()

dag = dag_branching_etl()

