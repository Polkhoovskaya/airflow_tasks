from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.base import PokeReturnValue
import pandas as pd
import logging
import os

# retry logic 
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')
# INCOMING_FOLDER = os.path.join(BASE_DIR, "branching")
CSV_PATH = os.path.join(BASE_DIR, "branching/users_activity_large.csv")
table_name = "users_activity_full"

@dag(
    dag_id="dag_branching_etl", 
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["branching", "task"],
)

def dag_branching_etl():

#     # sensor waits for new file
#     @task.sensor(poke_interval=30, timeout=3600, mode="reschedule")
#     def wait_for_csv() -> PokeReturnValue:
#         print("Checking for new CSV files...")
#         files = os.listdir(INCOMING_FOLDER)
#         csv_files = [f for f in files if f.endswith(".csv")]
      
#         if csv_files:
#             file_path = os.path.join(INCOMING_FOLDER, csv_files[0])
#             print(f"Found new file: {file_path}.")
#             return PokeReturnValue(is_done=True, xcom_value=file_path)

#         print("No new files found yet...")
#         return PokeReturnValue(is_done=False)

    @task()
    # def read_csv(file_path: str) -> str:
    # 1.	read_csv — read the file, return the file path (NOT the DataFrame) via XCom
    def read_csv() -> str:

        print("Reading CSV file...")
        df = pd.read_csv(CSV_PATH)
        print(f"Read {len(df)} rows.")
        
        return CSV_PATH

    @task.branch()
    # 2.	check_volume — read the file, count rows, return the string 'light_process' 
    # or 'heavy_process'. This task must be decorated with @task.branch.
    def check_volume(csv_path: str) -> str:

        print("Сounting the number of rows...")
        df = pd.read_csv(csv_path)
        row_count = len(df)
        print(f"Row count: {row_count}")
        
        return 'light_process' if row_count < 23 else 'heavy_process'
    
    @task()
    # 3.	light_process — log a summary of row counts 
    # per event_type to the Airflow task log.
    def light_process(csv_path: str):

        print("Processing with light process...")
        df = pd.read_csv(csv_path)
        summary = df.groupby("event_type").size()

        for event_type, count in summary.items():
            logging.info(f"event_type={event_type}, count={count}")


    @task()
    # 4.	heavy_process — load the data into 
    # MSSQL table users_activity_full using MsSqlHook.
    def heavy_process(csv_path: str):

        print("Processing with heavy process...")
        df = pd.read_csv(csv_path)

        print("Loading data into SQL Server...")
        mssql_hook = MsSqlHook(mssql_conn_id="mssql_local")
        engine = mssql_hook.get_sqlalchemy_engine()

        print("Connection established, loading data...")
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

        print(f"Data loaded into table {table_name}.")


    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    # 5.	finalize — always runs after either branch. Log: 
    # 'Pipeline completed for execution date: {{ ds }}'. Must use trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS.

    def finalize():

        context = get_current_context()
        ds = context["ds"]

        logging.info(f"Pipeline completed for execution date: {ds}")


    # csv_path = wait_for_csv()
    # read_csv(csv_path)
    csv_path = read_csv()
    branch = check_volume(csv_path)
    branch >> [light_process(csv_path), heavy_process(csv_path)] >> finalize()

dag = dag_branching_etl()

