from airflow.decorators import dag, task
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from airflow.sensors.base import PokeReturnValue
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import os
import re

# default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

INCOMING_FOLDER = "./data/incoming"

# DAG definition
@dag(
    dag_id="task_4_dag",
    default_args=default_args,
    tags=["task"],
    catchup=False,
)

def task_4_etl():

    # sensor waits for new file
    @task.sensor(poke_interval=30, timeout=3600, mode="reschedule")
    def wait_for_csv() -> PokeReturnValue:
        print("Checking for new CSV files...")
        files = os.listdir(INCOMING_FOLDER)
        csv_files = [f for f in files if f.endswith(".csv")]
      
        if csv_files:
            file_path = os.path.join(INCOMING_FOLDER, csv_files[0])
            print(f"Found new file: {file_path}.")
            return PokeReturnValue(is_done=True, xcom_value=file_path)

        print("No new files found yet...")
        return PokeReturnValue(is_done=False)

    # read CSV file
    @task()
    def read_csv(file_path: str) -> pd.DataFrame:
        print(f"Reading CSV file: {file_path}.")
        data = pd.read_csv(file_path)
        print(f"Read {len(data)} rows.")
        return data

    # validate schema
    @task()
    def validate_schema(data: pd.DataFrame) -> pd.DataFrame:
        required_columns = {"campaign_id", "user_id", "clicks", "impressions", "spend", "event_date"}
        print("Validating schema...")
        missing_columns = required_columns - set(data.columns)
        
        if missing_columns:
             raise ValueError(f"Missing required columns: {missing_columns}.")

        print("Schema validation passed.")
        return data

    # transform data
    @task()
    def transform_data(data: pd.DataFrame) -> pd.DataFrame:
        print("Transforming data...")
        df = data.drop_duplicates()
        df["event_date"] = pd.to_datetime(df["event_date"])
        print("Data transformation completed.")
        return df

    # calculate
    @task()
    def calculate_metrics(data: pd.DataFrame) -> pd.DataFrame:
        print("Calculating metrics...")
        data["CTR"] = np.where(data["impressions"] != 0, data["clicks"] / data["impressions"], 0)
        data["CPC"] = np.where(data["clicks"] != 0, data["spend"] / data["clicks"], 0)
        print("Metrics calculation completed.")
        return data

    # load into SQL
    @task()
    def add_data_to_sql(data: pd.DataFrame, file_path: str):

        df_renamed = data.rename(columns={
            "campaign_id": "campaign_id",
            "clicks": "total_clicks",
            "impressions": "total_impressions",
            "spend": "total_spend",
            "CTR": "ctr",
            "CPC": "cpc"
        })

        df_renamed = df_renamed[["campaign_id", "event_date", "total_clicks", "total_impressions", "total_spend", "ctr", "cpc"]]

        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0]
        table_name = re.sub(r'[^0-9a-zA-Z_]', '_', table_name)

        print("Loading data into SQL Server...")
        mssql_hook = MsSqlHook(mssql_conn_id="mssql_local")
        engine = mssql_hook.get_sqlalchemy_engine()
        print("Connection established, loading data...")

        df_renamed.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        )

        print("Data loaded successfully")
        return True

    # move file to /processed/
    @task()
    def move_file(file_path: str, load_success: bool):
        
        print(f"Moving file: {file_path} to processed folder...")
        processed_folder = os.path.join(INCOMING_FOLDER, "../processed")
        os.makedirs(processed_folder, exist_ok=True)
        new_path = os.path.join(processed_folder, os.path.basename(file_path))
        os.rename(file_path, new_path)
        print(f"Moved file to {new_path}")
    
    # DAG pipeline
    csv_path = wait_for_csv()
    csv_data = read_csv(csv_path)
    validated = validate_schema(csv_data)
    transformed_data = transform_data(validated)
    final_data = calculate_metrics(transformed_data)
    load_success = add_data_to_sql(final_data, csv_path)
    move_file(csv_path, load_success)

dag = task_4_etl()
