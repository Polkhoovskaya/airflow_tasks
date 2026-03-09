from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
#for testing
# from sqlalchemy import create_engine 

# retry logic 
default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

CSV_PATH = "./data/users_activity.csv"
table_name = "UsersActivity"

@dag(
    dag_id="task_3_dag",
    default_args=default_args,
    start_date=datetime(2026, 3, 9),
    schedule="@daily", # schedule daily
    catchup=False,
    tags=["task"],
)

def task_3_etl():

    # read CSV
    @task()
    def read_csv() -> pd.DataFrame:

        print("Reading CSV file...")
        df = pd.read_csv(CSV_PATH)
        print(f"Read {len(df)} rows.")
        
        return df

    # validate schema
    @task()
    def validate_schema(df: pd.DataFrame) -> pd.DataFrame:

        print("Validating schema...")
        required_columns = {"user_id", "event_type", "event_time", "device", "country"}
        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        
        print("Schema validation passed.")
        return df

    # transform data
    @task()
    def transform_data(data: pd.DataFrame) -> pd.DataFrame:
        
        print("Transforming data...")
        df = data.drop_duplicates()
        df["event_time"] = pd.to_datetime(df["event_time"])
        
        print("Data transformation completed.")
        return df

    # load into SQL
    @task()
    def load_into_sql(df: pd.DataFrame) -> None:

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

    #for testing
    # @task() 
    # def load_into_sqlite(df: pd.DataFrame) -> None:
    #     print("Loading data into SQLite...")
    #     engine = create_engine("sqlite:///users_activity.db")

    #     print("Connection established, loading data...")
    #     df.to_sql(
    #         name=table_name, 
    #         con=engine,
    #         if_exists="fail",
    #         index=False
    #     )
    #     print(f"{len(df)} rows inserted.")


    csv_data = read_csv()
    validated_data = validate_schema(csv_data)
    transformed_data = transform_data(validated_data)
    load_into_sql(transformed_data)

dag = task_3_etl()