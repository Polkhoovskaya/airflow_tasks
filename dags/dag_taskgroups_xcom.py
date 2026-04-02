from airflow.decorators import dag, task
from datetime import datetime, timedelta
# from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
import os
import logging
import pandas as pd
from airflow.operators.python import get_current_context

# retry logic 
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

BASE_DIR = os.environ.get('AIRFLOW_DATA_DIR', '/opt/airflow/data')

@dag(
    dag_id="dag_taskgroups_xcom",
    default_args=default_args,
    start_date=datetime(2026, 4, 2),
    schedule="@daily",
    catchup=False,
    tags=["taskgroups", "task"]
)

def dag_taskgroups_xcom_etl():

    @task()
    def read_users():
        file_path = os.path.join(BASE_DIR, "users_activity.csv")
        
        print("Reading CSV file...")
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} rows.")
        return file_path

    @task()
    def transform_users(file_path: str):
        df = pd.read_csv(file_path)

        df = df.dropna(subset=["user_id"])
        output_path = os.path.join(BASE_DIR, "processed_users.csv")
        df.to_csv(output_path, index=False)
        return output_path

    @task()
    def push_users_summary(output_path, **context):
        df = pd.read_csv(output_path)

        summary = {
            "total_rows_users": len(df),
            "unique_users": df["user_id"].nunique(),
            "unique_countries": df["country"].nunique()
        }

        logging.info(f"Users activity summary: {summary}")

        context["task_instance"].xcom_push(
            key="users_summary",
            value=summary
        )

    # @task_group(group_id='group_users')
    # def group_users():
    with TaskGroup('group_users') as group_users:
        t1 = read_users()
        t2 = transform_users(t1)
        t3 = push_users_summary(t2)
       
        t1 >> t2 >> t3


    @task()
    def read_campaigns():
        file_path = os.path.join(BASE_DIR, "incoming/campaign_data.csv")
        
        print("Reading CSV file...")
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} rows.")
        return file_path

    @task()
    def transform_campaigns(file_path: str):
        df = pd.read_csv(file_path)

        df = df.dropna(subset=["campaign_id"])
        output_path = os.path.join(BASE_DIR, "processed_campaigns.csv")
        df.to_csv(output_path, index=False)
        return output_path

    @task()
    def push_campaign_summary(output_path, **context):
        df = pd.read_csv(output_path)
        df["ctr"] = df["clicks"] / df["impressions"]

        summary = {
            "total_rows_campaigns": len(df),
            "total_spend": df["spend"].sum(),
            "avg_ctr": df["ctr"].mean()
        }

        logging.info(f"Campaigns summary: {summary}")

        context["task_instance"].xcom_push(
            key="campaigns_summary",
            value=summary
        )

    # @task_group(group_id='group_campaigns')
    # def group_campaigns():
    with TaskGroup('group_campaigns') as group_campaigns:
        t1 = read_campaigns()
        t2 = transform_campaigns(t1)
        t3 = push_campaign_summary(t2)
       
        t1 >> t2 >> t3

    @task()
    def join_report():
        context = get_current_context()
        summary = (context['ti'].xcom_pull(key="users_summary", task_ids="group_users.push_users_summary")) | (context['ti'].xcom_pull(key="campaigns_summary", task_ids="group_campaigns.push_campaign_summary"))
       
        logging.info(f"Joining reports...")
        logging.info(f"Summary: {summary}")

    [group_users, group_campaigns] >> join_report()

dag = dag_taskgroups_xcom_etl()
        
