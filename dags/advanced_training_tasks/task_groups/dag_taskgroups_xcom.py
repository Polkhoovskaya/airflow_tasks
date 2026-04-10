from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from advanced_training_tasks.paths import build_path
from advanced_training_tasks.constants import DEFAULT_OWNER, DEFAULT_RETRIES, DEFAULT_DELAY_MINUTES, DEFAULT_START_DATE

from common.transformation.cleaning import drop_nulls
from common.transformation.users import users_summary
from common.transformation.campaigns import campaigns_summary
from common.airflow.defaults import get_default_args

import logging
import pandas as pd

# retry logic 
default_args = get_default_args(owner=DEFAULT_OWNER, retries=DEFAULT_RETRIES, delay_minutes=DEFAULT_DELAY_MINUTES)

@dag(
    dag_id="dag_taskgroups_xcom",
    default_args=default_args,
    start_date=DEFAULT_START_DATE,
    schedule="@daily",
    catchup=False,
    tags=["taskgroups", "task"]
)

def dag_taskgroups_xcom_etl():

    @task()
    def read_users():
        file_path = build_path("users_activity.csv")
        
        logging.info("Reading CSV file...")
        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows.")

        return file_path

    @task()
    def transform_users(file_path: str):
        df = pd.read_csv(file_path)

        logging.info("Deleting all rows where user_id = NULL / NaN")
        df = drop_nulls(df, "user_id")
        logging.info(f"Rows after cleaning: {len(df)}")

        output_path = build_path("processed_users.csv")
        df.to_csv(output_path, index=False)

        logging.info(f"Saved to {output_path}")
        return output_path

    @task()
    def push_users_summary(output_path):
        df = pd.read_csv(output_path)
        context = get_current_context()

        logging.info("Calculating summary statistics...")
        summary = users_summary(df)
        logging.info(f"Users activity summary: {summary}")

        logging.info("Pushing summary to XCom...")
        context["task_instance"].xcom_push(
            key="users_summary",
            value=summary
        )

    # "group_users" TaskGroup
    with TaskGroup('group_users') as group_users:
        t1 = read_users()
        t2 = transform_users(t1)
        t3 = push_users_summary(t2)
       
        t1 >> t2 >> t3


    @task()
    def read_campaigns():
        file_path = build_path("incoming", "campaign_data.csv")
        
        logging.info("Reading CSV file...")
        df = pd.read_csv(file_path)
        logging.info(f"Read {len(df)} rows.")

        return file_path

    @task()
    def transform_campaigns(file_path: str):
        df = pd.read_csv(file_path)

        logging.info("Deleting all rows where campaign_id = NULL / NaN")
        df = drop_nulls(df, "campaign_id")

        logging.info(f"Rows after cleaning: {len(df)}")

        output_path = build_path("processed_campaigns.csv")
        df.to_csv(output_path, index=False)

        logging.info(f"Saved to {output_path}")
        return output_path

    @task()
    def push_campaign_summary(output_path):
        df = pd.read_csv(output_path)
        context = get_current_context()

        summary = campaigns_summary(df)

        logging.info(f"Campaigns summary: {summary}")

        logging.info("Pushing summary to XCom...")
        context["task_instance"].xcom_push(
            key="campaigns_summary",
            value=summary
        )

    # "group_campaigns" TaskGroup
    with TaskGroup('group_campaigns') as group_campaigns:
        t1 = read_campaigns()
        t2 = transform_campaigns(t1)
        t3 = push_campaign_summary(t2)
       
        t1 >> t2 >> t3

    @task()
    def join_report():
        context = get_current_context()
        
        logging.info(f"Joining reports...")
        summary = (context['ti'].xcom_pull(key="users_summary", task_ids="group_users.push_users_summary")) | (context['ti'].xcom_pull(key="campaigns_summary", task_ids="group_campaigns.push_campaign_summary"))
        logging.info(f"Summary: {summary}")

    [group_users, group_campaigns] >> join_report()

dag = dag_taskgroups_xcom_etl()
        
