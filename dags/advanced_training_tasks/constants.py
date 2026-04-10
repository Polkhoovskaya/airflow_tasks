from airflow.datasets import Dataset
from datetime import datetime

DEFAULT_OWNER = "airflow"
DEFAULT_RETRIES = 2
DEFAULT_DELAY_MINUTES = 3
DEFAULT_START_DATE = datetime(2026, 4, 10)
USERS_ACTIVITY_TABLE = "users_activity"
USERS_SUMMARY_DATASET = Dataset("file:///tmp/users_summary.json")