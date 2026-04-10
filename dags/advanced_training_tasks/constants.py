from airflow.datasets import Dataset

DEFAULT_OWNER = "airflow"
DEFAULT_RETRIES = 2
DEFAULT_DELAY_MINUTES = 3
USERS_ACTIVITY_TABLE = "users_activity"
USERS_SUMMARY_DATASET = Dataset("file:///tmp/users_summary.json")