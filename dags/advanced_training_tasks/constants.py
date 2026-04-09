from airflow.datasets import Dataset

DEFAULT_RETRIES = 3
DEFAULT_DELAY = 300
USERS_ACTIVITY_TABLE = "users_activity"
USERS_SUMMARY_DATASET = Dataset("file:///tmp/users_summary.json")