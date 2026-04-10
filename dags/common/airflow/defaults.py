from datetime import timedelta

def get_default_args(owner="airflow", retries=2, delay_minutes=3):
    return {
        "owner": owner,
        "retries": retries,
        "retry_delay": timedelta(minutes=delay_minutes),
    }