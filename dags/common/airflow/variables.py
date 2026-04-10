from airflow.models import Variable
from datetime import datetime
import logging

def write_run_timestamp(variable_key: str):
    ts = datetime.utcnow().isoformat()

    Variable.set(
        key=variable_key,
        value=ts
    )

    logging.info(f"Setting variable {variable_key} to: {ts}")
    return ts

def read_run_timestamp(variable_key: str):
    ts = Variable.get(variable_key)

    logging.info(f"Processing data produced at: {ts}")

    return ts