import logging
from common.airflow.exceptions import EmptyFileError

def on_failure_callback(context):
    logging.error(f"FAILED: {context['task_instance'].task_id} - {context['exception']}")

def on_success_callback(context):
    results = context['task_instance'].xcom_pull(task_ids='process_file')
    n = len(results) if results else 0
    logging.info(f"Pipeline complete. Processed {n} files.")

def on_empty_file_callback(context):
    exception = context.get("exception")

    if isinstance(exception, EmptyFileError):
        logging.warning(f"EMPTY FILE detected: {exception}")

def combined_failure_callback(context):
    on_failure_callback(context)
    on_empty_file_callback(context)