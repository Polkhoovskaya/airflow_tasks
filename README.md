# airflow_tasks

# Advanced - "advanced_training_tasks" folder


## TASK 1 – Branching ETL Pipeline

### 📌 Background
Your team receives a daily `users_activity.csv` file.  
Depending on the data volume for that day, downstream processing must follow two different paths:

- **Light path** → small files (quick aggregation + CSV export)
- **Heavy path** → large files (full SQL load + summary report)

You must implement this conditional routing inside an Airflow DAG.

### Requirements

Build a DAG named dag_branching_etl with the following pipeline:


```bash
read_csv
    |
check_volume
    |                          |
[row_count < 10]         [row_count >= 10]
    |                          |
light_process              heavy_process
    |                          |
    └─────────┬─────────────────┘
          finalize
```

1.  read_csv — read the file, return the file path (NOT the DataFrame) via XCom.
2.  check_volume — read the file, count rows, return the string 'light_process' or 'heavy_process'. This task must be decorated with @task.branch.
3.  light_process — log a summary of row counts per event_type to the Airflow task log.
4.  heavy_process — load the data into MSSQL table users_activity_full using MsSqlHook.
5.  finalize — always runs after either branch. Log: 'Pipeline completed for execution date: {{ ds }}'. Must use trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS.

- Schedule: @daily
- Use catchup=False
- Add retry logic: retries=2, retry_delay=timedelta(minutes=3)

### Key Concepts to Learn

|     |     |
| --- | --- |
| **Concept** | **What to understand** |
| @task.branch | Returns the task_id (string) of the next task to run. All other downstream branches are skipped automatically. |
| TriggerRule | By default, a task only runs if ALL upstream tasks succeeded. NONE_FAILED_MIN_ONE_SUCCESS allows it to run even when some upstream tasks were skipped. |
| Skipped state | In the Airflow UI, skipped tasks appear in pink. This is normal — it means the branch logic worked correctly. |
| XCom for paths | Returning a file path string (not a DataFrame) from read_csv is the correct pattern. XCom is designed for small metadata, not bulk data. |

### Acceptance Criteria

|     |     |
| --- | --- |
| **✓** | **Acceptance Criteria** |
| □   | Run the DAG with a small file — only light_process runs, heavy_process is pink (skipped). |
| □   | Run the DAG with a large file — only heavy_process runs, light_process is pink (skipped). |
| □   | finalize always runs regardless of which branch fired. |
| □   | XCom tab in the task instance shows a file path string, not a serialized object. |
| □   | The DAG completes without manual fixes in the Airflow UI. |


## TASK 2 – File & HTTP Sensor Pipeline

### 📌 Background
Your pipeline should not start processing until two conditions are met: a new campaign CSV file has landed in the incoming folder, and an external API health endpoint returns HTTP 200. You will build a DAG that waits for both conditions in parallel before proceeding to transform and load the data.

### Setting Up the Mock API

Run this in a terminal inside your Docker container to simulate an API health endpoint:

# Inside the Airflow scheduler or worker container
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{\"status\": \"ok\"}')
HTTPServer(('0.0.0.0', 8888), H).serve_forever()"

### Requirements

Build a DAG named dag_sensor_pipeline:


```bash
wait_for_file ──┐
                ├──► validate_and_load
wait_for_api  ──┘
```

1.  wait_for_file — use @task.sensor with mode='reschedule', poke_interval=20, timeout=600. Check if any .csv file exists in data/incoming/. Return the file path via PokeReturnValue.
2.  wait_for_api — use @task.sensor with mode='reschedule', poke_interval=30, timeout=300. Make an HTTP GET to http://localhost:8888/health. Return True only when response status is 200.
3.  validate_and_load — runs only after BOTH sensors succeed. Reads the CSV, validates that columns campaign_id, clicks, impressions, spend, event_date exist, then logs row count and first 3 rows.

- Both sensors must run in parallel (not sequential).
- Use mode='reschedule' on both sensors — never mode='poke' in production.
- Handle sensor timeout gracefully: add on_failure_callback that logs 'Sensor timed out: {task_id}'.
- Schedule: None (manual trigger only).

### Key Concepts to Learn

|     |     |
| --- | --- |
| **Concept** | **What to understand** |
| poke vs reschedule mode | poke holds a worker slot for the entire wait time. reschedule releases the worker between checks — always use reschedule in production to avoid starving the worker pool. |
| PokeReturnValue | Allows a sensor to return data (via xcom_value) when it succeeds. Without it, sensors can only signal done/not-done. |
| Parallel sensors | Two tasks with no dependency between them run in parallel automatically. Do not chain them with >> unless you want them sequential. |
| Sensor timeout | timeout is in seconds. If the condition is never met before timeout, the task fails. poke_interval controls how often to check. |

### Acceptance Criteria

|     |     |
| --- | --- |
| **✓** | **Acceptance Criteria** |
| □   | Both sensors appear in the same tier in the Graph view (parallel, not sequential). |
| □   | Removing the CSV file causes wait_for_file to keep poking until the file reappears — without blocking a worker. |
| □   | Stopping the mock API causes wait_for_api to keep retrying with no worker held. |
| □   | validate_and_load only starts after both sensors turn green. |
| □   | on_failure_callback logs the correct task_id when a sensor times out. |


## TASK 3 – TaskGroups & XCom Patterns

### 📌 Background
As pipelines grow, a flat list of tasks becomes hard to read and maintain in the Airflow UI. TaskGroups let you visually and logically group related tasks. This task also introduces explicit XCom push/pull — a pattern needed when you want to share data between tasks that are not directly chained.

### Requirements

Build a DAG named dag_taskgroups_xcom with the following structure:


```bash
┌─── group_users ──────────────┐    ┌─── group_campaigns ──────────┐
│  read_users                  │    │  read_campaigns               │
│       │                      │    │       │                        │
│  transform_users             │    │  transform_campaigns          │
│       │                      │    │       │                        │
│  push_users_summary ─────────┼────┼─ push_campaign_summary        │
└──────────────────────────────┘    └──────────────────────────────┘
                        │                          │
                        └──────────┬───────────────┘
                              join_report
```

1.  Create two TaskGroups: group_users and group_campaigns. Each group must be visible as a collapsed unit in the Airflow Graph view.
2.  push_users_summary — at the end of group_users, explicitly push a dict to XCom using task_instance.xcom_push(key='users_summary', value={...}). The dict must contain: total_rows, unique_users, unique_countries.
3.  push_campaign_summary — same pattern for campaigns. Push: total_rows, total_spend, avg_ctr (compute CTR = clicks / impressions per row, then take the mean).
4.  join_report — pulls both XCom values explicitly using context\['ti'\].xcom_pull(task_ids='group_users.push_users_summary', key='users_summary'). Logs a formatted joint report.

- Do NOT return DataFrames from any task. Only push/pull summary dicts.
- Use the TaskGroup context manager: with TaskGroup('group_users') as group_users.
- Schedule: @daily, catchup=False.

### Key Concepts to Learn

|     |     |
| --- | --- |
| **Concept** | **What to understand** |
| TaskGroup | A visual and logical grouping. Does not change execution order — it only organizes the Graph view. Nested groups are allowed. |
| XCom push (explicit) | xcom_push(key, value) gives your XCom a named key, making it easier to pull from multiple downstream tasks reliably. |
| XCom pull with task_ids | When pulling from a task inside a TaskGroup, the task_id includes the group prefix: 'group_users.push_users_summary'. |
| XCom size warning | XCom is stored in the metadata DB. Keep pushed values small: a dict with 5 keys is fine, a DataFrame with 10,000 rows is not. |

### Acceptance Criteria

|     |     |
| --- | --- |
| **✓** | **Acceptance Criteria** |
| □   | Graph view shows two collapsed TaskGroups. Clicking each expands it to show its internal tasks. |
| □   | join_report logs all 6 values (3 from each group) correctly. |
| □   | XCom tab in the Airflow UI shows the pushed dicts with correct keys. |
| □   | Changing the input CSV and re-running produces updated XCom values — no stale data. |
| □   | No DataFrame is returned or stored in XCom at any point. |


## TASK 4 – Dynamic Task Mapping & Callbacks

### 📌 Background
In real pipelines, the number of files or items to process is not known at DAG write time. Dynamic task mapping lets Airflow create one task instance per item at runtime. This task also introduces callbacks — a critical production feature for alerting and audit logging when tasks succeed or fail.

### Dataset

Create three CSV files in data/incoming/ before running the DAG:

- **campaign_eu.csv** — copy of campaign_data.csv
- **campaign_us.csv** — same schema, change some values
- **campaign_asia.csv** — same schema, make one file intentionally empty (headers only) to test failure handling

### Requirements

Build a DAG named dag_dynamic_mapping:


```bash
list_files
    |
    |-- process_file[0]  (campaign_eu.csv)
    |-- process_file[1]  (campaign_us.csv)
    |-- process_file[2]  (campaign_asia.csv)
    |
consolidate_results
```

1.  list_files — returns a list of full file paths found in data/incoming/. No hardcoded filenames.
2.  process_file — decorated with @task. Takes a single file_path argument. Reads the CSV, validates schema, computes CTR and CPC, then returns a dict: {'file': filename, 'rows': count, 'status': 'ok' or 'empty'}. This task is mapped using .expand(file_path=list_files()).
3.  consolidate_results — receives the list of all process_file results (Airflow collects mapped outputs automatically). Logs: total files, total rows, list of any files with status='empty'. Raises an exception if ALL files were empty.

- Add on_failure_callback to process_file that logs: f"FAILED: {context\['task_instance'\].task_id} — {context\['exception'\]}".
- Add on_success_callback to consolidate_results that logs: f"Pipeline complete. Processed {n} files.".
- Schedule: None (manual trigger).
- Airflow 2.6+ required for .expand() with TaskFlow API.

### Key Concepts to Learn

|     |     |
| --- | --- |
| **Concept** | **What to understand** |
| .expand() | Creates one task instance per item in the input list at runtime. The number of instances is not known until list_files runs. |
| Mapped task outputs | consolidate_results receives a list of all mapped outputs automatically — you don't need to XCom pull manually. |
| on_failure_callback | A Python function called when a task fails. Receives a context dict with task_instance, exception, dag, execution_date, etc. |
| on_success_callback | Same as failure callback but triggered on success. Useful for audit logs and downstream notifications. |
| Partial failures | If one mapped instance fails (e.g. empty file), other instances continue. consolidate_results reflects the partial results. |

### Acceptance Criteria

|     |     |
| --- | --- |
| **✓** | **Acceptance Criteria** |
| □   | Adding a 4th CSV file to data/incoming/ causes a 4th task instance to appear in the Grid view — with zero DAG code changes. |
| □   | Each process_file instance shows independently in the Grid view with its own log. |
| □   | on_failure_callback fires for campaign_asia.csv (empty file) and logs the correct task_id. |
| □   | consolidate_results receives and logs results from all instances. |
| □   | If all three files are empty, consolidate_results raises an exception and the DAG run fails. |


## TASK 5 – Datasets, Event-Driven Scheduling & Backfill

### 📌 Background
Time-based scheduling (daily, hourly) is the most common pattern, but it is often the wrong one. If your upstream pipeline runs late or fails, a time-based downstream DAG will fire on stale data or worse — fail silently. Airflow Datasets let you express data dependencies explicitly: DAG B runs when DAG A has produced fresh data, regardless of wall-clock time. This task also covers backfill — how to re-run historical DAG runs for a date range, and what catchup=True means.

### Requirements

Build two DAGs that form a producer-consumer pipeline connected by a Dataset:

### DAG A: dag_producer (Producer)

1.  Reads users_activity.csv and computes a daily summary: total events, unique users per country.
2.  Writes the summary to /tmp/users_summary_{{ ds }}.json.
3.  **Declares an Airflow Dataset outlet:** Dataset('file:///tmp/users_summary.json'). This signals to Airflow that this DAG produces that dataset.
4.  Schedule: @daily, catchup=True (important — see backfill section below).

### DAG B: dag_consumer (Consumer)

1.  **Schedules on the Dataset:** schedule=\[Dataset('file:///tmp/users_summary.json')\]. This DAG only runs when dag_producer has completed successfully and updated the dataset.
2.  Reads the latest users_summary JSON and joins it with campaign_data.csv to produce a combined report.
3.  Logs: 'Processing data produced at: {producer_run_timestamp}'.
4.  Has no start_date and no time-based schedule. It is purely event-driven.


```bash
dag_producer runs (writes users_summary_2026-03-01.json)
       │
       │  [Dataset updated]
       ▼
dag_consumer triggered automatically
       │
       │  reads summary + campaign data
       ▼
  joint report logged
```

### Backfill Section

Once both DAGs are running, perform the following backfill exercise:

1.  Pause dag_producer in the Airflow UI.
2.  Use the Airflow CLI to backfill 5 past days:

airflow dags backfill dag_producer \\

\--start-date 2026-03-01 \\

\--end-date 2026-03-05

1.  Observe in the Grid view: 5 historical runs are created and executed.
2.  Answer the following questions in a comment at the top of dag_producer.py:

- What is the difference between backfill and catchup=True?
- If you had catchup=False and ran backfill, what would happen to the runs between start_date and today?
- What happens to dag_consumer when the backfill completes? Does it trigger once or 5 times?

### Key Concepts to Learn

|     |     |
| --- | --- |
| **Concept** | **What to understand** |
| Airflow Dataset | A logical URI that represents a data asset. DAGs declare outlets (what they produce) and other DAGs declare inlets (what they depend on). |
| schedule=\[Dataset(...)\] | Replaces the cron string. The DAG runs when all listed datasets have been updated by their producer DAGs since the consumer's last run. |
| catchup=True | When a DAG is unpaused or first activated, Airflow creates runs for all missed intervals between start_date and now. This can create a large backlog. |
| backfill | An explicit CLI command to run a DAG for a specific historical date range, regardless of catchup setting. Useful for reprocessing after a bug fix. |
| Dataset vs time schedule | Datasets decouple pipeline stages — the consumer always processes the freshest data and is never triggered prematurely. |

### Acceptance Criteria

|     |     |
| --- | --- |
| **✓** | **Acceptance Criteria** |
| □   | dag_consumer does not appear in the DAG list with a cron schedule — it shows 'Dataset' as its schedule type in the Airflow UI. |
| □   | Triggering dag_producer manually causes dag_consumer to start automatically within seconds. |
| □   | dag_consumer does NOT trigger if dag_producer fails (the dataset is not updated on failure). |
| □   | Backfill of 5 days creates exactly 5 historical runs in the Grid view for dag_producer. |
| □   | Questions in the comment block are answered correctly and show understanding of catchup vs backfill. |






## Quick Reference

### Common Airflow Patterns

|     |     |
| --- | --- |
| **Pattern** | **Correct approach** |
| Pass data between tasks | Return a file path or ID string. Write data to /tmp/ or a staging table. Never return a DataFrame. |
| Conditional routing | @task.branch returns the task_id string of the next task. Use TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS on merge tasks. |
| Wait for external event | @task.sensor with mode='reschedule'. Never mode='poke' in production. |
| Process N files in parallel | .expand(param=upstream_task()) with dynamic task mapping. |
| Group tasks visually | with TaskGroup('my_group') as grp: — then define tasks inside the block. |
| Failure notification | on_failure_callback=my_func in default_args or per-task. Function receives context dict. |
| Re-run historical data | airflow dags backfill dag_id --start-date YYYY-MM-DD --end-date YYYY-MM-DD |
| Event-driven trigger | schedule=\[Dataset('uri')\] on the consumer DAG. Producer DAG declares outlets=\[\] on @dag. |

### Useful CLI Commands

\# List all DAGs
airflow dags list
 
\# Trigger a DAG manually
airflow dags trigger dag_id
 
\# Test a single task without running the full DAG
airflow tasks test dag_id task_id 2026-03-01
 
\# Backfill a date range
airflow dags backfill dag_id --start-date 2026-03-01 --end-date 2026-03-05
 
\# View XCom values for a run
airflow tasks xcom-list dag_id task_id --execution-date 2026-03-01
 
\# Check DAG import errors
airflow dags list-import-errors


### Trigger Rules Reference

|     |     |
| --- | --- |
| **TriggerRule** | **When the task runs** |
| ALL_SUCCESS (default) | All upstream tasks succeeded. |
| ALL_DONE | All upstream tasks finished (success, failed, or skipped). |
| NONE_FAILED | No upstream task failed — skipped is acceptable. |
| NONE_FAILED_MIN_ONE_SUCCESS | No failure, and at least one success. Use for branch merge tasks. |
| ONE_SUCCESS | At least one upstream task succeeded — fires immediately, doesn't wait for others. |
| ALL_FAILED | All upstream tasks failed. Useful for fallback/recovery tasks. |









# Bacic - "junior_data_engineering_work" folder

## TASK 3 – Airflow Basic Pipeline

### Dataset
`users_activity.csv`

---

### Tasks

Create an **Airflow DAG** with the following tasks:

1. **Read CSV**
   - Load the `users_activity.csv` dataset.

2. **Validate Schema**
   - Check that required columns exist.
   - Verify data types and basic data integrity.

3. **Transform Data**
   - Clean and prepare the dataset.
   - Apply required transformations (e.g., filtering, formatting, normalization).

4. **Load into SQL**
   - Insert the processed data into a SQL database table.

---

### Schedule

- **Frequency:** Daily

---

### Deliverable

- An **Airflow DAG visible in the Airflow UI**.

---

### Acceptance Criteria

- The **DAG runs successfully without manual fixes**.
- **Retry logic is implemented** for failed tasks.
- **Logs are visible in the Airflow UI** for monitoring and debugging.






## TASK 4 – File Sensor Pipeline (Real World)

### Dataset
`campaign_data.csv`

**Folder:**  
`/data/incoming/`

---

### Tasks

Create an **Airflow DAG** with the following tasks:

1. **File Sensor**
   - Wait for a new file to appear in `/data/incoming/`.

2. **Validate Schema**
   - Ensure the required columns exist.
   - Verify correct data types and basic data validity.

3. **Calculate Metrics**
   - Compute the following fields:
     - **CTR** = `clicks / impressions`
     - **CPC** = `spend / clicks`

4. **Load into SQL**
   - Insert the processed data into the target SQL table.

5. **Move Processed File**
   - Move the processed file to `/processed/`.

---

### Output Table

campaign_id
event_date
total_clicks
total_impressions
total_spend
ctr
cpc

---

### Acceptance Criteria

- **Fully automated pipeline**
- **No manual trigger required**
- **Each file is processed only once**