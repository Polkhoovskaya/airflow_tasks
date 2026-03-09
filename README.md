# airflow_tasks
# TASK 3 – Airflow Basic Pipeline

## Dataset
`users_activity.csv`

---

## Tasks

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

## Schedule

- **Frequency:** Daily

---

## Deliverable

- An **Airflow DAG visible in the Airflow UI**.

---

## Acceptance Criteria

- The **DAG runs successfully without manual fixes**.
- **Retry logic is implemented** for failed tasks.
- **Logs are visible in the Airflow UI** for monitoring and debugging.
