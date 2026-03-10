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






# TASK 4 – File Sensor Pipeline (Real World)

## Dataset
`campaign_data.csv`

**Folder:**  
`/data/incoming/`

---

## Tasks

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

## Output Table

campaign_id
event_date
total_clicks
total_impressions
total_spend
ctr
cpc

---

## Acceptance Criteria

- **Fully automated pipeline**
- **No manual trigger required**
- **Each file is processed only once**