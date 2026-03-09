#Running Airflow in Docker

1. Download docker-compose.yaml:from (https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)

2. Enable TCP/IP protocol over a network: (https://medium.com/@vedkoditkar/connect-to-local-ms-sql-server-from-docker-container-9d2b3d33e5e9)

3. Include the following lines in the environment section of docker-compose.yaml:
`_PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-microsoft-mssql`

4. Add .env file with the following information:
`AIRFLOW_IMAGE_NAME=apache/airflow:3.1.7`
`AIRFLOW_UID=50000`

5. Add `- ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data` to volumes (docker-compose.yaml)

6. Set `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` (docker-compose.yaml)

7. Initialize the database, run:
`docker compose up airflow-init`

After initialization is complete, you should see output related to files, folders, and plug-ins and finally a message like this:
`airflow-init-1 exited with code 0`

The account created has the login airflow and the password airflow.

8. Start all services:
`docker compose up -d`

9. Add new connection in Airflow:
ID - mssql_local
TYPE - Microsoft SQL Server
HOST - host.docker.internal
LOGIN - sa 
PASSWORD - changeMe1!
PORT - 1433
SCHEMA - AirflowDB