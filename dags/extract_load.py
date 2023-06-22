import os
import csv
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psycopg2 import connect
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _extract_data(**context):
    rows_dict = []
    with connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123') as source_conn:
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbs'")
        tables = [row[0] for row in source_cursor.fetchall()]
        for table in tables:
            file_path = os.path.join('/opt/airflow/dags/intermediate_data', f'{table}.csv')
            with open(file_path, 'w') as f:
                source_cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", f)
            rows_dict.append({'table': table, 'file_path': file_path})
    return rows_dict

def _load_data(**context):
    rows_dict = [row for row in context['ti'].xcom_pull(task_ids='extract_data')]

    with connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        dest_cursor = dest_conn.cursor()

        for row in rows_dict:
            table = row['table']
            file_path = row['file_path']
            dest_table = f"sta.st_{table}"
            dest_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS sta")
            dest_cursor.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} (LIKE dbs.{table} INCLUDING ALL) INHERITS (dbs.{table})")
            dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                rows = [row for row in reader]
            execute_values(dest_cursor, f"INSERT INTO {dest_table} VALUES %s", rows)

        dest_conn.commit()

with DAG(
        dag_id='dw_el_job',
        default_args=default_args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60),
        description='ETL Job to load data in a DW with Airflow',
        start_date=airflow.utils.dates.days_ago(1),
        ) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=_load_data,
        provide_context=True
    )

    extract_task >> load_task