import csv
import os
import time
from datetime import datetime, timedelta

import airflow
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def _transfer_data():
    with connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123') as source_conn, \
            connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        source_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dbs'")
        tables = [row[0] for row in source_cursor.fetchall()]

        for table in tables:
            source_cursor.execute(f"SELECT * FROM dbs.{table}")
            rows = source_cursor.fetchall()

            dest_table = f"sta.st_{table}"
            dest_cursor.execute(f"CREATE TABLE IF NOT EXISTS {dest_table} (LIKE dbs.{table} INCLUDING ALL)")
            dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")
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

    transfer_task = PythonOperator(
        task_id='transfer_data',
        python_callable=_transfer_data
    )

transfer_task
