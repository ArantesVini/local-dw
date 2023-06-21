import csv
import os
import time
from datetime import datetime, timedelta

import airflow
import pandas as pd
import psycopg2
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

def _extract_data():
    source_conn = psycopg2.connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123')
    query = "SELECT * FROM dbs.sr_customers"
    df = pd.read_sql(query, source_conn)
    csv_file_path = os.path.join('/opt/airflow/dags/intermediate_data', 't_1.csv')
    df.to_csv(csv_file_path, index=False)

with DAG(
        dag_id='dw_el_job',
        default_args=default_args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60),
        description='ETL Job to load data in a DW with Airflow',
        start_date=airflow.utils.dates.days_ago(1),
        ) as dag:


# def insert_data():
#     # Connect to the destination Postgres database
#     dest_conn = psycopg2.connect(host='postgres-container-2', database='dest_db_name', user='dest_user', password='dest_password')

#     # Load the data from the intermediary file
#     df = pd.read_csv('/path/to/intermediary_file.csv')

#     # Insert the data into the destination database
#     cursor = dest_conn.cursor()
#     for index, row in df.iterrows():
#         # Perform the necessary insert operation here
#         insert_query = "INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)"
#         values = (row['column1'], row['column2'], ...)
#         cursor.execute(insert_query, values)
#     cursor.close()
#     dest_conn.commit()
#     dest_conn.close()

# with DAG('postgres_data_transfer', default_args=default_args, schedule_interval='@daily') as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=_extract_data
    )

#     insert_task = PythonOperator(
#         task_id='insert_data',
#         python_callable=insert_data,
#         network_mode='bridge'
#     )

#     extract_task >> insert_task
