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
    # Connect to the source database
    source_conn = connect(
        host='source_db',
        database='postgresDB',
        user='dbadmin',
        password='dbadmin123'
    )
    source_cursor = source_conn.cursor()

    # Connect to the destination database
    dest_conn = connect(
        host='destiny_db',
        database='postgresDB',
        user='dbadmin',
        password='dbadmin123'
    )
    dest_cursor = dest_conn.cursor()

    # Get a list of all tables in the source schema
    source_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dbs'")
    tables = [row[0] for row in source_cursor.fetchall()]

    # Transfer data for each table
    for table in tables:
        # Select data from the source table
        source_cursor.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier('dbs'), sql.Identifier(table)))
        rows = source_cursor.fetchall()

        # Check if the destination table exists
        dest_table = f"st_{table}"
        dest_cursor.execute(
            sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='sta' AND table_name={})")
                .format(sql.Literal(dest_table))
        )
        table_exists = dest_cursor.fetchone()[0]

        # Truncate and insert data if the table exists, otherwise create the table and insert data
        if table_exists:
            dest_cursor.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier('sta'), sql.Identifier(dest_table)))
        else:
            # Get the column names and data types of the source table
            source_cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='dbs' AND table_name=%s", (table,))
            columns = source_cursor.fetchall()

            # Create the destination table
            create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (").format(sql.Identifier('sta'), sql.Identifier(dest_table))
            column_definitions = []
            for column in columns:
                column_name, data_type = column
                column_definitions.append(sql.SQL("{} {}").format(sql.Identifier(column_name), sql.SQL(data_type)))
            create_table_query += sql.SQL(", ").join(column_definitions) + sql.SQL(")")
            dest_cursor.execute(create_table_query)

        # Generate the INSERT statement
        insert_statement = sql.SQL("INSERT INTO {}.{} VALUES %s").format(sql.Identifier('sta'), sql.Identifier(dest_table))

        # Execute the INSERT statement with the data rows
        execute_values(dest_cursor, insert_statement, rows)

    # Commit the changes in the destination DB and close the connections
    dest_conn.commit()
    source_cursor.close()
    source_conn.close()
    dest_cursor.close()
    dest_conn.close()


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
