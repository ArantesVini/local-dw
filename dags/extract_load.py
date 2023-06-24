import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psycopg2 import connect
from psycopg2.extras import execute_values
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def _transfer_data(**context):
    with connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123') as source_conn, \
            connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        source_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbs'")
        tables = [row[0] for row in source_cursor.fetchall()]
        print(tables) #TODO
        for table in tables:
            dest_table = f"sta.sta_{table}"
            with open(f"{table}.csv", "w") as f:
                source_cursor\
                    .execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'dbs' AND table_name = '{table}'")
                header = ';'.join([row[0] for row in source_cursor.fetchall()])
                f.write(header + '\n')
                print(table)
                print('*' * 50)
                source_cursor.copy_to(f, f'"dbs"."{table}"', sep=';')
                f.seek(0)
                dest_cursor.copy_from(f, dest_table, sep=';')
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
        python_callable=_transfer_data,
        dag=dag
    )

    transfer_task