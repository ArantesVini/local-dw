import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psycopg2 import connect
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

def _transfer_data(**context):
    with connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123') as source_conn, \
            connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        tables = ['sr_categories', 'sr_subcategory', 'sr_products',\
            'sr_cities', 'sr_localities', 'sr_customer_type', 'sr_customers', 'sr_sales']
        
        for table in tables:
            dest_table = f"sta.sta_{table}"
            source_cursor.execute(f"SELECT * FROM dbs.{table}")
            truncate_query = f"TRUNCATE TABLE {dest_table} CASCADE"
            dest_cursor.execute(truncate_query)
            rows = source_cursor.fetchall()
            for row in rows:
                insert_query = f"INSERT INTO {dest_table} VALUES %s"
                dest_cursor.execute(insert_query, (row,))
            dest_conn.commit()

def _refresh_materialized_view(mv_name):
    with connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'REFRESH MATERIALIZED VIEW {mv_name};')
        
        conn.commit()

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
    
    refresh_mv_task = PythonOperator(
        task_id='refresh_mv',
        python_callable=_refresh_materialized_view,
        op_kwargs={'mv_name': 'dw.mv_report'},
        dag=dag,
    )

    transfer_task >> refresh_mv_task
