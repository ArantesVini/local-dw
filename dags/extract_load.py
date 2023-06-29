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

sql_load_dimensions_queries = {
    "d_time": """
    TRUNCATE TABLE dw.d_time CASCADE;
    INSERT INTO dw.d_time (
        time_year, 
        time_month, 
        time_day, 
        time_hour, 
        time_date
        )
    SELECT 
        EXTRACT(YEAR FROM d)::INT, 
        EXTRACT(MONTH FROM d)::INT, 
        EXTRACT(DAY FROM d)::INT, 
        LPAD(EXTRACT(HOUR FROM d)::integer::text, 2, '0'), 
        d::DATE
    FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE, '1 HOUR'::INTERVAL) d;
        """,
    "d_product": """
    TRUNCATE TABLE dw.d_product CASCADE;
        INSERT INTO dw.d_product (
        product_id,
        product_name, 
        product_category, 
        product_subcategory
    )
    SELECT 
        product_id, 
        product_name, 
        category_name, 
        subcategory_name
    FROM sta.sta_sr_products tb1,
        sta.sta_sr_subcategory tb2, 
        sta.sta_sr_categories tb3
    WHERE tb3.category_id = tb2.category_id
        AND tb2.subcategory_id = tb1.subcategory_id;
        """,
    "d_customer": """
        TRUNCATE TABLE dw.d_customer CASCADE;
        INSERT INTO dw.d_customer (
        customer_id,
        customer_name, 
        customer_type
        )
    SELECT 
        customer_id, 
        customer_name, 
        customer_type_name
    FROM sta.sta_sr_customers tb1,
        sta.sta_sr_customer_type tb2
    WHERE tb2.customer_type_id = tb1.customer_type_id;
""",
    "d_locale":
    """
        TRUNCATE TABLE d_locale CASCADE;
        INSERT INTO dw.d_locale (
        locale_id, 
        locale_country, 
        locale_region, 
        locale_state, 
        locale_city)
    SELECT 
        locality_id AS locale_id, 
        locality_country AS locale_country, 
        locality_region AS locale_region, 
        CASE
            WHEN city_name = 'Natal' 
                THEN 'Rio Grande do Norte'
            WHEN city_name = 'Rio de Janeiro' 
                THEN 'Rio de Janeiro'
            WHEN city_name = 'Belo Horizonte' 
                THEN 'Minas Gerais'
            WHEN city_name = 'Salvador' 
                THEN 'Bahia'
            WHEN city_name = 'Blumenau' 
                THEN 'Santa Catarina'
            WHEN city_name = 'Curitiba' 
                THEN 'Paraná'
            WHEN city_name = 'Fortaleza' 
                THEN 'Ceará'
            WHEN city_name = 'Recife' 
                THEN 'Pernambuco'
            WHEN city_name = 'Porto Alegre' 
                THEN 'Rio Grande do Sul'
            WHEN city_name = 'Manaus' 
                THEN 'Amazonas'
        END locale_state, 
        city_name AS locale_city
    FROM sta.sta_sr_localities lo,
        sta.sta_sr_cities ci
    WHERE ci.city_id = lo.city_id;
""",
}

sql_load_fact_table = """
    TRUNCATE TABLE dw.fact_sales CASCADE;

    INSERT INTO
        dw.fact_sales (
            sk_product,
            sk_customer,
            sk_locale,
            sk_time,
            quantity,
            price_sale,
            price_product,
            sales_revenue,
            sales_result
        )
    SELECT
        sk_product,
        sk_customer,
        sk_locale,
        sk_time,
        SUM(sale_quantity) AS sale_quantity,
        SUM(sale_price) AS sale_price,
        SUM(product_cost) AS product_cost,
        SUM(
            ROUND (
                (
                    CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)
                ),
                2
            )
        ) AS sales_revenue,
        SUM(
            ROUND (
                (
                    CAST(sale_quantity AS numeric) * CAST(sale_price AS numeric)
                ),
                2
            ) - product_cost
        ) AS sales_result
    FROM
        sta.sta_sr_sales tb1
        JOIN sta.sta_sr_customers tb2 ON tb2.customer_id = tb1.customer_id
        JOIN sta.sta_sr_localities tb3 ON tb3.locality_id = tb1.locality_id -- TODO problem
        JOIN sta.sta_sr_products tb4 ON tb4.product_id = tb1.sale_product
        JOIN dw.d_time tb5 ON (
            to_char(tb5.time_date, 'YYYY-MM-DD') = to_char(tb1.sale_date, 'YYYY-MM-DD')
            AND tb5.time_hour = to_char(tb1.sale_date, 'HH')
        )
        JOIN dw.d_product tb6 ON tb4.product_id = tb6.product_id
        JOIN dw.d_locale tb7 ON tb3.locality_id = tb7.locale_id -- TODO problem
        JOIN dw.d_customer tb8 ON tb2.customer_id = tb8.customer_id
    GROUP BY
        sk_product,
        sk_customer,
        sk_locale,
        sk_time;
"""


def _transfer_data(**context):
    with connect(host='source_db', database='postgresDB', user='dbadmin', password='dbadmin123') as source_conn, \
            connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        tables = ['sr_categories', 'sr_subcategory', 'sr_products',
                  'sr_cities', 'sr_localities', 'sr_customer_type', 'sr_customers', 'sr_sales']

        for table in tables:
            dest_table = f"sta.sta_{table}"
            truncate_query = f"TRUNCATE TABLE {dest_table} CASCADE"
            dest_cursor.execute(truncate_query)
            source_cursor.execute(f"SELECT * FROM dbs.{table}")
            rows = source_cursor.fetchall()
            for row in rows:
                insert_query = f"INSERT INTO {dest_table} VALUES %s"
                dest_cursor.execute(insert_query, (row,))
            dest_conn.commit()


def _load_dw_table(query):
    with connect(host='destiny_db', database='postgresDB', user='dbadmin', password='dbadmin123') as dest_conn:
        with dest_conn.cursor() as dest_cursor:
            dest_cursor.execute(query)
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

    load_dw_d_locale = PythonOperator(
        task_id='load_dw_d_locale',
        python_callable=_load_dw_table,
        op_kwargs={'query': sql_load_dimensions_queries['d_locale']},
        dag=dag,
    )

    load_dw_d_product = PythonOperator(
        task_id='load_dw_d_product',
        python_callable=_load_dw_table,
        op_kwargs={'query': sql_load_dimensions_queries['d_product']},
        dag=dag,
    )

    load_dw_d_customer = PythonOperator(
        task_id='load_dw_d_customer',
        python_callable=_load_dw_table,
        op_kwargs={'query': sql_load_dimensions_queries['d_customer']},
        dag=dag,
    )

    load_dw_d_time = PythonOperator(
        task_id='load_dw_d_time',
        python_callable=_load_dw_table,
        op_kwargs={'query': sql_load_dimensions_queries['d_time']},
        dag=dag,
    )

    load_dw_fact_sales = PythonOperator(
        task_id='load_dw_fact_sales',
        python_callable=_load_dw_table,
        op_kwargs={'query': sql_load_fact_table},
        dag=dag,
    )

    # TODO refresh task to execute the ETL step on postgres (from staging to DW)

    transfer_task >> load_dw_d_locale >> load_dw_d_product >> \
        load_dw_d_customer >> load_dw_d_time >> load_dw_fact_sales >> refresh_mv_task
