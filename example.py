from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 15),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('example_python',
          default_args=default_args,
          schedule_interval='@once',
          start_date=datetime(2017, 3, 20), 
          catchup=False)

def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_db').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/usr/local/airflow/dags/example.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'example_table', sep=',')
        get_postgres_conn.commit()


task1 = PostgresOperator(task_id = 'etl',
                         sql = ("create table if not exists example_table " +
                                "(" +
                                    "test_id text, " +
                                    "test_value text " +
                                ")"),
                         postgres_conn_id='airflow_db', 
                         autocommit=True,
                         dag= dag)

task = PythonOperator(task_id='csv_to_db',
                   provide_context=False,
                   python_callable=csvToPostgres,
                   dag=dag)