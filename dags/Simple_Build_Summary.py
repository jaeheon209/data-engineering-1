from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
 
from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2
 
dag = DAG(
    dag_id='simple_build_summary',
    schedule_interval="@once",
    max_active_runs=1,
    concurrency=2,
    start_date=datetime(2021, 2, 10),
    catchup=False
)
 
 
def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()
 
def create_query(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    sql = context["params"]["sql"]
        
    cur = get_Redshift_connection()
    sql = "BEGIN;DROP TABLE IF EXISTS {schema}.{table};CREATE TABLE {schema}.{table} AS {sql};END;".format(schema=schema, table=table, sql=sql)
    logging.info(sql)
    cur.execute(sql)
 
 
create_query = PythonOperator(
    task_id = 'user_session_summary',
    #python_callable param points to the function you want to run 
    python_callable = create_query,
    params = {
        'table': 'user_session_summary',
        'schema': 'analytics',
        'sql': """SELECT usc.*, t.ts
FROM raw_data.user_session_channel usc
LEFT JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid"""
    },
    provide_context=True,
    dag = dag)
