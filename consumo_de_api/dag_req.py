'''
MANOEL OLEGÁRIO

02/01/2024
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pendulum import datetime as pd_dt 
from datetime import timedelta, datetime
import req_API

args_etl_operacao = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pd_dt(2024, 1, 9, 22, 0, 0, 0, tz="America/Sao_Paulo"),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)}

dag_etl_operacao_rota = DAG(
    dag_id ='ETL_operacao_rota',
    default_args=args_etl_operacao,
    description='Extração de dados somente das rotas da operacao',
    schedule_interval='0 4 * * *',
    catchup=False,
    max_active_runs=1)

# Define the tasks
task_etl_operacao_rota = PythonOperator(
    task_id='Task_etl_operacao_rota',
    python_callable=req_API.main,
    provide_context=True,
    task_concurrency=1,
    dag=dag_etl_operacao_rota)

task_etl_operacao_rota

