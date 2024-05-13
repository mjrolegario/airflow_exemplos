from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow import DAG
from pendulum import datetime as pd_dt 
import exemplo_orm

args_orm = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pd_dt(2024, 4, 10, 17, 0, 0, 0, tz="America/Sao_Paulo"),
    'retries': 0,
    'retry_delay': timedelta(minutes=2)}

dag_orm = DAG(
    dag_id ='Dados_orm',
    default_args=args_orm,
    description='Pegamos dados da tabela_main para criar uma tabela para o antifraude.',
    schedule_interval='0 5 * * *',
    catchup=False,
    max_active_runs=1)

# Define the tasks
task_orm = PythonOperator(
    task_id='Task_orm',
    python_callable= exemplo_orm.main_orm,
    provide_context=True,
    task_concurrency=1,
    dag=dag_orm)