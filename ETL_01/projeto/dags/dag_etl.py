import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.transform import Etl, Load
from etl.run_db_create_tables import RunToDB


DB_NAME = 'RZK'
DB_USER = 'JONANOV'
DB_PASSWORD = 'teste123'


def create_tables():
    executor = RunToDB(
        host="postgres",
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    executor.run_sql_file("/opt/airflow/sql/create_tables.sql")


def run_etl(**kwargs):
    csv_filename = kwargs['csv_filename']
    table_name = kwargs['table_name']

    etl = Etl(csv_file=csv_filename)
    load = Load(etl=etl, table_name=table_name, db=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    load.load_to_db()

def create_view():
    executor = RunToDB(
        host="postgres",
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    executor.run_sql_file("/opt/airflow/sql/create_views.sql")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='dag_load_to_db',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:

    criar_tabelas_task = PythonOperator(
        task_id="criar_tabelas_no_postgres",
        python_callable=create_tables
    )

    task_clientes = PythonOperator(
        task_id='etl_clientes',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'clientes.csv', 'table_name': 'clientes'}
    )

    task_empresas = PythonOperator(
        task_id='etl_empresas',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'empresas.csv', 'table_name': 'empresas'}
    )

    
    task_receitas = PythonOperator(
        task_id='etl_receitas',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'receitas.csv', 'table_name': 'receitas'}
    )

    task_despesas = PythonOperator(
        task_id='etl_despesas',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'despesas.csv', 'table_name': 'despesas'}
    )

    task_orcamentos = PythonOperator(
        task_id='etl_orcamentos',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'orcamentos.csv', 'table_name': 'orcamentos'}
    )

    task_transferencias = PythonOperator(
        task_id='etl_transferencias',
        python_callable=run_etl,
        op_kwargs={'csv_filename': 'transferencias.csv', 'table_name': 'transferencias'}
    )

    
    criar_views_task = PythonOperator(
        task_id="criar_views",
        python_callable=create_view
    )


    criar_tabelas_task >> [task_empresas, task_clientes] 
    [task_empresas, task_clientes] >> task_receitas
    [task_empresas, task_clientes] >> task_despesas
    task_empresas >> task_orcamentos
    task_empresas >> task_transferencias
    [task_receitas, task_despesas, task_orcamentos, task_transferencias] >> criar_views_task
