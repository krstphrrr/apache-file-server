from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utilityfuns import create_command


with DAG('table_setup', start_date=datetime(2022,1,1),
         schedule_interval='@daily', catchup=False) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='1_external_postgres_local',
        sql=create_command('geoIndicators')
    )