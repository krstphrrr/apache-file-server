from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

with DAG('table_setup', start_date=datetime(2022,1,1),
         schedule_interval='@daily', catchup=False) as dag:
    
    from utilityfuns import create_command
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='1_external_postgres_local',
        sql=create_command('geoIndicators')
    )
    from utilityfuns import _files_available
    are_files_available = PythonOperator(
        task_id='files_available',
        python_callable=_files_available
    )

    create_table >> are_files_available