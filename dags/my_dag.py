from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import os
import pandas as pd
# import 
path = os.path.join(os.getcwd(),"dags","csv","dataHeader.csv")


def choose_best(ti):
  print("PULLING JSON DATAFRAME>>>")
  pulled_df = ti.xcom_pull(task_ids=[
    'read_csv',
    # 'test_task_2',
    # 'test_task_3'
  ])
  print(pulled_df)
  df = pd.read_json(pulled_df)
  best_accuracy = max(len(df.columns))
  
  print(best_accuracy)
  if (best_accuracy > 8):
    return 'accurate'
  return 'inaccurate'

def test_function():
  df = pd.read_csv(path).to_json()
  print("DONE WITH READING + CONVERTING")
  return df

with DAG("my_dag", start_date=datetime(2021,1,1),
    schedule_interval="@daily", catchup=False) as dag:

          task_1 = PythonOperator(
            task_id="read_csv",
            python_callable=test_function
          )

          # task_2 = PythonOperator(
          #   task_id="test_task_2",
          #   python_callable=test_function
          # )

          # task_3 = PythonOperator(
          #   task_id="test_task_3",
          #   python_callable=test_function
          # )

          choose_task = BranchPythonOperator(
            task_id="choose_best",
            python_callable=choose_best
          )

          accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
          )

          inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
          )

          [
              task_1,
              # task_2,
              # task_3
              ] >> choose_task >> [accurate, inaccurate]

