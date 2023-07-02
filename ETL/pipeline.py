from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import extract
import transform
import load

dag = DAG(
    dag_id="dvdRental_dag", schedule="@daily", start_date=datetime(2023, 7, day=2)
)


extract_task = PythonOperator(
    task_id="extract", python_callable=extract.extract, dag=dag
)

transform_task = PythonOperator(
    task_id="transform", python_callable=transform.transform, dag=dag
)

load_task = PythonOperator(task_id="load", python_callable=load.load, dag=dag)

extract_task >> transform_task >> load_task
