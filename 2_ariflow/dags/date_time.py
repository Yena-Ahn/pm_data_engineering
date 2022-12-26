
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import pendulum

def date_time():
    datetime_str = pendulum.now("Asia/Seoul").to_datetime_string()
    f = open(f"{datetime_str}.txt", "w")
    f.close()

with DAG(
    dag_id = "date_time_string",
    schedule_interval= '@hourly',
    start_date=pendulum.datetime(2022, 12, 19, tz="Asia/Seoul")
) as dag:
    date_time_string_task = PythonOperator(task_id="date_time_string",
                                    python_callable=date_time,
                                    dag=dag)

    date_time_string_task
