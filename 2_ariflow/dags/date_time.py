
from airflow import DAG
from airflow.decorators import task
import pendulum



with DAG(
    dag_id = "date_time_string",
    schedule_interval= '@hourly',
    start_date=pendulum.datetime(2022, 12, 19, tz="Asia/Seoul")
) as dag:
    @task(task_id="date_time_string")
    def date_time():
        datetime_str = pendulum.now("Asia/Seoul").to_datetime_string()
        f = open(f"{datetime_str}.txt", "w")
        f.close()
    
    date_time()
    