
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import pendulum

def hello_world():
    with open("hello_world.txt", "w+") as f:
        f.write("Hello world!")
    f.close()

with DAG(
    dag_id = "hello_world",
    schedule_interval= '0 18 * * 5',
    start_date=pendulum.datetime(2022, 12, 19, tz="Asia/Seoul")
) as dag:
    hello_world_task = PythonOperator(task_id="hello_world",
                                    python_callable=hello_world,
                                    dag=dag)
    
    hello_world_task
    