
from airflow import DAG
from airflow.decorators import task
import pendulum



with DAG(
    dag_id = "hello_world",
    schedule_interval= '0 18 * * 5',
    start_date=pendulum.datetime(2022, 12, 19, tz="Asia/Seoul")
) as dag:
    @task(task_id="hello_world")
    def hello_world():
        with open("hello_world.txt", "w+") as f:
            f.write("Hello world!")
        f.close()

    hello_world()
    