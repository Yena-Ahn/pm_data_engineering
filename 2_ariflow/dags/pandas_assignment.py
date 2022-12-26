import pandas as pd
import json
import os
import gzip
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from dynamodb_json import json_util
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum


def parse_data():
    for file in os.listdir("/opt/airflow/data"):
        if not file.startswith("."):
            json_f = gzip.open("/opt/airflow/data/"+file, "rb")
            for line in json_f:
                yield json_util.loads(line.decode())

def save_data(df, name):
    df.to_csv("/opt/airflow/saved/" + name)
    print(f"Data processed and saved in '{name}'")

def convert_date_week_hour(row, option):
    if option == "w":
        return row.strftime("%A")
    if option == "h":
        return int(row.strftime("%-H"))
    if option == "d":
        return row.strftime("%Y-%m-%d")

def ylabel():
    ylabel = []
    for x in range(24):
        dt = datetime.time(x)
        ylabel.append(dt.strftime("%p %-I:%M"))
    return ylabel


def convert_to_df():
    df_list = []
    for dictionary in parse_data():
        df_list += dictionary["Item"],
    final_df = pd.DataFrame(df_list)
    del df_list
    print("Done.")
    final_df.to_csv("/opt/airflow/saved/raw_data.csv")


def clean_data():
    df = pd.read_csv("/opt/airflow/saved/raw_data.csv")
    df["created_at"] = pd.to_datetime(df["created_at"], format="%Y-%m-%d %H:%M:%S")

    # number of plays of each content by date - "result_id", "slug", "created_at"
    df = df[["result_id", "created_at", "slug"]]

    # add week no., hour and date cols
    df["week"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "w"))
    df["hour"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "h"))
    df["date"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "d"))
    save_data(df, "answer.csv")


def count_contents_play():
    df = pd.read_csv("/opt/airflow/saved/answer.csv")
    df = df.groupby(["slug", "date"], dropna=False).size().reset_index(name="no. visitor")
    df.columns = ["content", "date", "no. visitors"]
    print(df)


def time_week_heatmap():
    df = pd.read_csv("/opt/airflow/saved/answer.csv")
    df = df.groupby(["week", "hour"]).size().reset_index(name="no. visitors")
    df["week"] = pd.Categorical(df["week"], ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    df = df.sort_values("week")
    pivot_table = df.pivot("hour", "week", "no. visitors") # yaxis, xaxis, value
    plt.figure(figsize=(28,30))
    sns.set(font_scale=2)
    plot = sns.heatmap(pivot_table, cmap="YlGnBu", annot=True, fmt="d", linewidths=0.3, yticklabels=ylabel())
    plot.figure.savefig("/opt/airflow/saved/heatmap.png")
    print("Heatmap saved in 'heatmap.png'.")



with DAG(
    dag_id = "contents_play_count_per_date",
    schedule_interval= '@once',
    start_date=pendulum.datetime(2022, 12, 23, tz="Asia/Seoul")
) as dag:

    t1 = PythonOperator(task_id="convert_to_df",
                        python_callable=convert_to_df,
                        dag=dag)
    
    t2 = PythonOperator(task_id = "clean_data",
                        python_callable=clean_data,
                        dag=dag)
    
    t3 = PythonOperator(task_id = "count_contents_play",
                        python_callable=count_contents_play,
                        dag=dag)
    
    t4 = PythonOperator(task_id = "time_week_heatmap",
                        python_callable=time_week_heatmap,
                        dag=dag)
    
    t1 >> t2 >> [t3, t4]