import pandas as pd
import json
import os
import gzip
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.decorators import task
import pendulum


def parse_data():
    for file in os.listdir("/opt/airflow/data"):
        if not file.startswith("."):
            json_f = gzip.open("/opt/airflow/data/"+file, "rb")
            for line in json_f:
                yield json.loads(line.decode())

def save_data(df, name):
    df.to_csv("/opt/airflow/saved/" + name)
    print(f"Data processed and saved in '{name}'")

def to_dict(i):
    item = i["Item"]
    dict = {}
    for key in item.keys():
        if "S" in item[key]:
            dict[key] = item[key]["S"]
        elif "N" in item[key]:
            dict[key] = int(item[key]["N"])
        elif "M" in item[key]:
            continue
            # dict["question"] = []
            # dict["response"] = []
            # for qID in item[key]["M"].keys():
            #     dict["question"] += [int(qID)]
            #     dict["response"] += [int(item[key]["M"][qID]["N"])]
        elif key == "state":
            dict[key] = item[key]["NULL"]
        else:
            dict[key] = item[key]
    del item
    return dict


def yield_dataframe():
    for i in parse_data():
        # dict = 
        yield to_dict(i)

# def to_df():
#     df_list = []
#     count = 0
#     for df in yield_dataframe():
#         df_list += df,
#         count += 1
#         if count % 20000 == 0:
#             print(f"Processing Data... {count}")
#     print("Returning a dataframe...")
#     final_df = pd.concat(df_list, ignore_index=True)
#     del df_list
#     print("Done.")
#     final_df.to_csv("/opt/airflow/saved/raw_data.csv")
#     # return final_df #20mins

def convert_date_week_hour(row, option):
    if option == "w":
        weekday = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        if row[-1].isalpha():
            week_idx = datetime.datetime.fromisoformat(row[:-1]).weekday()
            return weekday[week_idx]
        week_idx = datetime.datetime.fromisoformat(row).weekday()
        return weekday[week_idx]
    if option == "h":
        if row[-1].isalpha():
            return datetime.datetime.fromisoformat(row[:-1]).time().hour
        return datetime.datetime.fromisoformat(row).time().hour
    if option == "d":
        return datetime.datetime.fromisoformat(row[:10]).date()


def ylabel():
    ylabel = []
    for x in range(24):
        if x < 12:
            ylabel.append("AM " + str(x) + ":00")
        else:
            ylabel.append("PM " + str(x) + ":00")
    return ylabel



with DAG(
    dag_id = "contents_play_count_per_date",
    schedule_interval= '@once',
    start_date=pendulum.datetime(2022, 12, 23, tz="Asia/Seoul")
) as dag:
    

    @task
    def to_df():
        df_list = []
        # count = 0
        for dictionary in yield_dataframe():
            df_list += dictionary,
        #     count += 1
        #     if count % 20000 == 0:
        #         print(f"Processing Data... {count}")
        # print("Returning a dataframe...")
        final_df = pd.DataFrame(df_list)
        del df_list
        # del df
        print("Done.")
        final_df.to_csv("/opt/airflow/saved/raw_data.csv")
        # return final_df #20mins
    
    @task
    def data_cleaning():
        df = pd.read_csv("/opt/airflow/saved/raw_data.csv")
        # df = df.drop(["question", "response"], axis=1) # answer data not needed

        # number of plays of each content by date - "result_id", "slug", "created_at"
        df = df[["result_id", "created_at", "slug"]]

        df = df.drop_duplicates(ignore_index=True) # rows were created with each question-answer so remain only unique

        # add week no., hour and date cols
        df["week"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "w"))
        df["hour"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "h"))
        df["date"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "d"))
        save_data(df, "answer.csv")
    
    
    
    @task
    def play_count():
        df = pd.read_csv("/opt/airflow/saved/answer.csv")
        df = df.groupby(["slug", "date"], dropna=False).size().reset_index(name="no. visitor")
        df.columns = ["content", "date", "no. visitors"]
        print(df)
    
    @task
    def time_week():
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


    to_df = to_df()
    data_cleaning = data_cleaning()
    play_count = play_count()
    time_week = time_week()
    
    to_df >> data_cleaning >> [play_count, time_week]