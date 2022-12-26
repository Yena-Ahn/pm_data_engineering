import pandas as pd
import os
import gzip
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from dynamodb_json import json_util

def parse_data():
    for file in os.listdir("1_pandas/data"):
        if not file.startswith("."):
            json_f = gzip.open("1_pandas/data/"+file, "rb")
            for line in json_f:
                yield json_util.loads(line.decode())

def convert_to_df():
    count = 0
    dict_list = []
    for dictionary in parse_data():
        dict_list.append(dictionary["Item"])
        count += 1
        if count % 20000 == 0:
            print(f"Processing Data... {count}")
    print("Returning a dataframe...")
    final_df = pd.DataFrame(dict_list)
    print("Done.")
    final_df.to_csv("1_pandas/raw_data.csv")
    return final_df

def convert_date_week_hour(row, option):
    if option == "w":
        return row.strftime("%A")
    if option == "h":
        return int(row.strftime("%-H"))
    if option == "d":
        return row.strftime("%Y-%m-%d")

def save_data(df, name):
    df.to_csv("1_pandas/1/" + name)
    print(f"Data processed and saved in '{name}'")
    
def clean_data():
    df = convert_to_df()
    df["created_at"] = pd.to_datetime(df["created_at"], format="%Y-%m-%d %H:%M:%S")
    # df = df.drop(["question", "response"], axis=1) # answer data not needed

    # number of plays of each content by date - "result_id", "slug", "created_at"
    df = df[["result_id", "created_at", "slug"]]

    # df = df.drop_duplicates(ignore_index=True) # rows were created with each question-answer so remain only unique

    # add week no., hour and date cols
    df["week"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "w"))
    df["hour"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "h"))
    df["date"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "d"))
    return df

def count_contents_play(df):
    df = df.groupby(["slug", "date"], dropna=False).size().reset_index(name="no. visitor")
    df.columns = ["content", "date", "no. visitors"]
    return df

def ylabel():
    ylabel = []
    for x in range(24):
        dt = datetime.time(x)
        ylabel.append(dt.strftime("%p %-I:%M"))
    return ylabel

def time_week_pivot(df):
    df = df.groupby(["week", "hour"]).size().reset_index(name="no. visitors")
    df["week"] = pd.Categorical(df["week"], ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    df = df.sort_values("week")
    pivot_table = df.pivot("hour", "week", "no. visitors") # yaxis, xaxis, value
    return pivot_table

def create_heatmap(pivot):
    plt.figure(figsize=(28,30))
    sns.set(font_scale=2)
    plot = sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt="d", linewidths=0.3, yticklabels=ylabel())
    plot.figure.savefig("1_pandas/1/heatmap.png")
    print("Heatmap saved in 'heatmap.png'.")

def main():
    df = clean_data()
    save_data(df, 'answer.csv')
    # play count
    print()
    print("1-1 Count of play of each content by date")
    play_df = count_contents_play(df)
    print()
    print("[Number of Content Play by Date]")
    print(play_df)
    # week-hour heatmap
    print()
    print("1-2 Week-hour visitors")
    time_df = time_week_pivot(df)
    create_heatmap(time_df)
    

main()