import pandas as pd
import json
import os
import gzip
import datetime
import seaborn as sns
import matplotlib.pyplot as plt

def parse_data():
    for file in os.listdir("1_pandas/data"):
        if not file.startswith("."):
            json_f = gzip.open("1_pandas/data/"+file, "rb")
            for line in json_f:
                yield json.loads(line.decode())

def to_dict(i):
    item = i["Item"]
    dict = {}
    for key in item.keys():
        if "S" in item[key]:
            dict[key] = item[key]["S"]
        elif "N" in item[key]:
            dict[key] = int(item[key]["N"])
        elif "M" in item[key]:
            dict["question"] = []
            dict["response"] = []
            for qID in item[key]["M"].keys():
                dict["question"] += [int(qID)]
                dict["response"] += [int(item[key]["M"][qID]["N"])]
        elif key == "state":
            dict[key] = item[key]["NULL"]
        else:
            dict[key] = item[key]
    return dict

def yield_dictionary():
    for i in parse_data():
        yield to_dict(i)


def to_df():
    count = 0
    dict_list = []
    for dictionary in yield_dictionary():
        dict_list.append(dictionary)
        count += 1
        if count % 20000 == 0:
            print(f"Processing Data... {count}")
    print("Returning a dataframe...")
    final_df = pd.DataFrame(dict_list)
    print("Done.")
    final_df.to_csv("1_pandas/raw_data.csv")
    return final_df #20mins

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

def save_data(df, name):
    df.to_csv("1_pandas/1/" + name)
    print(f"Data processed and saved in '{name}'")
    
def data_cleaning():
    df = to_df()
    df = df.drop(["question", "response"], axis=1) # answer data not needed

    # number of plays of each content by date - "result_id", "slug", "created_at"
    df = df[["result_id", "created_at", "slug"]]

    df = df.drop_duplicates(ignore_index=True) # rows were created with each question-answer so remain only unique

    # add week no., hour and date cols
    df["week"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "w"))
    df["hour"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "h"))
    df["date"] = df["created_at"].apply(lambda row : convert_date_week_hour(row, "d"))
    return df

def play_count(df):
    df = df.groupby(["slug", "date"], dropna=False).size().reset_index(name="no. visitor")
    df.columns = ["content", "date", "no. visitors"]
    return df

def ylabel():
    ylabel = []
    for x in range(24):
        if x < 12:
            ylabel.append("AM " + str(x) + ":00")
        else:
            ylabel.append("PM " + str(x) + ":00")
    return ylabel

def time_week(df):
    df = df.groupby(["week", "hour"]).size().reset_index(name="no. visitors")
    df["week"] = pd.Categorical(df["week"], ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    df = df.sort_values("week")
    pivot_table = df.pivot("hour", "week", "no. visitors") # yaxis, xaxis, value
    return pivot_table

def heatmap(pivot):
    plt.figure(figsize=(28,30))
    sns.set(font_scale=2)
    plot = sns.heatmap(pivot, cmap="YlGnBu", annot=True, fmt="d", linewidths=0.3, yticklabels=ylabel())
    plot.figure.savefig("1_pandas/1/heatmap.png")
    print("Heatmap saved in 'heatmap.png'.")

def main():
    df = data_cleaning()
    save_data(df, 'answer.csv')
    # play count
    print()
    print("1-1 Count of play of each content by date")
    play_df = play_count(df)
    print()
    print("[Number of Content Play by Date]")
    print(play_df)
    # week-hour heatmap
    print()
    print("1-2 Week-hour visitors")
    time_df = time_week(df)
    heatmap(time_df)
    

main()