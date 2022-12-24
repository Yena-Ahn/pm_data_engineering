import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from user_agents import parse

def clean_df():
    df = pd.read_csv("1_pandas/raw_data.csv", index_col=0, dtype={"question":int, "response":int})
    df.drop(["question", "response"], axis=1, inplace=True)
    df.drop(df[df.state.isnull()].index, inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[["result_id", "userAgent" ,"slug", "resultCode"]]
    return df

def parse_userAgent(df):
    for i, row in df["userAgent"].iteritems():
        if isinstance(row,str):
            if row == "Android" or row == "whoscall|android":
                continue
            parsed = parse(row)
            if parsed.device.brand == "Generic_Android":
                if "LM" in parsed.device.model or "LGM" in parsed.device.model:
                    df.at[i, "device.brand"] = "LG"
                # elif "Xiaomi" in row or "POCOPHONE" in parsed.device.family:
                #     df.at[i, "device.brand"] = "Xiaomi"
                elif "A9" in row or "A9" in parsed.device.family:
                    df.at[i, "device.brand"] = "Samsung"
                else:
                    df.at[i, "device.brand"] = parsed.device.brand
            if parsed.device.brand != "Generic_Android":
                df.at[i, "device.brand"] = parsed.device.brand
            if "iPad_Mac" in row:
                df.at[i,"device"] = "tablet"
                df.at[i, "device.brand"] = "Apple"
            if parsed.is_mobile:
                df.at[i, "device"] = "mobile"
            elif parsed.is_pc:
                df.at[i, "device"] = "pc"
            elif parsed.is_tablet:
                df.at[i,"device"] = "tablet"
            df.at[i, "browser"] = parsed.browser.family
            if parsed.browser.family in ['Chrome Mobile WebView','Mobile Safari UI/WKWebView','Crosswalk']:
                df.at[i, "environment"] = "app"
            elif parsed.browser.family in ['Instagram', 'LINE', 'Facebook','Pinterest', 'Apple Mail', 'Twitter', 'Snapchat']:
                df.at[i, "environment"] = "inapp"
            else:
                df.at[i, "environment"] = "web"

def resultCode_graph(df):
    fig, axs = plt.subplots(4,4, figsize=(100,100))
    i = 0
    flag = False
    content_l = df.slug.unique()
    data = df.groupby(["slug", "resultCode"])["result_id"].count().reset_index(name="count")
    for row in range(4):
        for col in range(4):
            if row == 3 and col == 3:
                flag = True
                break
            if not isinstance(content_l[i], str):
                i += 1
            o = data[data.slug == content_l[i]][["resultCode", "count"]]
            pie = o.plot.pie(y="count", ax=axs[row,col], labels=o.resultCode, title=content_l[i], figsize=(20,20), autopct='%.0f%%', legend=False, )
            i += 1
        if flag == True:
            break
    fig.suptitle("% Result Code of Content")
    fig.savefig("1_pandas/2/resultCode-content.png")
    print("A graph is saved at '1_pandas/2/resultCode-content.png'")

def environment_graph(df):
    env_df = df.groupby(["environment"])["result_id"].count()
    dev_df = df.groupby(["device"])["result_id"].count()
    fig, axs = plt.subplots(1,2)
    env_df.plot.pie(ax=axs[0], ylabel="", title="User Environment", autopct='%.0f%%')
    dev_df.plot.pie(autopct='%.0f%%', title="User Device", ylabel="", ax=axs[1])
    fig.savefig("1_pandas/2/user_env_device.png")
    print("A graph is saved at '1_pandas/2/user_env_device.png'")

def tendency_resultcode(df):
    fig, axs = plt.subplots(4,4, figsize=(100,100))
    
    i = 0
    flag = False
    content_l = df.slug.unique()
    random1 = df.groupby(["slug", "resultCode", "environment"])["result_id"].count().reset_index(name="count")
    for row in range(4):
        for col in range(4):
            if i >= 16:
                flag = True
                break
            if not isinstance(content_l[i], str):
                i += 1
            data = random1[random1.slug == content_l[i]][["resultCode", "environment", "count"]]
            pivot = data.pivot("resultCode", "environment", "count")
            if "app" not in pivot:
                pivot["app"] = None
            if "web" not in pivot:
                pivot["web"] = None
            if "inapp" not in pivot:
                pivot["inapp"] = None
            pivot=pivot.fillna(0)
            
            main_category = list(data.resultCode.unique())
            x_axis = np.arange(len(main_category))
            sub_category = ["app", "inapp", "web"]
            num_sub_category = 3
            width = 1/num_sub_category*0.85
            config_tick = dict()
            config_tick['ticks'] = [t + width*(num_sub_category-1)/2 for t in x_axis]
            config_tick["labels"] = main_category
            colors = sns.color_palette('hls',num_sub_category)
            axs[row,col].set_xticks(config_tick["ticks"])
            axs[row,col].set_xticklabels(config_tick["labels"], fontsize=20)
            for j in range(num_sub_category):
                axs[row,col].bar(x_axis+width*j, pivot[sub_category[j]], width, label=sub_category[j], color=colors[j])
            axs[row,col].set_title(content_l[i], fontsize=40)
            axs[row,col].legend(sub_category)
            i += 1
        

        if flag == True:
            break

    fig.suptitle("User Agent Tendency per Result Code of Content", fontsize=100)
    fig.savefig("1_pandas/2/tendency.png")
    print("A graph is saved at '1_pandas/2/tendency.png'")

def main():
    df = clean_df()
    print("Loaded data.")
    print()
    parse_userAgent(df)
    df.to_csv("1_pandas/2/answer.csv")
    resultCode_graph(df)
    environment_graph(df)
    tendency_resultcode(df)
    print("Done.")

main()