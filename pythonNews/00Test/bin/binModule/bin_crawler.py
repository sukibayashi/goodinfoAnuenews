import requests
import pandas as pd
import numpy as np
from tqdm import tqdm
# 使用的時候，記得將ipynb切換成anaconda版本
import ipywidgets as widgets
from IPython.display import display # 用來顯示widgeets
import datetime



# 如果存在则返回True
def table_exist(conn, table):
    return list(conn.execute(
        "select count(*) from sqlite_master where type='table' and name='" + table + "'"))[0][0] == 1

def table_latest_date(conn, table):
    cursor = conn.execute('SELECT date FROM ' + table + ' ORDER BY date DESC LIMIT 1;')
    return datetime.datetime.strptime(list(cursor)[0][0], '%Y/%m')

def update_table(conn, table_name, crawl_function, dates):


    print('start crawl ' + table_name + ' from ', dates[0] , 'to', dates[-1])

    df = pd.DataFrame()
    dfs = {}

    progress = tqdm_notebook(dates, )

    for d in progress:

        print('crawling', d)
        progress.set_description('crawl' + table_name + str(d))

        data = crawl_function(d)

        if data is None:
            print('fail, check if it is a holiday')

        # update multiple dataframes
        elif isinstance(data, dict):
            if len(dfs) == 0:
                dfs = {i:pd.DataFrame() for i in data.keys()}

            for i, d in data.items():
                dfs[i] = dfs[i].append(d)

        # update single dataframe
        else:
            df = df.append(data)
            print('success')


        if len(df) > 50000:
            add_to_sql(conn, table_name, df)
            df = pd.DataFrame()
            print('save', len(df))

        time.sleep(15)



def widget(conn,table_name):
    初始時間 = widgets.DatePicker(
        description='旧资料',
        disabled=False
    )
    if table_exist(conn, table_name):
        初始時間.value = table_latest_date(conn,table_name)
    
    今日時間 = widgets.DatePicker(
        description='到',
        disabled=False
    )
    今日時間.value = datetime.datetime.now().date()
    
    
    更新按鈕 = widgets.Button(description='更新資料 ')
    
    
    
    
    label = widgets.Label('月报表')
    
    items = [初始時間,今日時間,更新按鈕]
    display(widgets.VBox([label,widgets.HBox(items)]))