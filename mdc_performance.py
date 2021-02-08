import pymysql
import pandas as pd
import numpy as np
import datetime
import copy
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

engine = create_engine('mysql://root:password@localhost:3306/liuzhou?charset=utf8&connect_timeout=5')
conn = pymysql.connect(host="localhost", user="root", password="password", database="liuzhou", port=3306)


def get_columns(table_name):
    columns = engine.execute(
        "SELECT COLUMN_NAME  FROM information_schema.columns WHERE table_name='{}'".format(table_name)).fetchall()
    if len(columns) == 0:
        logging.error('table %s does not exist' % (table_name))
        return
    l = []
    for column in columns:
        l.append(column[0])
    return l


def insert_data(conn, column_num, datalist, table_name):
    def get_sql():
        ns = str(",".join([["%s"] * column_num][0]))
        sql = (''' replace into ''' + table_name + ''' values(''' + ns + ''')''')
        return sql

    try:
        with conn.cursor() as cursor:
            insertsql = get_sql()
            cursor.executemany(insertsql, datalist)
            conn.commit()
            logging.info("导入成功")

    except Exception as e:
        logging.error(e)
        logging.error("导入失败")
        conn.rollback()


def df_to_list(df):
    l = []
    for i in range(len(df)):
        item = list(df.iloc[i, :])
        item[3] = item[3].to_pydatetime()
        item[4] = item[4].to_pydatetime()
        l.append(item)
    return l


def performance_df_to_list(df):
    l = []
    for i in range(len(df)):
        item = list(df.iloc[i, :])
        item[1] = int(item[1])
        item[2] = datetime.datetime(item[2].year, item[2].month, item[2].day, item[2].hour, item[2].minute,
                                    item[2].second)
        item[3] = datetime.datetime(item[3].year, item[3].month, item[3].day, item[3].hour, item[3].minute,
                                    item[3].second)
        item[6] = int(item[6])
        item[7] = float(item[7])
        item[10] = int(item[10])
        item[11] = int(item[11])
        item[12] = float(item[12])
        l.append(item)
    return l


def split_start_time(create_time):
    year = create_time.year
    month = create_time.month
    day = create_time.day
    hour = create_time.hour
    split_time = datetime.datetime(year, month, day, hour, 30, 0)
    if create_time <= split_time:
        return split_time - datetime.timedelta(minutes=30)
    else:
        return split_time


def add_start_end_time(sql):
    df = pd.read_sql(sql, con=engine)
    df['start_time'] = df['create_time']
    df['end_time'] = df['create_time']
    df['create_date'] = df.create_time.apply(lambda x: datetime.date(x.year, x.month, x.day))
    df.drop('create_time', axis=1, inplace=True)
    concat_list = []
    for device_id in set(df['device_id']):
        temp = df.loc[df['device_id'] == device_id, :]
        temp['end_time'] = list(temp.iloc[1:, 4].append(pd.Series(np.nan)))
        concat_list.append(temp)
    final_df = pd.concat(concat_list)
    final_df = final_df.sort_index()
    final_df = final_df.fillna(method='ffill', axis=1)
    return final_df


def get_time(T):
    year = T.year
    month = T.month
    day = T.day
    hour = T.hour
    return datetime.datetime(year, month, day, hour, 0, 0)


def split_item(alist):
    new_list = []
    for data in alist:
        start_hour = get_time(data[3])
        end_hour = get_time(data[4])
        if start_hour == end_hour:
            split_time = start_hour + datetime.timedelta(minutes=30)
            if data[4] > split_time > data[3]:
                copy1 = copy.deepcopy(data)
                copy1[4] = start_hour + datetime.timedelta(minutes=30)
                new_list.append(copy1)
                copy2 = copy.deepcopy(data)
                copy2[3] = copy1[4]
                new_list.append(copy2)
            else:
                new_list.append(data)
        else:
            start_time = data[3]
            if start_time.minute < 30:
                end_time = start_hour
            else:
                end_time = start_hour + datetime.timedelta(minutes=30)
            while True:
                copy3 = copy.deepcopy(data)
                copy3[3] = start_time
                start_date = datetime.date(copy3[3].year, copy3[3].month, copy3[3].day)
                if 0 <= copy3[3].hour < 5:
                    copy3[5] = start_date - datetime.timedelta(days=1)
                else:
                    copy3[5] = start_date
                end_time += datetime.timedelta(minutes=30)
                if end_time >= data[4]:
                    copy3[4] = data[4]
                    new_list.append(copy3)
                    break
                copy3[4] = end_time
                start_time = copy3[4]
                new_list.append(copy3)
    return new_list


def get_create_date(create_time):
    year = create_time.year
    month = create_time.month
    day = create_time.day
    hour = create_time.hour
    if 0 <= hour < 5:
        return datetime.date(year, month, day) - datetime.timedelta(days=1)
    else:
        return datetime.date(year, month, day)


def merge_data(new_list):
    new_list2 = []
    i = 0
    while i < len(new_list):
        data = new_list[i]
        if data[4] == data[3]:
            i += 1
            continue
        elif datetime.timedelta(0) < data[4] - data[3] < datetime.timedelta(minutes=30):
            start_time = data[3]
            end_time = data[4]
            start_hour = get_time(start_time)
            end_hour = get_time(end_time)
            copy1 = copy.deepcopy(data)
            if start_hour == end_hour:
                split_time = start_hour + datetime.timedelta(minutes=30)
                if data[4] <= split_time:
                    copy1[3] = start_hour
                    copy1[4] = split_time
                else:
                    copy1[3] = split_time
                    copy1[4] = split_time + datetime.timedelta(minutes=30)
            else:
                copy1[3] = end_time - datetime.timedelta(minutes=30)
                copy1[4] = end_time
            j = i + 1
            status = set()
            status.add(data[2])
            while j < len(new_list):
                data1 = new_list[j]
                if j + 1 < len(new_list) and data1[1] == new_list[j + 1][1]:
                    if data1[4] == data[3]:
                        status.add(data1[2])
                        j += 1
                        continue
                    if data1[4] <= copy1[4]:
                        status.add(data1[2])
                        j += 1
                    else:
                        i = j
                        break
                else:
                    i = j
                    break
            if 1 in status:
                copy1[2] = 1
            elif 2 in status:
                copy1[2] = 2
            elif 3 in status:
                copy1[2] = 3
            else:
                copy1[2] = 0
            new_list2.append(copy1)
        else:
            new_list2.append(data)
            i += 1
    return new_list2


def get_performance_data(product_sql, status_sql, device_sql, mes_code_sql, item_cap_sql):
    status_t_df = add_start_end_time(status_sql)
    status_t_list = df_to_list(status_t_df)
    status_t_splited = split_item(status_t_list)
    list_merged = merge_data(status_t_splited)
    df_merged = pd.DataFrame(list_merged,
                             columns=['id', 'device_id', 'status', 'start_time', 'end_time', 'create_date'])
    df_merged['create_date'] = df_merged.start_time.apply(get_create_date)
    df_merged = df_merged[df_merged['create_date'] >= (datetime.date.today() - datetime.timedelta(days=2))]
    status_df = pd.DataFrame(status_t_splited,
                             columns=['id', 'device_id', 'status', 'start_time', 'end_time', 'create_date'])
    status_df['create_date'] = status_df.start_time.apply(get_create_date)
    status_df['status1'] = status_df.status.apply(lambda x: 1 if x == 1 else 0)
    status_df['status2'] = status_df.status.apply(lambda x: 0 if x == 0 else 1)
    status_df['duration'] = (status_df['end_time'] - status_df['start_time']).apply(lambda x: x.seconds)
    status_df['work_time'] = status_df.duration * status_df['status1']
    status_df['non_shutdown_time'] = status_df.duration * status_df['status2']
    status_df['start_time'] = status_df.start_time.apply(get_start_time)
    status_df['end_time'] = status_df.end_time.apply(get_end_time)
    status_df1 = status_df.groupby(['device_id', 'create_date', 'start_time', 'end_time'], as_index=False)[
        'duration', 'work_time', 'non_shutdown_time'].sum()
    status_df1 = status_df1[status_df1['duration'] > 0]
    merged1 = pd.merge(df_merged, status_df1, how='left', on=['device_id', 'create_date', 'start_time', 'end_time'])
    product_df = pd.read_sql(product_sql, con=engine)
    device_df = pd.read_sql(device_sql, con=engine)
    product_df['start_time'] = product_df.create_time.apply(split_start_time)
    product_df['end_time'] = product_df.start_time + datetime.timedelta(minutes=30)
    new_product_df = product_df.groupby(['device_id', 'product_id', 'start_time', 'end_time'], as_index=False)[
        'addnum'].sum()
    mes_df = pd.read_sql(mes_code_sql, con=engine).drop_duplicates()
    item_cap_df = pd.read_sql(item_cap_sql, con=engine)
    merged2 = pd.merge(new_product_df, mes_df, how='left', left_on=['device_id', 'product_id'],
                       right_on=['device_code', 'mdc_product_item_code'])
    merged2['new_product_id'] = merged2['product_item_code'].fillna(merged2['product_id'])
    merged3 = pd.merge(merged2, item_cap_df, how='left', left_on=['new_product_id'], right_on=['ITEM_NUMBER'])
    merged3['new_take_time'] = merged3.take_time.fillna(merged3.product_time_per_item)
    merged3['new_take_time'] = merged3.new_take_time.fillna(0)
    merged32 = merged3.groupby(['device_id', 'start_time', 'end_time', 'addnum', 'product_id'], as_index=False)[
        'new_take_time'].mean()
    merged32['total_time'] = merged32['addnum'] * merged32['new_take_time']
    merged33 = merged32.groupby(['device_id', 'start_time', 'end_time'], as_index=False)['addnum', 'total_time'].sum()
    merged4 = pd.merge(merged1, merged33, how='left', on=['device_id', 'start_time', 'end_time'])
    merged4['addnum'] = merged4.addnum.fillna(0)
    merged4['total_time'] = merged4.total_time.fillna(0)
    new_df_merged = pd.merge(merged4, device_df, on=['device_id'], how='left')
    new_df_merged['performance'] = new_df_merged.apply(lambda x: devide(x, 'total_time', 'non_shutdown_time'), axis=1)
    new_df_merged['performance'] = new_df_merged['performance'].fillna(0)
    new_df = parse_performance(new_df_merged)
    return new_df


def get_and_fill_performance(new_df_merged):
    ST_MDC_DEVICE_PERFORMANCE = pd.DataFrame()
    ST_MDC_DEVICE_PERFORMANCE['YMD'] = new_df_merged['create_date']
    ST_MDC_DEVICE_PERFORMANCE['UNITCODE'] = 10119
    ST_MDC_DEVICE_PERFORMANCE['START_TIME'] = new_df_merged['start_time']
    ST_MDC_DEVICE_PERFORMANCE['END_TIME'] = new_df_merged['end_time']
    ST_MDC_DEVICE_PERFORMANCE['DEVICE_ID'] = new_df_merged['device_id']
    ST_MDC_DEVICE_PERFORMANCE['DEVICE_NAME'] = new_df_merged['device_name']
    ST_MDC_DEVICE_PERFORMANCE['DEVICE_STATUS'] = new_df_merged['status']
    ST_MDC_DEVICE_PERFORMANCE['ACTUAL_PRODUCTION'] = new_df_merged.addnum.fillna(0)
    ST_MDC_DEVICE_PERFORMANCE['PLANNED_PRODUCTION'] = None
    ST_MDC_DEVICE_PERFORMANCE['UTILIZATION_RATIO'] = None
    ST_MDC_DEVICE_PERFORMANCE['DEVICE_PRODUCT_TIME'] = new_df_merged['total_time']
    ST_MDC_DEVICE_PERFORMANCE['NON_SHUTDOWN_TIME'] = new_df_merged['non_shutdown_time']
    ST_MDC_DEVICE_PERFORMANCE['PERFORMANCE'] = new_df_merged['performance_y']
    return ST_MDC_DEVICE_PERFORMANCE


def devide(x, a, b):
    try:
        return x[a] / x[b]
    except Exception as e:
        return 0


def get_yesterday_data():
    today = datetime.date.today()
    if 0 <= today.day < 5:
        today = today - datetime.timedelta(days=2)
    else:
        today = today - datetime.timedelta(days=1)
    return today


def get_start_time(x):
    if 0 <= x.minute < 30:
        return datetime.datetime(x.year, x.month, x.day, x.hour, 0, 0)
    elif 30 <= x.minute < 60:
        return datetime.datetime(x.year, x.month, x.day, x.hour, 30, 0)
    else:
        return x.to_pydatetime()


def get_end_time(x):
    if 0 <= x.minute < 30:
        if x.minute == 0 and x.second == 0:
            return datetime.datetime(x.year, x.month, x.day, x.hour, 0, 0)
        else:
            return datetime.datetime(x.year, x.month, x.day, x.hour, 30, 0)
    elif 30 <= x.minute < 60:
        if x.minute == 30 and x.second == 0:
            return datetime.datetime(x.year, x.month, x.day, x.hour, 30, 0)
        else:
            return datetime.datetime(x.year, x.month, x.day, x.hour, 0, 0) + datetime.timedelta(hours=1)
    else:
        return x.to_pydatetime()


def parse_performance(new_df_merged):
    lst = []
    i = 0
    while i <= len(new_df_merged) - 1:
        if new_df_merged.iloc[i, 2] == 1:
            j = i
            lst.append(j)
            i += 1
            while i <= len(new_df_merged) - 1:
                if new_df_merged.iloc[i, 2] == 1:
                    lst.append(j)
                    i += 1
                else:
                    lst.append(0)
                    i += 1
                    break
        else:
            lst.append(0)
            i += 1
    new_df_merged['continous'] = lst
    new_p = new_df_merged.groupby(['device_id', 'continous'], as_index=False)['performance'].mean()
    new_p.loc[new_p.continous == 0, 'performance'] = 0
    return pd.merge(new_df_merged, new_p, how='left', on=['device_id', 'continous'])


time1 = str(datetime.date.today() - datetime.timedelta(days=3))
time2 = str(datetime.date.today() - datetime.timedelta(days=2))
status_sql = "select id,device_id,device_status,create_time from ods_mdc_product_status_t where create_time >= '{}' order by device_id,create_time".format(
    time1)
device_sql = "select device_id,device_name from ods_mdc_device_t"
product_sql = "select device_id,product_id,addnum,create_time from ods_mdc_product_t where create_time >= '{}' and addnum > 0 and product_id is not null".format(
    time2)
mes_code_sql = 'select t.device_code,t.mdc_product_item_code,t.product_item_code,t.take_time from T_MDC_BASIC_MES_PROD t'
item_cap_sql = "select ITEM_NUMBER, product_time_per_item from ST_MDC_MES_ITEM_CAPACITY"
try:
    performance_data = get_performance_data(product_sql, status_sql, device_sql, mes_code_sql, item_cap_sql)
    performance_df = get_and_fill_performance(performance_data)
    performance_list = performance_df_to_list(performance_df)
    insert_data(conn, 13, performance_list, 'ST_MDC_DEVICE_PERFORMANCE')
except Exception as e:
    print(e)
finally:
    conn.close()
