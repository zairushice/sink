# -*- coding:UTF-8 -*-
import pymysql
import pandas as pd
import os
import numpy as np
import datetime
import logging
import copy
import sys

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %('
                                                'message)s')

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# config = configparser.ConfigParser()
# config.read('/home/airflow/airflow/dags/efficiency/config.ini')
# max_time = config.get('ST_MDC_DEVICE_EFFICIENCY', 'max_time')
max_time = 'None'

conn = pymysql.connect(host="localhost", user="root", password="password", database="liuzhou", port=3306)


def get_datalist(sql):
    datalist = []
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            datalist = cursor.fetchall()
            return datalist
    except Exception as e:
        logging.error(e.args)


def insert_data(column_num, datalist, table_name):
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
        item[4] = datetime.datetime(item[4].year, item[4].month, item[4].day, item[4].hour, item[4].minute,
                                    item[4].second)
        item[5] = datetime.datetime(item[5].year, item[5].month, item[5].day, item[5].hour, item[5].minute,
                                    item[5].second)
        l.append(item)
    return l


def add_start_end_time(alist):
    df = pd.DataFrame(alist, columns=['id', 'device_id', 'status', 'create_date'])
    #    df['create_date'] = df.create_date.apply(lambda x: x[:19])
    #    df['create_date'] = df.create_date.apply(str_to_datetime)
    df['start_time'] = df['create_date']
    df['end_time'] = df['create_date']
    df['create_date'] = df.create_date.apply(lambda x: datetime.date(x.year, x.month, x.day))
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
        start_hour = get_time(data[4])
        end_hour = get_time(data[5])
        if start_hour == end_hour:
            split_time = start_hour + datetime.timedelta(minutes=30)
            if data[5] > split_time > data[4]:
                copy1 = copy.deepcopy(data)
                copy1[5] = start_hour + datetime.timedelta(minutes=30)
                new_list.append(copy1)
                copy2 = copy.deepcopy(data)
                copy2[4] = copy1[5]
                new_list.append(copy2)
            else:
                new_list.append(data)
        else:
            start_time = data[4]
            if start_time.minute < 30:
                end_time = start_hour
            else:
                end_time = start_hour + datetime.timedelta(minutes=30)
            while True:
                copy3 = copy.deepcopy(data)
                copy3[4] = start_time
                start_date = datetime.date(copy3[4].year, copy3[4].month, copy3[4].day)
                # if 0 <= copy3[4].hour < 5:
                #     copy3[3] = start_date - datetime.timedelta(days=1)
                # else:
                #     copy3[3] = start_date
                end_time += datetime.timedelta(minutes=30)
                if end_time >= data[5]:
                    copy3[5] = data[5]
                    new_list.append(copy3)
                    break
                copy3[5] = end_time
                start_time = copy3[5]
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


def parse_product_data(product_data):
    product_df = pd.DataFrame(product_data, columns=['product_id', 'device_id', 'addnum', 'create_time'])
    #    product_df['create_time'] = product_df.create_time.apply(lambda x: x[:19])
    #    product_df['create_time'] = product_df.create_time.apply(str_to_datetime)
    product_df['create_date'] = product_df['create_time'].apply(get_create_date)
    day_product_df = product_df.groupby(['product_id', 'device_id', 'create_date'], as_index=False)['addnum'].sum()
    return day_product_df


def str_to_date(string):
    date_time = datetime.datetime.strptime(string, '%Y-%m-%d')
    year = date_time.year
    month = date_time.month
    day = date_time.day
    return datetime.date(year, month, day)


def str_to_datetime(string):
    date_time = datetime.datetime.strptime(string, '%Y-%m-%d %H:%M:%S')
    year = date_time.year
    month = date_time.month
    day = date_time.day
    hour = date_time.hour
    minute = date_time.minute
    second = date_time.second
    return datetime.datetime(year, month, day, hour, minute, second)


def get_dayily_data(status_t, device_t, product_t, product_mes_t, mes_code_t, item_cap_t):
    status_t_df = add_start_end_time(status_t)
    status_t_list = df_to_list(status_t_df)
    status_t_splited = split_item(status_t_list)
    status_df = pd.DataFrame(status_t_splited,
                             columns=['id', 'device_id', 'status', 'create_date', 'start_time', 'end_time'])
    status_df['create_date'] = status_df.start_time.apply(get_create_date)
    status_df['status1'] = status_df.status.apply(lambda x: 1 if x == 1 else 0)
    status_df['status2'] = status_df.status.apply(lambda x: 0 if x == 0 else 1)
    status_df['duration'] = (status_df['end_time'] - status_df['start_time']).apply(lambda x: x.seconds)
    status_df['work_time'] = status_df.duration * status_df['status1']
    status_df['non_shutdown_time'] = status_df.duration * status_df['status2']
    day_df = status_df.groupby(['device_id', 'create_date'], as_index=False)['work_time', 'non_shutdown_time'].sum()
    day_df = day_df[day_df['create_date'] >= (datetime.date.today() - datetime.timedelta(days=2))]
    product_df = parse_product_data(product_t)
    item_cap_df = pd.DataFrame(item_cap_t, columns=['ITEM_NUMBER', 'product_time_per_item'])
    mes_code_df = pd.DataFrame(mes_code_t,
                               columns=['device_code', 'mdc_product_item_code', 'product_item_code', 'take_time'])
    mes_code_df = mes_code_df.drop_duplicates()
    mes_code_df['device_code'] = mes_code_df.device_code.astype('str')
    merged2 = pd.merge(product_df, mes_code_df, how='left', left_on=['device_id', 'product_id'],
                       right_on=['device_code', 'mdc_product_item_code'])
    merged2['new_product_id'] = merged2['product_item_code'].fillna(merged2['product_id'])
    merged2['addnum'] = merged2['addnum'].fillna(0)
    product_mes_df = pd.DataFrame(product_mes_t,
                                  columns=['ITEM_NUMBER', 'DATA_DATE', 'COMPLETE_NUM', 'DEFECTIVE_NUM'])
    if isinstance(product_mes_df['DATA_DATE'][0], str):
        product_mes_df['DATA_DATE'] = product_mes_df['DATA_DATE'].apply(str_to_date)
    merged3 = pd.merge(merged2, item_cap_df, how='left', left_on=['new_product_id'], right_on=['ITEM_NUMBER'])
    merged3['new_take_time'] = merged3.take_time.fillna(merged3.product_time_per_item)
    merged3['new_take_time'] = merged3.new_take_time.fillna(0)
    merged3 = pd.merge(merged3, product_mes_df, how='left', left_on=['create_date', 'new_product_id'],
                       right_on=['DATA_DATE', 'ITEM_NUMBER'])
    merged31 = merged3.groupby(['device_id', 'create_date', 'addnum', 'product_id'], as_index=False)[
        'new_take_time'].mean()
    merged31['total_time'] = merged31['addnum'] * merged31['new_take_time']
    merged32 = merged3.groupby(['device_id', 'create_date', 'addnum', 'product_id'],
                               as_index=False)['COMPLETE_NUM', 'DEFECTIVE_NUM'].sum()
    merged33 = pd.merge(merged31, merged32, how='left',
                        on=['device_id', 'create_date', 'addnum', 'product_id'])
    lst = []
    for i in range(len(merged33)):
        if merged33.COMPLETE_NUM[i] == 0:
            lst.append(merged33.addnum[i])
        else:
            lst.append(merged33.COMPLETE_NUM[i])
    merged33['COMPLETE_NUM'] = lst
    merged34 = merged33.groupby(['device_id', 'create_date'], as_index=False)[
        'total_time', 'COMPLETE_NUM', 'DEFECTIVE_NUM'].sum()
    merged4 = pd.merge(day_df, merged34, how='left', on=['device_id', 'create_date'])
    merged4['total_time'].fillna(0, inplace=True)
    merged4['COMPLETE_NUM'].fillna(0, inplace=True)
    merged4['DEFECTIVE_NUM'].fillna(0, inplace=True)
    device_df = pd.DataFrame(device_t,
                             columns=['DEVICE_CODE', 'DEVICE_NAME', 'BELONG_DEPARTMENT_DESC', 'BELONG_WORKCENTER_DESC',
                                      'STORAGE_LOCATION', 'MANUFACTURER'])
    merged5 = pd.merge(merged4, device_df, how='left', left_on=['device_id'], right_on=['DEVICE_CODE'])
    return merged5


def get_and_fill_efficiency(daily_data):
    ST_MDC_DEVICE_EFFICIENCY = pd.DataFrame()
    ST_MDC_DEVICE_EFFICIENCY['YMD'] = daily_data['create_date']
    ST_MDC_DEVICE_EFFICIENCY['UNITCODE'] = 10119
    ST_MDC_DEVICE_EFFICIENCY['ORG_NAME'] = '柳州凌云'
    ST_MDC_DEVICE_EFFICIENCY['BELONG_DEPARTMENT_DESC'] = daily_data['BELONG_DEPARTMENT_DESC']
    ST_MDC_DEVICE_EFFICIENCY['BELONG_WORKCENTER_DESC'] = daily_data['BELONG_WORKCENTER_DESC']
    ST_MDC_DEVICE_EFFICIENCY['STORAGE_LOCATION'] = daily_data['STORAGE_LOCATION']
    ST_MDC_DEVICE_EFFICIENCY['MANUFACTURER'] = daily_data['MANUFACTURER']
    ST_MDC_DEVICE_EFFICIENCY['DEVICE_ID'] = daily_data['DEVICE_CODE']
    ST_MDC_DEVICE_EFFICIENCY['DEVICE_NAME'] = daily_data['DEVICE_NAME']
    ST_MDC_DEVICE_EFFICIENCY['WORK_TIME'] = daily_data['work_time']
    ST_MDC_DEVICE_EFFICIENCY['NON_SHUTDOWN_TIME'] = daily_data['non_shutdown_time']
    ST_MDC_DEVICE_EFFICIENCY['SHIFT_TIME'] = 28800
    ST_MDC_DEVICE_EFFICIENCY['PLAN_WORK_TIME'] = None
    ST_MDC_DEVICE_EFFICIENCY['DEVICE_PRODUCT_TIME'] = daily_data['total_time']
    ST_MDC_DEVICE_EFFICIENCY['TARGET_UTILIZATION_RATE'] = None
    ST_MDC_DEVICE_EFFICIENCY['TARGET_OEE'] = None
    ST_MDC_DEVICE_EFFICIENCY['ACTUAL_PRODUCTION'] = daily_data['COMPLETE_NUM'].fillna(0)
    ST_MDC_DEVICE_EFFICIENCY['PLANNED_PRODUCTION'] = None
    ST_MDC_DEVICE_EFFICIENCY['DEFECTIVE_NUM'] = daily_data['DEFECTIVE_NUM'].fillna(0)
    ST_MDC_DEVICE_EFFICIENCY['TIME_EFFICIENCY'] = ST_MDC_DEVICE_EFFICIENCY.apply(
        lambda x: devide(x, 'WORK_TIME', 'NON_SHUTDOWN_TIME'), axis=1)
    ST_MDC_DEVICE_EFFICIENCY['PER_EFFICIENCY'] = ST_MDC_DEVICE_EFFICIENCY.apply(
        lambda x: devide(x, 'DEVICE_PRODUCT_TIME', 'WORK_TIME'), axis=1)
    ST_MDC_DEVICE_EFFICIENCY['FPY'] = ST_MDC_DEVICE_EFFICIENCY.apply(
        lambda x: devide2(x, 'ACTUAL_PRODUCTION', 'DEFECTIVE_NUM', 'ACTUAL_PRODUCTION'), axis=1)
    ST_MDC_DEVICE_EFFICIENCY['OEE'] = ST_MDC_DEVICE_EFFICIENCY['TIME_EFFICIENCY'] * ST_MDC_DEVICE_EFFICIENCY[
        'PER_EFFICIENCY'] * ST_MDC_DEVICE_EFFICIENCY['FPY']
    ST_MDC_DEVICE_EFFICIENCY['TEEP'] = ST_MDC_DEVICE_EFFICIENCY['OEE'] * (480 - 85) / 480
    logging.warning(ST_MDC_DEVICE_EFFICIENCY[ST_MDC_DEVICE_EFFICIENCY['OEE'] > 1])
    return ST_MDC_DEVICE_EFFICIENCY


def devide(x, a, b):
    try:
        return x[a] / x[b]
    except Exception as e:
        return 0


def devide2(x, a, b, c):
    try:
        return x[a] / (x[b] + x[c])
    except Exception as e:
        return 1


def efficiency_df_to_list(df):
    l = []
    for i in range(len(df)):
        item = list(df.iloc[i, :])
        item[1] = int(item[1])
        item[7] = str(item[7])
        item[9] = float(item[9])
        item[10] = float(item[10])
        item[11] = float(item[11])
        item[13] = float(item[13])
        item[16] = float(item[16])
        item[18] = float(item[18])
        item[19] = float(item[19])
        item[20] = float(item[20])
        item[21] = float(item[21])
        item[22] = float(item[22])
        item[23] = float(item[23])
        l.append(item)
    return l


def get_yesterday_data():
    today = datetime.date.today()
    if 0 <= today.day < 5:
        today = today - datetime.timedelta(days=2)
    else:
        today = today - datetime.timedelta(days=1)
    return today


def delete_sql(sql):
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()
            logging.info('删除成功')
    except Exception as e:
        logging.error(e.args)


def get_yesterday():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    year = yesterday.year
    month = yesterday.month
    day = yesterday.day
    return datetime.datetime(year, month, day, 5, 0, 0)


time1 = str(datetime.date.today() - datetime.timedelta(days=3))
time2 = str(datetime.date.today() - datetime.timedelta(days=2))
status_sql = "select id,device_id,device_status,create_time from ods_mdc_product_status_t where create_time >= '{}' order by device_id,create_time".format(
    time1)
device_sql = "select DEVICE_CODE,DEVICE_NAME,BELONG_DEPARTMENT_DESC,BELONG_WORKCENTER_DESC,STORAGE_LOCATION,MANUFACTURER from ST_EAM_COMPANY_STATUS"
product_sql = "select product_id,device_id,addnum,create_time from ods_mdc_product_t where create_time >= '{}' and addnum > 0 and product_id is not null".format(
    time2)
product_mes_sql = "select ITEM_NUMBER,DATA_DATE,COMPLETE_NUM,DEFECTIVE_NUM from ST_MES_ITEM_PRODUCTION"
mes_code_sql = 'select t.device_code,t.mdc_product_item_code,t.product_item_code,t.take_time from T_MDC_BASIC_MES_PROD t'
item_cap_sql = "select ITEM_NUMBER, product_time_per_item from ST_MDC_MES_ITEM_CAPACITY"
status_t = get_datalist(status_sql)
if len(status_t) == 0:
    sys.exit()
device_t = get_datalist(device_sql)
product_t = get_datalist(product_sql)
product_mes_t = get_datalist(product_mes_sql)
mes_code_t = get_datalist(mes_code_sql)
item_cap_t = get_datalist(item_cap_sql)
try:
    daily_data = get_dayily_data(status_t, device_t, product_t, product_mes_t, mes_code_t, item_cap_t)
    efficiency_df = get_and_fill_efficiency(daily_data)
    efficiency_list = efficiency_df_to_list(efficiency_df)
    insert_data(24, efficiency_list, 'ST_MDC_DEVICE_EFFICIENCY')
except Exception as e:
    print(e)
