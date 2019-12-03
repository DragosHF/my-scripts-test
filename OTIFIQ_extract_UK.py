import mysql.connector
import pyodbc
import pandas as pd
import yaml
import os
from pathlib import Path
import numpy as np
# import datetime as dt

# file structure
cwd = Path(os.getcwd())
sqls_path = cwd/'sqls'
sql_fcms_uk = sqls_path/'fcms_uk.sql'
sql_dd = sqls_path/'dd.sql'

# read DB credentials, host, port etc from config.yml
with open('config.yml', 'r') as config_f:
    config = yaml.safe_load(config_f)

user_imp = os.getenv('jump_cloud_id')
pwd_imp = os.getenv('jump_cloud_pwd')
host_imp = config['DWH']['host']
driver_imp = config['DWH']['driver']


def fcms_to_df(sql_file):
    conn_fcms = mysql.connector.connect(
        host = config['FCMS']['UK']['host'],
        user = config['FCMS']['UK']['user'],
        passwd = config['FCMS']['UK']['pwd']
    )
    with open(sql_file, 'r') as f:
        sql_string = f.read()
    df = pd.read_sql(con = conn_fcms, sql = sql_string)
    return df


def impala_to_df(sql_file):
    conn_string_dwh = f'''
    Driver={driver_imp};
    Host={host_imp};
    Port=21050;
    AuthMech=3;
    SSL=1;
    UID={user_imp};
    PWD={pwd_imp}'''
    conn_dwh = pyodbc.connect(conn_string_dwh, autocommit = True)
    with open(sql_file, 'r') as f:
        sql_string = f.read()
    df = pd.read_sql(con = conn_dwh, sql = sql_string)
    return df


def otifiq_uk():
    print('Extracting FCMS data...')
    df_fcms = fcms_to_df(sql_fcms_uk)
    print('Extracting DWH data...')
    df_dd = impala_to_df(sql_dd).set_index('date_string_backwards')
    print('Processing...')

    # get the hellofresh week for each delivery
    df_fcms['delivery_date'] = df_fcms['delivery_date_time_start'].dt.date.astype(str)
    df_fcms = (
        df_fcms
        .merge(df_dd, left_on = 'delivery_date', right_index = True)
    )
    qty_cols = ['received_qty', 'palletised_usable_qty', 'rejected_qty', 'original_expected_qty']
    for col in qty_cols:
        df_fcms[col] = df_fcms[col].fillna(0).astype('int')

    df_fcms['usable_qty'] = df_fcms['received_qty'] - df_fcms['rejected_qty']  # the palletised is not consistent enough

    # calculate time deviation in hours for deliveries out of range
    df_fcms['time_dev_hours'] = np.where(
        df_fcms['actual_delivery_date_time'].between(df_fcms['delivery_date_time_start'],
                                                     df_fcms['delivery_date_time_end']), 0,
        np.where(
            df_fcms['actual_delivery_date_time'] < df_fcms['delivery_date_time_start'],
            (df_fcms['actual_delivery_date_time'] - df_fcms['delivery_date_time_start']).astype('timedelta64[m]') / 60,
            (df_fcms['actual_delivery_date_time'] - df_fcms['delivery_date_time_end']).astype('timedelta64[m]') / 60
        )
    )
    df_fcms['received_perc'] = df_fcms['received_qty']/df_fcms['original_expected_qty']

    df_fcms['iq_reference'] = df_fcms[['original_expected_qty', 'received_qty']].min(axis = 1)
    df_fcms['usable_perc'] = (df_fcms['usable_qty'] / df_fcms['iq_reference']).clip(upper = 1)
    df_fcms['rejected_perc'] = df_fcms['rejected_qty']/df_fcms['iq_reference']
    return df_fcms


if __name__ == '__main__':
    otifiq = otifiq_uk()
    rejections = otifiq[otifiq['rejected_qty'] > 0]
    print()
