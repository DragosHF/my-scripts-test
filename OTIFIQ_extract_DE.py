# test
import mysql.connector
import pyodbc
import pandas as pd
import yaml
import os
from pathlib import Path
import numpy as np
import datetime as dt
import boto3
from google_utils import df_to_gsheet_update, gsheet_read, convert_gsheet

# vars
date_start = '2019-06-28'
today_str = dt.date.today().strftime('%Y%m%d')

# file structure
cwd = Path(__file__).parent.resolve()
sql_path = cwd/'sqls'
fcms_sql = sql_path/'fcms_de.sql'
dd_sql = sql_path/'dd.sql'
out_path = cwd / 'output'

# read DB credentials, host, port etc from config.yml
with open('config.yml', 'r') as config_f:
    config = yaml.safe_load(config_f)

user_imp = os.getenv('jump_cloud_id')
pwd_imp = os.getenv('jump_cloud_pwd')
host_imp = config['DWH']['host']
driver_imp = config['DWH']['driver']
dest_wb = config['GSheet']['DE']['key']
dest_sheet = config['GSheet']['DE']['output_sheet']
s3_bucket = config['S3']['bucket']

conn_fcms = mysql.connector.connect(
    host=config['FCMS']['DE']['host'],
    user=config['FCMS']['DE']['user'],
    passwd=config['FCMS']['DE']['pwd']
)

conn_string_dwh = f'''
Driver={driver_imp};
Host={host_imp};
Port=21050;
AuthMech=3;
SSL=1;
UID={user_imp};
PWD={pwd_imp}'''
conn_dwh = pyodbc.connect(conn_string_dwh, autocommit = True)

# read sql files
# FCMS sql
with open(fcms_sql, 'r') as f:
    sql_string_fcms = f.read()
# Date dimension sql
with open(dd_sql, 'r') as f:
    sql_string_dd = f.read()


def otifiq(df):
    # get delivery week by joining FCMS with dd
    df['date_adj'] = df['delivery_date_time_start'].dt.date + dt.timedelta(days = 1)  # to include Friday in next week
    df['date_adj'] = df['date_adj'].astype(str)
    df = (pd.merge(df, df_dd, left_on = 'date_adj', right_on = 'date_string_backwards')
          .drop(columns = ['date_adj', 'date_string_backwards'])
          )

    # clean up data
    num_cols = df.select_dtypes(include = np.number).columns
    for col in num_cols:
        df[col].fillna(value = 0, inplace = True)
    df['supplier'] = df['supplier'].str.replace('Ã¼', 'ü').str.replace('Ã¤', 'ä').str.replace("Â´", "'")
    df['dc'] = df['po_number'].str[4:6]
    df['country'] = 'DE'
    df['category_abbr'] = df['sku_code'].str[:3]

    ###########
    # OT
    ###########

    # calculate time deviation in hours for deliveries out of range
    df['time_dev_hours'] = np.where(
        df['actual_delivery_date_time'].between(df['delivery_date_time_start'], df['delivery_date_time_end']), 0,
        np.where(
            df['actual_delivery_date_time'] < df['delivery_date_time_start'],
            (df['actual_delivery_date_time'] - df['delivery_date_time_start']).astype('timedelta64[m]') / 60,
            (df['actual_delivery_date_time'] - df['delivery_date_time_end']).astype('timedelta64[m]') / 60
        )
    )

    # apply the bins
    df['ot_bin'] = (pd
                    .cut(df['time_dev_hours'], time_dev_bins, right = False)
                    .cat.rename_categories(time_dev_labels)
                    .astype('int')
                    .clip(lower = 0)
                    )

    # unpivot the scores dataframe
    df_ot_melt = pd.melt(df_ot, id_vars = ['ot_bin', 'time_dev'],
                         value_vars = ['BAK', 'DAI', 'PHF', 'PTN', 'PRO', 'SPI', 'DRY'],
                         var_name = 'category_abbr', value_name = 'ot_score'
                         )

    # join main dataframe with the scores
    df = df.merge(df_ot_melt, how = 'left', on = ['ot_bin', 'category_abbr']).drop(columns = ['time_dev', 'ot_bin'])
    # all deliveries with 0 delivered should score 0 for the OT
    df.loc[df['total_received_units'] == 0, 'ot_score'] = 0

    #########
    # IF
    #########
    df['delivered_perc'] = df['total_received_units'] / df['total_ordered_units']
    # apply bins
    df['if_bin'] = (pd
                    .cut(df['delivered_perc'], delivered_perc_bins, right = False)
                    .cat.rename_categories(delivered_perc_labels)
                    .astype('int')
                    .clip(lower = 0)
                    )

    # join main dataframe with the scores table
    df = df.merge(df_if[['if_bin', 'if_score']], how = 'left', on = 'if_bin').drop(columns = ['if_bin'])

    ########
    # IQ
    ########

    df['final_usable_units'] = df['total_received_units'] - 0.3*df['out_of_spec_units'] - df['rejected_units']
    df['iq_reference'] = df[['total_ordered_units', 'total_received_units']].min(axis = 1)
    df['iq_score'] = df['final_usable_units']/df['iq_reference']
    df['iq_score'] = df['iq_score'].fillna(value = 0).clip(upper = 1).clip(lower = 0)

    ###############
    # final process
    ##############

    df['otifiq'] = df['ot_score'] * 0.4 + df['if_score'] * 0.3 + df['iq_score'] * 0.3
    sort_cols = [
        'country', 'dc', 'hf_week', 'supplier', 'po_number', 'category_abbr', 'sku_code', 'total_ordered_units',
        'delivery_date_time_start', 'delivery_date_time_end', 'total_received_units', 'actual_delivery_date_time',
        'rejected_units', 'out_of_spec_units', 'final_usable_units', 'time_dev_hours', 'ot_score', 'delivered_perc',
        'if_score', 'iq_reference', 'iq_score', 'otifiq'
    ]

    df = df[sort_cols]
    return df


def s3_upload(table, file, bucket):
    s3_client = boto3.client('s3',
                             aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
                             )
    s3_client.upload_file(str(file), bucket, f'{table}/{file.name}')


if __name__ == '__main__':
    # load sql results in dataframes
    print('Reading FCMS data...')
    df_fcms = pd.read_sql(con = conn_fcms, sql = sql_string_fcms)
    print('Reading DWH Date dimension...')
    df_dd = pd.read_sql(con = conn_dwh, sql = sql_string_dd)

    # load score tables
    print('Loading Score tables...')
    df_ot = gsheet_read(dest_wb, 'ot_scores', 1)
    df_ot = convert_gsheet(df_ot, ['ot_bin', 'time_dev', 'BAK', 'DAI', 'PHF', 'PTN', 'PRO', 'SPI', 'DRY'], [])
    df_if = gsheet_read(dest_wb, 'if_scores', 1)
    df_if = convert_gsheet(df_if, ['if_bin', 'delivered_perc', 'if_score'], [])

    # create the time deviation bins
    time_dev_bins = df_ot['time_dev'].tolist()
    time_dev_bins.append(np.inf)
    time_dev_labels = df_ot['ot_bin'].tolist()

    # create delivered deviations bins
    delivered_perc_bins = df_if['delivered_perc'].tolist()
    delivered_perc_bins.append(np.inf)
    delivered_perc_labels = df_if['if_bin'].tolist()

    print('Processing OTIFIQ...')
    df_fcms = otifiq(df_fcms)
    now = int(dt.datetime.timestamp(dt.datetime.utcnow()))
    df_fcms['updated_at'] = now
    print('Exporting...')
    export_txt = out_path/f'otifiq_fcms_de_{today_str}_{now}.txt'
    df_fcms.to_csv(export_txt, sep = '\t', index = False, encoding = 'utf-8-sig', na_rep = '\\N')
    print('Uploading to Google Sheet...')
    df_to_gsheet_update(dest_wb, dest_sheet, 2, df_fcms)

    # truncate existing DWH table
    print('Deleting current records...')
    with conn_dwh.cursor() as imp_cursor:
        imp_cursor.execute('TRUNCATE TABLE uploads.gp_otifiq_fcms')

    # upload latest csv to S3
    print('Uploading to S3...')
    s3_upload('gp_otifiq_fcms', export_txt, s3_bucket)

    print('Done!')

