import mysql.connector
import pyodbc
import pandas as pd
import yaml
import os
from pathlib import Path
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import datetime as dt
import boto3
# import whatever

# vars
date_start = '2019-06-28'
today_str = dt.date.today().strftime('%Y%m%d')

# file structure
cwd = os.getcwd()
auth_path = Path(cwd)/'auth'
sql_path = Path(cwd)/'sqls'
fcms_sql = sql_path/'fcms.sql'
dd_sql = sql_path/'dd.sql'
scores_path = Path(cwd)/'score tables'
arch_path = Path(cwd)/'csvs archive'

# read DB credentials, host, port etc from config.yml
with open('config.yml', 'r') as config_f:
    config = yaml.safe_load(config_f)

user_imp = os.getenv('jump_cloud_id')
pwd_imp = os.getenv('jump_cloud_pwd')
host_imp = config['DWH'].get('host')
driver_imp = config['DWH'].get('driver')
dest_wb = config['GSheet'].get('key')
dest_sheet = config['GSheet'].get('sheet')

conn_fcms = mysql.connector.connect(
    host=config['FCMS_DE'].get('host'),
    user=config['FCMS_DE'].get('user'),
    passwd=config['FCMS_DE'].get('pwd')
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

# load sql results in dataframes
df_fcms = pd.read_sql(con = conn_fcms, sql = sql_string_fcms, params={"d_start": date_start})
df_dd = pd.read_sql(con = conn_dwh, sql = sql_string_dd, params=(date_start,))

# get delivery week by joining FCMS with dd
df_fcms['date_adj'] = df_fcms['delivery_start_time'].dt.date + dt.timedelta(days = 1)
df_fcms['date_adj'] = df_fcms['date_adj'].astype(str)
df_fcms = (pd
           .merge(df_fcms, df_dd, left_on = 'date_adj', right_on = 'date_string_backwards')
           .drop(columns = ['date_adj', 'date_string_backwards'])
           )

# clean up data
num_cols = df_fcms.select_dtypes(include = np.number).columns
for col in num_cols:
    df_fcms[col].fillna(value = 0, inplace = True)

df_fcms['supplier'] = df_fcms['supplier'].str.replace('Ã¼', 'ü').str.replace('Ã¤', 'ä').str.replace("Â´", "'")

df_fcms['dc'] = df_fcms['po_number'].str[4:6]
df_fcms['country'] = 'DE'
df_fcms['category_abbr'] = df_fcms['sku_code'].str[:3]

# OT
# calculate delivery window: earlier than 5:30, between 5:30 and 11:00, other
df_fcms['it_window'] = np.where(
    df_fcms['delivery_end_time'].dt.strftime('%H:%M').between('00:00', '05:30'), 1,
    np.where(
        df_fcms['delivery_end_time'].dt.strftime('%H:%M').between('05:31', '11:00'), 2, 3
    )
)
# calculate time deviation in hours for deliveries out of range
df_fcms['time_dev'] = np.where(
    df_fcms['actual_delivery_date_time'].between(df_fcms['delivery_start_time'], df_fcms['delivery_end_time']), 0,
    np.where(
        df_fcms['actual_delivery_date_time'] < df_fcms['delivery_start_time'],
            (df_fcms['actual_delivery_date_time'] - df_fcms['delivery_start_time']).astype('timedelta64[m]')/60,
            (df_fcms['actual_delivery_date_time'] - df_fcms['delivery_end_time']).astype('timedelta64[m]')/60
    )
)
# create bins for delivery time deviations
time_dev_bins = pd.IntervalIndex.from_tuples([
    (-99999, -6),
    (-6, -4),
    (-4, -1),
    (-1, -0.5),
    (-0.5, -0.25),
    (-0.25, 0),
    (0, 0.25),
    (0.25, 0.5),
    (0.5, 1),
    (1, 1.5),
    (1.5, 2),
    (2, 99999)
], closed = 'left')
time_dev_labels = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

df_fcms['ot_bin'] = (pd
                     .cut(df_fcms['time_dev'], time_dev_bins)
                     .cat.rename_categories(time_dev_labels)
                     .astype('int')
                     )

# load the OT score table
df_ot_score = pd.read_csv(scores_path/'ot_scores.csv')

# join with main df to get OT scores
df_fcms = (pd
           .merge(df_fcms, df_ot_score,
                  how = 'left',
                  left_on = ['it_window', 'ot_bin'], right_on = ['delivery_window', 'deviation_bin'])
           .drop(columns = ['delivery_window', 'deviation_bin', 'ot_bin'])
           )

# IF
# calculate delivered percentage
df_fcms['delivered_perc'] = df_fcms['received_qty']/df_fcms['original_expected_qty']
# create bins for received percentages
rec_bins = pd.IntervalIndex.from_tuples([
    (0, 0.9),
    (0.9, 0.95),
    (0.95, 0.995),
    (0.995, 999)
], closed = 'left')
rec_labels = [1, 2, 3, 4]

df_fcms['delivered_bin'] = (pd
                            .cut(df_fcms['delivered_perc'], rec_bins)
                            .cat.rename_categories(rec_labels)
                            .astype('int')
                            )

# load the IF score table
df_if_score = pd.read_csv(scores_path/'if_scores.csv')

# join with main df to get IF scores
df_fcms = (pd
           .merge(df_fcms, df_if_score,
                  how = 'left',
                  left_on = 'delivered_bin', right_on = 'deviation_bin')
           .drop(columns = ['deviation_bin', 'delivered_bin'])
           )

# IQ
# calculate usable quantity
df_fcms['final_usable_qty'] = df_fcms['palletised_usable_qty'] - 0.3*df_fcms['out_of_spec_qty']
df_fcms['iq_reference'] = df_fcms[['original_expected_qty', 'received_qty']].min(axis = 1)
df_fcms['iq_score'] = df_fcms['final_usable_qty']/df_fcms['iq_reference']
df_fcms['iq_score'] = df_fcms['iq_score'].fillna(value = 0).clip(upper=1)

# arrange columns
sorted_cols = ['country', 'dc', 'hf_week', 'category_abbr', 'po_number', 'supplier', 'sku_code', 'original_expected_qty',
               'delivery_start_time', 'delivery_end_time', 'actual_delivery_date_time', 'received_qty', 'rejected_qty',
               'palletised_usable_qty', 'out_of_spec_qty', 'final_usable_qty', 'it_window', 'time_dev', 'delivered_perc',
               'iq_reference', 'ot_score', 'if_score', 'iq_score']
df_fcms = df_fcms.reindex(columns = sorted_cols)

# export to GSheet
# Credentials for GDrive
auth = auth_path/'gdrive.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(auth, scope)
client = gspread.authorize(creds)

# connect to the workbook
wb = client.open_by_key(dest_wb)
ws = wb.worksheet(dest_sheet)


# clear destination sheet
def clear_dest_sheet(source_df, destination_sheet):
    row_count = len(destination_sheet.col_values(1))
    col_count = source_df.shape[1]
    cell_list = destination_sheet.range(2, 1, row_count, col_count)
    for cell in cell_list:
        cell.value = ''
    destination_sheet.update_cells(cell_list)


clear_dest_sheet(df_fcms, ws)

# update GSheet
set_with_dataframe(ws, row = 2, col = 1, dataframe = df_fcms, include_column_header = False)


# upload to DWH using S3 upload
# credentials for S3
s3_client = boto3.client('s3',
                         aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                         )
bucket = 'hf-bi-dwh-uploader'

# export csv
print('Exporting csv...')
csv = f'OTIFIQ_FCMS_{today_str}.txt'
export_csv = arch_path/csv
df_fcms.to_csv(export_csv, sep = '\t', index = False, encoding = 'utf-8-sig', na_rep = '\\N')

# truncate existing DWH table
print('Deleting current records...')
with conn_dwh.cursor() as imp_cursor:
    imp_cursor.execute('TRUNCATE TABLE uploads.gp_otifiq_fcms')

# upload latest csv to S3
print('Uploading to S3...')
s3_client.upload_file(str(export_csv), bucket, 'gp_otifiq_fcms' + '/' + csv)

print('Done!')

