import io
import time
import os
import warnings
import sys
import zipfile

import requests
import pyodbc
import numpy as np
import pandas as pd

from geopy.distance import geodesic
from datetime import timedelta

pd.options.mode.chained_assignment = None
warnings.simplefilter(action='ignore', category=FutureWarning)

START_DATE = sys.argv[1]
END_DATE = sys.argv[2]

START_YEAR = START_DATE[:4]
END_YEAR = END_DATE[:4]
START_MONTH = START_DATE[5:7]
END_MONTH = END_DATE[5:7]


def fix_columns(df):
    for col in df.columns:
        df.rename(columns={col: col.lower().strip()}, inplace=True)
    return df


def distance(data):
    res = np.nan
    if not (np.isnan(data['latitude']) or np.isnan(data['latitude'])):
        try:
            res = geodesic((data['latitude'], data['longitude']), (data['lat'], data['lon'])).kilometers
            res = round(res, 2)
        except Exception as ex:
            print('Distance error:', ex)
    return res


def check_file(filename):
    flag = False
    if os.path.exists(filename):
        flag = True
    return flag


def get_station_info(st_name, date):
    st_year = date[:4]
    st_columns = ['STATION', 'DATE', 'SOURCE', 'REPORT_TYPE', 'CALL_SIGN',
                  'QUALITY_CONTROL', 'CIG', 'DEW', 'SLP', 'TMP', 'VIS', 'WND']

    file_url = f'https://www.ncei.noaa.gov/data/global-hourly/access/{st_year}/{st_name}.csv'
    url = (f'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&stations={st_name}'
           f'&dataTypes=WND,CIG,VIS,TMP,DEW,SLP'
           f'&startDate={date}&endDate={date}&includeAttributes=true&format=csv')

    code = 0
    rnd = 0
    response = None
    df = pd.DataFrame()
    while code != 200 or rnd == 3:
        rnd += 1
        response = requests.get(url)
        code = response.status_code
        print('API load status:', code)
        time.sleep(1)

    if rnd == 3:
        try:
            df = pd.read_csv(file_url)
            df = df[st_columns]
        except Exception as ex:
            print(ex)
    else:
        response_text = response.content.decode()
        df = pd.read_csv(io.StringIO(response_text))
        df.drop(columns=['SOURCE'], inplace=True)

    return df


def make_final(data):
    need_st = pd.DataFrame()
    tmp = pd.DataFrame(data=[list(data.values)], columns=data.index)
    tmp = tmp.merge(df_st_tmp, how='cross')

    tmp['st_distance'] = tmp.apply(distance, axis=1)

    df_sorted = tmp.groupby('airport_id').apply(lambda x: x.sort_values('st_distance')).reset_index(drop=True)
    df_sorted['row_number'] = df_sorted.groupby('airport_id').cumcount() + 1
    df_air_st_info = df_sorted.query('row_number <= 3')
    try:
        df_air_st_info.loc[:, 'time_round'] = pd.to_datetime(df_air_st_info.copy()['time'], format='%H:%M')
        df_air_st_info['time_round'] = df_air_st_info['time_round'].dt.round('H').dt.strftime('%H:%M')
    except Exception as ex:
        print('Error convert "time" in "df_air_st_info": ', ex)

    for i in range(3):
        st_code = df_air_st_info.iloc[i]['st_code']
        date = df_air_st_info.iloc[i]['incident_date'].strftime('%Y-%m-%d')
        try:
            need_st = get_station_info(st_code, date)
        except Exception as ex:
            print('Error, no station found: ', ex)
            print('st_code:', st_code, 'date:', date)
        if not need_st.empty:
            print(f'Station found, st_code: {st_code}, date: {date}')
            break
        else:
            print('Error, no station found:', st_code, 'date:', date)
            print('find next station')

    need_st = fix_columns(need_st)
    try:
        need_st['date'] = pd.to_datetime(need_st['date'])
        need_st['time_round'] = need_st['date'].dt.round('H').dt.strftime('%H:%M')
        need_st['date'] = need_st['date'].dt.date
        need_st['date'] = pd.to_datetime(need_st['date'])
    except Exception as ex:
        print('Error convert "time" in "need_st": ', ex)

    if df_air_st_info.iloc[0]['airport_id'] == 'ZZZZ':
        df_air_st_info['time_round'] = '24:00'

    left_cols = ['incident_date', 'time_round']
    right_cols = ['date', 'time_round']
    fin_df = pd.merge(df_air_st_info.head(1), need_st,
                      left_on=left_cols, right_on=right_cols, how='left')

    drop_col = ['airport_id', 'incident_date', 'time', 'latitude', 'longitude',
                'end', 'lat', 'lon', 'row_number', 'time_round', 'station', 'date']

    fin_df = fin_df.head(1).drop(columns=drop_col)
    result = fin_df.merge(df_inc, on='index_nr', how='inner')

    return result


# проверяем наличие архива с БД и извлекаем файл БД
file_bd = 'NWSD.zip'
if os.path.exists(file_bd):
    with zipfile.ZipFile(file_bd, 'r') as zfile:
        zfile.extract('Public.accdb')
    os.remove(file_bd)
else:
    print(f'Файл {file_bd} не найден')

# загружаем полную базу инцидентов из файла
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=D:\Develop\DE_Bird_Strike\Public.accdb;')

sql = (f"""
    SELECT *
    FROM STRIKE_REPORTS
    WHERE INCIDENT_YEAR BETWEEN {START_YEAR} AND {END_YEAR}
      AND INCIDENT_MONTH BETWEEN {START_MONTH} AND {END_MONTH}
""")

df_inc = pd.read_sql_query(sql=sql, con=conn)
df_inc = df_inc.query(
    f'INCIDENT_DATE >= "{START_DATE}" and INCIDENT_DATE <= "{END_DATE}"'
)

# загрузим все станции с координатами
st_url = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv'

df_st = pd.read_csv(
    st_url,
    delimiter=',',
    parse_dates=['BEGIN', 'END'],
    dtype={'USAF': str, 'WBAN': str}
)
df_st.query(f'LAT > 0 and END >= "{END_DATE}"', inplace=True)

# приведем названия колонок во всех DF к нормальному виду
df_inc = fix_columns(df_inc)
df_st = fix_columns(df_st)

# получим код станции
df_st['st_code'] = df_st['usaf'] + df_st['wban']

need_col = ['index_nr', 'airport_id', 'incident_date',
            'time', 'latitude', 'longitude']
df_inc_tmp = df_inc[need_col].copy()
df_inc_tmp['airport_id'] = df_inc_tmp['airport_id'].astype('category')
df_st_tmp = df_st[['st_code', 'end', 'lat', 'lon']].copy()

final_df = pd.DataFrame()
total_rows = df_inc_tmp.shape[0]
row_num = 0
errors = 0
total_time = 0
start_time = time.time()

for index, row in df_inc_tmp.iterrows():
    row_num += 1
    try:
        final_df = pd.concat([final_df, make_final(row)])
    except Exception as e:
        errors += 1
        print(f'Error load row {row_num}: ', e)

    total_time = time.time() - start_time
    avg_time = round((total_time / row_num) * total_rows - total_time)
    print(f'Row: {row_num}/{total_rows}, '
          f'errors: {errors} | wait time: {timedelta(seconds=avg_time)}')

final_df.to_csv(f'data_{START_DATE}_{END_DATE}.csv', index=False)
