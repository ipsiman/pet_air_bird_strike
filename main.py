import io
import time
import os
import warnings
import requests
import pyodbc
import numpy as np
import pandas as pd
import sys

from pandas import concat
from geopy.distance import geodesic

pd.options.mode.chained_assignment = None
warnings.simplefilter(action='ignore', category=FutureWarning)

START_DATE = sys.argv[1]
END_DATE = sys.argv[2]

START_YEAR = START_DATE[:4]
END_YEAR = END_DATE[:4]
START_MONTH = START_DATE[5:7]
END_MONTH = END_DATE[5:7]


def clear_columns(df):
    for col in df.columns:
        df.rename(columns={col: col.lower().strip()}, inplace=True)
    return df


def distance(row):
    res = np.nan
    try:
        res = geodesic((row['latitude'], row['longitude']), (row['lat'], row['lon'])).kilometers
    except Exception as e:
        print('Distance error:', e)
    return res


def check_file(filename):
    flag = False
    if os.path.exists(filename):
        flag = True
    return flag


def get_station_info(st_name, date):
    st_year = date[:4]
    file_name = f'./station_data/{st_year}_{st_name}.csv'
    csv_url = f'https://www.ncei.noaa.gov/data/global-hourly/access/{st_year}/{st_name}.csv'

    url = f'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&stations={st_name}'\
          f'&dataTypes=WND,CIG,VIS,TMP,DEW,SLP'\
          f'&startDate={st_year}-01-01&endDate={st_year}-12-31&includeAttributes=true&format=csv'

    if check_file(file_name):
        df = pd.read_csv(file_name)
    else:
        code = 0
        while code != 200:
            response = requests.get(url)
            code = response.status_code
            print('API load status:', code)
            time.sleep(2)
        response_text = response.content.decode()
        df = pd.read_csv(io.StringIO(response_text))
        df.drop(columns=['source'], inplace=True)
        if not df.empty:
            df.to_csv(file_name)

    return df


def make_final(row):
    tmp = pd.DataFrame(data=[list(row.values)], columns=row.index)
    tmp = tmp.merge(df_st_tmp, how='cross')

    tmp['st_distance'] = tmp.apply(distance, axis=1)

    df_sorted = tmp.groupby('airport_id').apply(lambda x: x.sort_values('distance')).reset_index(drop=True)
    df_sorted['row_number'] = df_sorted.groupby('airport_id').cumcount() + 1
    df_air_st_info = df_sorted.query('row_number <= 3')
    try:
        df_air_st_info.loc[:, 'time_round'] = pd.to_datetime(df_air_st_info.copy()['time'], format='%H:%M')
        df_air_st_info['time_round'] = df_air_st_info['time_round'].dt.round('H').dt.strftime('%H:%M')
    except Exception as e:
        print('Error convert "time" in "df_air_st_info": ', e)

    for i in range(3):
        st_code = df_air_st_info.iloc[i]['st_code']
        date = df_air_st_info.iloc[i]['incident_date'].strftime('%Y-%m-%d')
        try:
            need_st = get_station_info(st_code, date)
        except Exception as e:
            print('Error, no station found: ', e)
            print('st_code:', st_code, 'date:', date)
        if not need_st.empty:
            print(f'Station found, st_code: {st_code}, date: {date}')
            break
        else:
            print('Error, no station found:', st_code, 'date:', date)
            print('find next station')

    need_st = clear_columns(need_st)
    try:
        need_st['date'] = pd.to_datetime(need_st['date'])
        need_st['time_round'] = need_st['date'].dt.round('H').dt.strftime('%H:%M')
        need_st['date'] = need_st['date'].dt.date
        need_st['date'] = pd.to_datetime(need_st['date'])
    except Exception as e:
        print('Error convert "time" in "need_st": ', e)

    left_cols = ['incident_date', 'time_round']
    right_cols = ['date', 'time_round']
    fin_df = pd.merge(df_air_st_info.head(1), need_st, left_on=left_cols, right_on=right_cols, how='left')

    drop_col = ['airport_id', 'incident_date', 'time', 'latitude', 'longitude',
                'end', 'lat', 'lon', 'row_number', 'time_round', 'station', 'date']

    fin_df = fin_df.head(1).drop(columns=drop_col)
    result = fin_df.merge(df_inc, on='index_nr', how='inner')

    return result


# загружаем полную базу инцидентов из файла
conn = pyodbc.connect(
    r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ=D:\Develop\DE_Bird_Strike\Public.accdb;'
)

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
df_st = pd.read_csv(
    'isd-history.csv',
    delimiter=',',
    parse_dates=['BEGIN', 'END'],
    dtype={'USAF': str, 'WBAN': str}
)
df_st.query(f'LAT > 0 and END >= "{END_DATE}"', inplace=True)

# приведем названия колонок во всех DF к нормальному виду
df_inc = clear_columns(df_inc)
df_st = clear_columns(df_st)

# получим код станции
df_st['wban'] = df_st['wban'].astype('str')
df_st['usaf'] = df_st['usaf'].astype('str')
df_st['st_code'] = df_st['usaf'] + df_st['wban']

df_inc_tmp = df_inc[['index_nr', 'airport_id', 'incident_date', 'time', 'latitude', 'longitude']].copy()
df_inc_tmp['airport_id'] = df_inc_tmp['airport_id'].astype('category')
df_st_tmp = df_st[['st_code', 'end', 'lat', 'lon']].copy()


final_df = pd.DataFrame()
last_index = 0
errors = 0

for index, row in df_inc_tmp.iterrows():
    if index < 0:
        continue
    else:
        last_index = index
        print(f'Row num: {last_index}, errors: {errors}')
        try:
            final_df = concat([final_df, make_final(row)])
        except Exception as e:
            errors += 1
            print(f'Error load line {last_index}: ', e)
        if index % 50 == 0:
            final_df.to_csv(f'file_{START_DATE}_{END_DATE}_{index}.csv', index=False)

final_df.to_csv(f'file_{START_DATE}_{END_DATE}_full.csv', index=False)
