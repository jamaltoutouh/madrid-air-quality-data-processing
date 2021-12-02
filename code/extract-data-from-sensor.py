import pandas as pd
import os
from pathlib import Path


station = 35    # id of the station that senses the air pollutants
metric = 8      # id of the evaluated metric 8=NO2

validity_columns = ['V{:02d}'.format(col) for col in range(1,25)]
value_columns = ['H{:02d}'.format(col) for col in range(1,25)]

output_path = '../processed-data/'
output_csv_file = 'processed-time-series.csv'
data_path = '../raw-data/'

root_path = '.'
os.listdir(root_path)

def get_dataframe_station_metric(csv_file, station, metric):
    data_df = pd.read_csv(csv_file, index_col=None, sep=';')
    data_df = data_df[(data_df['ESTACION']==station) & (data_df['MAGNITUD']==metric)]
    return data_df

def process_data_measures_row(values, validity):
    values = values.tolist()
    validity = validity.tolist()
    hours = list()
    processed_values = list()
    for index in range(0, 24):
        if validity[index]=='V':
            hours.append(index)
            processed_values.append(values[index])
    return hours, processed_values

def process_month_dataframe(data_df):
    day_data_df_list = list()
    for index, row in data_df.iterrows():
        values = row[value_columns]
        validity = row[validity_columns]
        data_dict = dict()
        hours, processed_values = process_data_measures_row(values, validity)
        data_dict['VALUE'] = processed_values
        data_dict['HOUR'] = hours
        len_data = len(processed_values)
        data_dict['YEAR'] = [row['ANO']] * len_data
        data_dict['MONTH'] = [row['MES']] * len_data
        data_dict['DAY'] = [row['DIA']] * len_data
        day_data_df_list.append(pd.DataFrame(data_dict))

    return pd.concat(day_data_df_list, ignore_index=True)

def create_whole_year_data_dataframe(csv_year_path):
    csv_files = Path(csv_year_path).glob('*.csv')
    # print('CSV files found: {}'.format(len(list(csv_files))))
    processed_month_data_df_list = list()
    for csv_file in csv_files:
        month_data_df = get_dataframe_station_metric(csv_file, station, metric)
        processed_month_data_df_list.append(process_month_dataframe(month_data_df))
    return pd.concat(processed_month_data_df_list, ignore_index=True)


def create_whole_data_dataframe(csv_whole_data_path):
    csv_files = Path(csv_whole_data_path).glob('Anio*/*.csv')
    number_of_files_found = len(list(Path(csv_whole_data_path).glob('Anio*/*.csv')))
    print('CSV files found: {}'.format(number_of_files_found))
    processed_month_data_df_list = list()
    for i, csv_file in enumerate(csv_files):
        month_data_df = get_dataframe_station_metric(csv_file, station, metric)
        processed_month_data_df_list.append(process_month_dataframe(month_data_df))
        print('Processed {} of {}.'.format(i+1, number_of_files_found))
    return pd.concat(processed_month_data_df_list, ignore_index=True)

data_df = create_whole_data_dataframe(data_path)
rows, cols = data_df.shape
print('Created a time series of {} pollution measures stored in {}'.format(rows, output_path + output_csv_file))
data_df = data_df.sort_values(['YEAR', 'MONTH', 'DAY', 'HOUR'], ascending = (True, True, True, True))
data_df[['YEAR', 'MONTH', 'DAY', 'HOUR','VALUE']].to_csv(output_path + output_csv_file, index=None)
