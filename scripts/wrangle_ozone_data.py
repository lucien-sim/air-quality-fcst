#!/Users/Lucien/anaconda/envs/tdi/bin/python

import os
import pandas as pd
import sqlite3
import datetime
import logging
import sys
import warnings

sys.path.append('../scripts')
from global_vbls import data_path, db_file
from sql_functions import create_sql_connection, execute_sql_command


def read_o3_file(year):
    o3_fname = os.path.join(data_path, 'hourly_44201_'+year+'.csv')
    o3_df = pd.read_csv(o3_fname)
    return o3_df


def add_site_id(o3_df):
    def create_site_id(row): return str(
        row['State Code'])+str(row['County Code'])+str(row['Site Num'])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        o3_df['site_id'] = o3_df.apply(create_site_id, axis=1)
    return o3_df


def get_site_info_nyc(o3_df_nyc):
    sites_nyc = o3_df_nyc[['site_id', 'State Name',
                           'County Name', 'Latitude', 'Longitude']]
    rename_mapper = {'State Name': 'state', 'County Name': 'county',
                     'Latitude': 'lat', 'Longitude': 'lon'}
    sites_nyc = sites_nyc[~sites_nyc.duplicated()].rename(mapper=rename_mapper, axis=1)\
                                                  .set_index('site_id')
    return sites_nyc


def add_datetime_local(o3_df):
    def make_datetime_local(row): return pd.Timestamp(
        row['Date Local']+' '+row['Time Local'])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        o3_df['timestamp_local'] = o3_df.apply(make_datetime_local, axis=1)
    return o3_df


def test__check_o3_units(o3_df_nyc):
    all_units = o3_df_nyc['Units of Measure'].unique()
    if len(all_units) > 1 or all_units[0] != 'Parts per million':
        raise ValueError('O3 units may be incorrect. Units: ' +
                         [unit for unit in all_units])
    return None


def select_o3_observations_nyc(o3_df):
    nyc_counties = ['Kings', 'Queens', 'New York', 'Bronx', 'Richmond']
    o3_df_nyc = o3_df[(o3_df['State Name'] == 'New York') &
                      (o3_df['County Name'].isin(nyc_counties))]
    o3_df_nyc = add_site_id(o3_df_nyc)
    o3_df_nyc = add_datetime_local(o3_df_nyc)
    sites_nyc = get_site_info_nyc(o3_df_nyc)
    test__check_o3_units(o3_df_nyc)
    o3_df_nyc_f = o3_df_nyc[['site_id', 'timestamp_local', 'Sample Measurement']]\
        .rename(mapper={'Sample Measurement': 'o3_ppm'}, axis=1)\
        .set_index('timestamp_local')
    return o3_df_nyc_f, sites_nyc


if __name__ == '__main__': 

    # Setup log file.
    log_filename = '../logfiles/wrangle_ozone_data.log'
    os.remove(log_filename)
    logging.basicConfig(filename=log_filename, filemode="w", level=logging.DEBUG,
                        format='%(name)s - %(levelname)s - %(message)s')
    logging.info('**********************************************************')
    logging.info('Adding ozone observations to SQLite database.')
    logging.info('Time: '+str(datetime.datetime.today()))

    # Prepare to collect all the ozone observations. 
    sites_nyc_all = pd.DataFrame({
        'state': [],
        'county': [],
        'lat': [],
        'lon': []
    })
    conn = create_sql_connection(db_file)
    execute_sql_command(conn, "DROP TABLE IF EXISTS o3_obs_nyc")
    years = [str(yr) for yr in range(2003, 2020)]

    # Collect all the ozone observations in a single database file. 
    for year in years:

        logging.info('Processing ozone observations from year: '+year)
        o3_df = read_o3_file(year)
        o3_df_nyc_f, sites_nyc = select_o3_observations_nyc(o3_df)

        sites_nyc_all = sites_nyc_all.append(sites_nyc)
        sites_nyc_all = sites_nyc_all[~sites_nyc_all.duplicated()]

        logging.info(
            'Adding ozone observations to SQLite database, table: o3_obs_nyc')
        o3_df_nyc_f.to_sql('o3_obs_nyc', conn, index=True, if_exists='append')

    logging.info(
        'Adding ozone observation site information to SQLite database, table: o3_sites_nyc')
    sites_nyc_all.to_sql('o3_sites_nyc', conn, index=True, if_exists='replace')
    
