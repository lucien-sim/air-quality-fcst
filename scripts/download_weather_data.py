#!/Users/Lucien/anaconda/envs/tdi/bin/python

import requests
import json
import os
import warnings
import pandas as pd
import pickle
from MesoPy import Meso
import datetime
from sql_functions import create_sql_connection, execute_sql_command
import sys

sys.path.append('../')
from api_keys import api_key_synopticlabs
from global_vbls import data_path, db_file


def retrieve_wxobs_synopticlabs(api_key, data_path, station_id='knyc',
                                st_time='201801010000', ed_time='201801020000', 
                                vbl_list=['date_time','air_temp', 'relative_humidity'],
                                download_new=False):
    """
    Function to retrieve timeseries weather observations from an observation site. Uses the
    MesoWest/SynopticLabs API to retrieve the observations.

    PARAMETERS:
    *********** 

    api_key: SynopticLabs api_key.

    data_path: Path to directory in which we want to save the observations.

    station_id: Four-letter station id.

    st_time: Start time for observations, in format 'YYYYMMDDhhmm'. 

    ed_time: End time for observations, in format 'YYYYMMDDhhmm'. 

    vbl_list: List of variables to retrieve from SynopticLabs. 

    download_new: Boolean, re-download data from SynopticLabs if True. 

    OUTPUTS:
    ********

    data_ts: Data structure returned from API request. 

    path_name: File path/name for data structure that was just retrieved.
    """

    def get_synopticlabs_token(api_key):
        request_generate_token = 'http://api.mesowest.net/v2/auth?apikey=' + api_key
        api_out = requests.get(request_generate_token).text
        token_dict = json.loads(api_out)
        token_synopticlabs = token_dict['TOKEN']
        return token_synopticlabs

    file_name = 'wxobs_' + station_id + '_' + st_time + '_' + ed_time + '.pkl'

    if download_new:

        token_synopticlabs = get_synopticlabs_token(api_key)
        m = Meso(token=token_synopticlabs)
        data_ts = m.timeseries(stid=station_id, start=st_time, end=ed_time, vars=vbl_list)
        pickle.dump(data_ts, open(os.path.join(data_path, file_name), 'wb'))

    else:

        try:
            data_ts = pickle.load(open(os.path.join(data_path, file_name), 'rb'))
        except:
            raise (OSError('File not found: ' + file_name))

    return data_ts, os.path.join(data_path, file_name)


def observations_to_df(wx_obs, vbl_list):
    """Place observations returned from 'retr_wxobs_synopticlabs' 
    in a dataframe.
    """

    def get_station_attrs(data_ts, station_attrs):
        station_info = {}
        for attr in station_attrs:
            station_info[attr] = data_ts['STATION'][0][attr]
        return station_info

    def get_station_obs(data_ts, vbl_list):
        station_data = dict()
        station_data['date_time'
                     ] = data_ts['STATION'][0]['OBSERVATIONS']['date_time']
        for vbl in vbl_list:
            if vbl in data_ts['STATION'][0]['SENSOR_VARIABLES'].keys(): 
                station_data[vbl] = data_ts['STATION'][0]['OBSERVATIONS'][
                    list(data_ts['STATION'][0]['SENSOR_VARIABLES'][vbl].keys())[0]]
        return station_data


    station_attrs = ['STID', 'ELEVATION', 'NAME', 'LONGITUDE', 'LATITUDE']

    # Put everything in a single dictionary.
    obs_dict = dict()
    obs_dict['station_attrs'] = get_station_attrs(wx_obs, station_attrs)
    obs_dict['station_obs'] = get_station_obs(wx_obs, vbl_list)
    obs_dict['units'] = wx_obs['UNITS']
    obs_dict['qc_summary'] = wx_obs['QC_SUMMARY']

    # Convert to dataframe, resample to nearest hour. 
    obs_df = pd.DataFrame(obs_dict['station_obs'])
    obs_df['date_time'] = pd.to_datetime(obs_df['date_time'], utc=True).dt.tz_convert('EST')
    obs_df = obs_df.set_index('date_time')
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        obs_df = obs_df.resample('H').agg('nearest')  # Resample to nearest hour.

    return obs_df


if __name__ == '__main__': 

    # Retrieve data from SynopticLabs API
    vbl_list = ['air_temp','dew_point_temperature',
                'wind_speed', 'wind_direction', 'wind_gust', 
                'precip_accum_one_hour', 'sea_level_pressure', 
                'solar_radiation', 'cloud_layer_1_code',
                'cloud_layer_2_code','cloud_layer_3_code']
    st_time = '200301010000'
    ed_time = '201907171600'  
    # datetime.datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M')
    wx_obs, fname = retrieve_wxobs_synopticlabs(api_key_synopticlabs, data_path, station_id='knyc',
                                                st_time=st_time, ed_time=ed_time,
                                                vbl_list=vbl_list, download_new=False)

    # Place observations in dataframe
    obs_df = observations_to_df(wx_obs, vbl_list)

    # Add observations to a SQL database table. 
    conn = create_sql_connection(db_file)
    obs_df.to_sql('wx_obs', conn, index=True, if_exists='replace')
    conn.close()
