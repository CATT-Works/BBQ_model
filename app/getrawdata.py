# Contains functions that grab data raw data from baybridge API or files
# and process them to Pandas DataFrames
# Functions:
# - connect_bb: Connects to bb API if not conneted
# - get_bb_data: Gets bb configuration data from baybridge.ritis.org website
# - get_speed_data: Gets recent speed data from baybridge.ritis.org website
# - get_bb_current_status_df: Transforms get_bb_data output into a dataframe
# - agg_speed_5m: Aggregates dataframe with speeds to 5 minute granulation
# - read_all_tmcs: Reads all tmcs of interests
# - read_target_tmcs: Loads the target tmcs dataframes

import os
import pandas as pd
import numpy as np

import datetime
from datetime import timedelta
import time

import twill
import urllib.parse

import json
import yaml

def connect_bb(credentials : dict):
    """
    Connects to bb API if not conneted
    Args:
        credentials (dict): dictionary with USERNAME and PASSWORD
    """
    twill.commands.go('https://baybridge.ritis.org/')
    forms = twill.commands.showforms()
    
    # If already logged in, skip this part
    if len(forms) > 0:      
        form_id = 'popover-login'
        twill.commands.fv(form_id, "popover-username", credentials['USERNAME'])
        twill.commands.fv(form_id, "popover-password", credentials['PASSWORD'])
        twill.commands.submit(0)

        devnull = open(os.devnull, 'w')
        twill.set_output(devnull)


def get_bb_data(credentials = None):
    """
    Gets bb configuration data from baybridge.ritis.org website. 

    Args:
        credentials (dict): dictionary with USERNAME and PASSWORD
                           If None, function assumes that we are already
                           logged in

    Returns:
        str: string with jsonfile of current bb status
    """

    if credentials is not None:
        connect_bb(credentials)
 
   # Downloading data
    link = "https://baybridge.ritis.org/status"
    twill.commands.go(link)
    html_bb = twill.commands.show()  
    ret_bb = json.loads(html_bb)

    return ret_bb


def get_speed_data(tmc_list, credentials = None):
    """
    Gets recent speed data from baybridge.ritis.org website. 

    Args:
        tmc_list (list): list of tmcs to get data for
        credentials (dict): dictionary with USERNAME and PASSWORD
                           If None, function assumes that we are already
                           logged in

    Returns:
        pd.DataFrame: DataFrame with speeds
    """

    if credentials is not None:
        connect_bb(credentials)
 
   # Downloading data

    tmc_str = urllib.parse.quote(','.join(tmc_list))
    link = f'https://baybridge.ritis.org/speed/recent/?tmcs={tmc_str}'
    twill.commands.go(link)
    html = twill.commands.show()
    lines =  html.split('\n')
    df = pd.DataFrame([x.split(',') for x in lines[1:]], columns = lines[0].split(','))

    # Let's do a sanity check here, to be sure that we have right columns in
    # the right order
    df = df [[
        'tmc_code','measurement_tstamp',
        'speed','average_speed','reference_speed','travel_time_minutes'
        ]]

    for c in ['speed','average_speed','reference_speed']:
        df[c] = df[c].astype(int)
    df.travel_time_minutes = df.travel_time_minutes.astype(float)
        
    return df 

def get_bb_current_status_df(json_file):
    """
    Transforms output from baybridge API into a dataframe

    Args:
        json_file (str): string with jsonfile of current bb status

    Returns:
        pd.DataFrame: DataFrame with current bb status
    """
    
    dfs = pd.DataFrame(json_file['status'])
    dfs[['isClosed', 'isContraflow', 'defaultDirection', 'direction']]=dfs['lanes'].apply(pd.Series)
    dfs['status'] = 1
    dfs.loc[dfs.isClosed, 'status'] = 0
    dfs.loc[dfs.isContraflow, 'status'] = -1
    df_time = dfs.sort_index()[['status']].T
    df_time.columns = [f'{x}status' for x in df_time.columns]

    df_time[['L1status', 'L2status', 'L3status']]=df_time[['L1status', 'L2status', 'L3status']].replace(1,'W')
    df_time[['L1status', 'L2status', 'L3status']]=df_time[['L1status', 'L2status', 'L3status']].replace(-1,'E')
    df_time[['L4status','L5status']]=df_time[['L4status','L5status']].replace(1,'E')
    df_time[['L4status','L5status']]=df_time[['L4status','L5status']].replace(-1,'W')


    df_time['West']=df_time[df_time=='W'].count(axis=1)
    df_time['East']=df_time[df_time=='E'].count(axis=1)
    df_time['measurement_tstamp'] = datetime.datetime.now(datetime.timezone.utc)
    cols = ['measurement_tstamp','L1status','L2status','L3status','L4status','L5status','West','East']
    return df_time[cols]

def agg_speed_5m(df):
    """
    Aggregates dataframe with speeds to 5 minute granulation
    Args:
        df (pd.DataFrame): DataFrame with speed data
    Returns:
        pd.DataFrame: aggregated dataframe with speed data
    """

    df.measurement_tstamp = pd.to_datetime(df.measurement_tstamp, utc=True).dt.floor("5min")
    df = df.groupby(['tmc_code', 'measurement_tstamp']).agg({
        'speed' : 'mean',
        'average_speed' : 'mean',
        'reference_speed' : 'mean',
        'travel_time_minutes' : 'mean'
    }).reset_index()
    return df



def read_all_tmcs(data_path : str):
    """
    Reads all tmcs of interests
    Args:
        data_path (str): path to the data folder    
    Returns:
        Returns:
            {
                'East': list,
                'West': list
            }
            Dictionary that contains two tmc lists
    """

    ret = {}
    for traffic_dir in ['East', 'West']:

        filename = os.path.join(data_path, f'{traffic_dir}bound_all_TMCs.csv')

        with open(filename) as f:
            lines = f.readlines()
        ret[traffic_dir] = [x[:-1] for x in lines[1:]]

    return ret


def read_target_tmcs(data_path : str):
    """
    Loads the target tmcs dataframes
    Args:
        data_path (str): path to the data folder    
    Returns:
        {
            'East': pd.DataFrame,
            'West': pd.DataFrame
        }
        Dictionary that contains target tmc dataframes in both directions
    """
    res = dict()
    for traffic_dir in ['East', 'West']:
        tmc_file = os.path.join(
            data_path, f'{traffic_dir}bound_target_TMCs.csv')    
        
        res[traffic_dir] = pd.read_csv(tmc_file, delimiter=',' ,dtype=None)
    return res

