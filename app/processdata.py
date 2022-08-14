# Script used for processing data
import pandas as pd
from datetime import timedelta


def generate_queue_data(df, link):
    """
    Generates queue data.
    Args:
        df (pd.DataFrame): speeds (read from bb API)
        link (pd.DataFrame): target tmcs

    Returns:
        pd.DataFrame: queue lengths info 
    """

    # STEP 1 - Calculate speed ratio.
    df['speed_ratio']=df['speed']/df['reference_speed']

    # Merging and sorting input data
    df['measurement_tstamp']=pd.to_datetime(df['measurement_tstamp'])
    df=link.merge(df[['tmc_code','measurement_tstamp','speed_ratio']],on=['tmc_code'])
    df=df.sort_values(by=['dist'])    

    # STEP 2 - Determine the queue.
    df['queue']=0
    df.loc[df['speed_ratio']<0.6,'queue']=df.loc[df['speed_ratio']<0.6,'length']
    

    # STEP 3 - Calculate the queue length.
    dq=df.groupby('measurement_tstamp')[['queue']].sum().reset_index()
    

    # STEP 4 - Reshape the table.
    for lag in ([5,10,15,20,25,30]):
        dq1=dq.copy()
        dq1['measurement_tstamp'] += timedelta(hours=0, minutes=lag)
        dq=dq.merge(dq1[['measurement_tstamp','queue']],on=['measurement_tstamp'],suffixes=['','_'+str(lag)], how = 'left')    




    # STEP 5 - Calculate the start and the end of the queue.
    dq2=pd.merge(df.loc[df['speed_ratio']<0.6].drop_duplicates(subset=['measurement_tstamp'],keep='first')[['measurement_tstamp','dist','length']],
             df.loc[df['speed_ratio']<0.6].drop_duplicates(subset=['measurement_tstamp'],keep='last')[['measurement_tstamp','dist','length']],
             on='measurement_tstamp',suffixes=['_start','_end'])
    dq2['dist_start']=dq2['dist_start']-dq2['length_start']
    dq=dq2[['measurement_tstamp','dist_start','dist_end']].merge(dq,on='measurement_tstamp',how='right')
    
    # STEP 6 - determine the number of queues
    dq['num_queue']='zero'
    dq.loc[(dq['queue']>0)&(dq['dist_end']-dq['dist_start']-dq['queue']<0.001),'num_queue']='one'
    dq.loc[(dq['queue']>0)&(dq['dist_end']-dq['dist_start']-dq['queue']>0.001),'num_queue']='more_than_one'
    
    # STEP 7 (optional) - drop rows with nans
    queue_cols = [f'queue_{x}' for x in range (5, 31, 5)]
    dq.dropna(axis = 0, how = 'any', subset=queue_cols, inplace = True)


    return dq

def prepare_ml_data(tmc_list, tmc_target, speeds, lane_data, queue_data):
    """
    Prepares data for machine learning
    Arguments:
        tmc_list   - list of all tmcs in the defined direction
        tmc_target - a dataframe with target tmcs, distances and lengths
        speeds     - dataframe with speeds and travel times for each 
                     tmc/timestamp
        lane_data  - lane data, output from generate_bb_data() function
        queue_data - queue data, output from generate_queue_data() function
    Returns:
        A dataframe prepared for Machnie Learning.
    """


    # 2 - merge past and future speed ratios
    df = speeds.copy()
    df['sr']=df['speed']/df['reference_speed']
    
    dg=df[['tmc_code','measurement_tstamp','sr','average_speed','reference_speed']]

    tmp = dg.copy()
    for lag in ([5,10,15,20,25,30]):
        dg1=tmp.copy()
        dg1['measurement_tstamp'] += timedelta(hours=0, minutes=lag)
        dg=dg.merge(dg1[['measurement_tstamp','tmc_code','sr']],on=['measurement_tstamp','tmc_code'],suffixes=['','_'+str(lag)], how='left')

    dg = dg.sort_values(['measurement_tstamp', 'tmc_code'])
    
    # 3 - Add all speed ratios
    tmcs = sorted(list(dg.tmc_code.unique()))
    for col in ['sr','sr_5', 'sr_10', 'sr_15','sr_20','sr_25','sr_30']:
        v=dg[['measurement_tstamp',col]].groupby(['measurement_tstamp'])[col].apply(lambda x: x.tolist())
        dg=dg.merge(pd.DataFrame(v.tolist(), index=v.index)\
           .rename(columns=lambda x: f'_{tmcs[x]}')\
           .add_prefix(col)\
           .reset_index())    
        
    # Add speed ratio flags
    dg['sr_flag']=0
    dg.loc[dg['sr']<0.6,'sr_flag']=1    
    
    # 5 - Filter TMCS
    dg=dg.loc[(dg['tmc_code'].isin(tmc_target.tmc_code))].reset_index(drop=True)
    
    # 6 - Add temporal variables
    dg['season']=dg['measurement_tstamp'].dt.quarter
    dg['month']=dg['measurement_tstamp'].dt.month
    dg['dow']=dg['measurement_tstamp'].dt.weekday
    dg['hour']=dg['measurement_tstamp'].dt.hour    
    

    # 7 Add lane status
    lane_data['measurement_tstamp']=pd.to_datetime(lane_data['measurement_tstamp'])
    dg=dg.merge(lane_data[['measurement_tstamp', 'West', 'East']],
                on=['measurement_tstamp'],how='left')    
    

    # 8 - Add length and distance
    dg=dg.merge(tmc_target , on=['tmc_code'],how='left')    
    
    # 9 - Add queue info

    dg=dg.merge(queue_data[[ 'measurement_tstamp','queue','queue_5', 'queue_10', 'queue_15', 'queue_20', 'queue_25', 'queue_30']],how='left')
    
    
    return dg


