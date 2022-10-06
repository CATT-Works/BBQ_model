from doctest import OutputChecker
import os
import joblib
from getrawdata import *
from processdata import *
import yaml
from json import dumps
from datetime import datetime, timezone, timedelta


CONFIG_FILE = 'config.yaml'

class ModelZoo:
    def __init__(self,
        bb_endpoint = 'https://baybridge.ritis.org/status', 
        speed_endpoint  = 'https://baybridge.ritis.org/speed/recent/',
        token = None):
        
        self.read_configs()
        
        self.bb_endpoint = bb_endpoint
        self.speed_endpoint = speed_endpoint
        self.token = token
        
        if (token is None) and (os.path.isfile(self.TOKEN_FILE)):
            with open(self.TOKEN_FILE, 'r') as f:
                self.token = f.read()
        
        self.tmcs_all_dict = read_all_tmcs(data_path=self.DATA_PATH)
        self.target_tmcs_df_dict = read_target_tmcs(data_path=self.DATA_PATH)
        
        self.load_model_dict()

    def read_configs(self):
        with open(CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f)
        
        self.TOKEN_FILE = config['CREDENTIALS']['TOKEN_FILE']
        self.MODEL_PATH = config['SETTINGS']['MODEL_PATH']
        self.DATA_PATH = config['SETTINGS']['DATA_PATH']
        self.LINETERMINATOR = config['SETTINGS']['LINETERMINATOR']
        self.VERSION = config['GENERAL']['VERSION']
                
                        
    def load_model_dict(self):
        self.model_dict = {}
        for direction in ['East', 'West']:
            for forecast_horizon in range(5, 31, 5):
                model_path = os.path.join(self.MODEL_PATH, f'model_{direction}_{forecast_horizon}_min.pkl')
                self.model_dict[f'{direction}{forecast_horizon}'] = joblib.load(model_path)
                
                
                
    def get_data_now(self, direction, 
                     asof = None,
                     read_config = False):
        """
        Prepares data for the model

        Args:
            direction (string): Traffic direction (East/West)
            asof (datetime, optional): Timestamp of the current situation, 
                                       for speed endpoint.
            read_config (bool, optional): If True, the curent
                configuration of BB is read and used. Otherwise uses all
                configurations.
        Returns:
            pd.DataFrame: The ML data for the model
        """ 
  
  
        if read_config:
            lane_data = get_bb_current_status_df(get_bb_data(bb_endpoint = self.bb_endpoint,
                                                 token = self.token))
        else:
            lane_data = get_bb_base_status_df()
            
        lane_data.measurement_tstamp = lane_data.measurement_tstamp.dt.floor("5min")
            
        speeds = agg_speed_5m(get_speed_data(
            self.tmcs_all_dict[direction],
            speed_endpoint=self.speed_endpoint,
            asof = asof,
            token=self.token))

        queue_data = generate_queue_data(speeds, self.target_tmcs_df_dict[direction])
        ml_data = prepare_ml_data(
            tmc_list = self.tmcs_all_dict[direction], 
            tmc_target = self.target_tmcs_df_dict[direction],
            speeds = speeds,
            lane_data = lane_data, 
            queue_data = queue_data
        )
        
        ml_data = ml_data[ml_data.measurement_tstamp == ml_data.measurement_tstamp.max()]

        if read_config:
            return ml_data
        
        # Returns all the BB configurations data
        
        ret = pd.DataFrame()
        for east in range (1, 5):
            for west in range (1, 6-east):
                ml_data.East = east
                ml_data.West = west
                if len(ret) == 0:
                    ret = ml_data.copy()
                else:
                    ret = pd.concat([ret, ml_data],ignore_index=True)
        return ret
             
                
        return ml_data
    
    def estimate_now(self, direction, forecast_horizon, 
                     read_config=False, outputformat = 'json'):
        """
        Provides estimates for current situation
            direction (string): Traffic direction (East/West)
            forecast_horizon: Forecast horizon[s] in minutes. Could be an int,
                                    a list of integers or a string 'all'
            read_config (bool, optional): If True, the curent
                configuration of BB is read and used. Otherwise uses all
                configurations.
        Returns:
            pd.DataFrame: A dataframe with results. The columsn are 
            ['horizon', 'tmc_code', 'measurement_tstamp', 'sr', 
            'average_speed', 'reference_speed']. 
        """
        
        timestamp = datetime.now(timezone.utc)
                
        ml_data = self.get_data_now(
            direction=direction, 
            asof = timestamp,
            read_config=read_config)
        
        
        
        res_df = self.estimate(ml_data, direction, forecast_horizon)
        
        
        if outputformat == 'df':
            return res_df
        if outputformat == 'csv':
            return res_df.to_csv(index=False, line_terminator=self.LINETERMINATOR)
        elif outputformat == 'json':
            dic = {}
            dic['header'] = self.get_json_header_dic(direction, forecast_horizon, timestamp)
            dic['predictions'] = self.get_json_body_dic(res_df)
            return dumps(dic)
        else:
            return res_df            
                    
        
    
    def estimate(self, ml_data, direction, forecast_horizon='all'):
        """
        Provides estimates for current situation
        Args:
            ml_data (pd.DataFrame): DataFrame with ML data.
            direction (string): Traffic direction (East/West)
            forecast_horizon: Forecast horizon[s] in minutes. Could be an int,
                              a list of integers or a string 'all'
        Returns:
            pd.DataFrame/csv/json with restuls.
            If it is a dataframe or a csv,  The columsn are 
            ['horizon', 'tmc_code', 'measurement_tstamp', 'sr', 
            'average_speed', 'reference_speed']. 
        """
        
        if forecast_horizon == 'all':
            forecast_horizon = [5, 10, 15, 20, 25, 30]
        elif isinstance(forecast_horizon, (int, str)):
            forecast_horizon = [forecast_horizon]

        res_df = pd.DataFrame()        
        for horizon in forecast_horizon:                
            model = self.model_dict[f'{direction}{horizon}']
            pred = model.predict(ml_data)
            
            if len (res_df) == 0:
                res_cols = ['tmc_code', 'measurement_tstamp', 'West', 'East', 'reference_speed']
                res_df = ml_data[res_cols].copy()       
                
            res_df[f'sr_pred_{horizon}'] = pred

        return res_df
    
    def __get_date_str(self, dt):
        return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
    
    def get_json_header_dic(self, direction, forecast_horizon, 
                            timestamp = None,
                            asOf = None, 
                            measurement_tstamp = None):
        """Provives a dictionary for json output header
        Args:
            direction: traffic direction [East/West]
            forecast_horizon: forecast horizon [min] 
                              [5, 10, 15, 20, 25, 30, 'all']
            timestamp: request timestamp. if None, current timestamp is used
            asOf: timestamp of the speed data. If None, timestamp argument is 
                  used
            measurement_tstamp: measurement timestamp (5 minute interval). 
                                If None, computed automatically from timestamp

        Returns:
            _type_: _description_
        """
        
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
            
        if asOf is None:
            asOf = timestamp
        if measurement_tstamp is None:          
            measurement_tstamp = timestamp - timedelta(minutes=timestamp.minute % 5,
                                                       seconds=timestamp.second,
                                                       microseconds=timestamp.microsecond)
            
        
        header = {
            'timestamp' : self.__get_date_str(timestamp),
            'input_parameters' : {
                'direction' : direction,
                'horizon' : str(forecast_horizon),
                'speed_data' : {
                    'asOf' : self.__get_date_str(asOf),
                }
            },
            'measurement_tstamp' : self.__get_date_str(measurement_tstamp),
            'version' : self.VERSION
        }
        
        return header
    
    def get_json_predictions_dic(self, res_df, nr_east, nr_west):
        """
        Returns a dictionary with predictions for the given configuration

        Args:
            res_df: DataFrame with predictions, as returned by estimate
            nr_east: number of eastbound lanes
            nr_west: number of westbound lanes

        Returns:
            dictionary: Dictionary in format:
                horizon_X1: { // group sr predictions for each horizon
                    'TMC_code1': speed_ratio1,
                    'TMC_code2': speed_ratio2,
                    ...
                },
                horizon_X2: ...
            for all available horizons and TMCs            
        """
        
        df = res_df[(res_df.East == nr_east) & (res_df.West == nr_west)].set_index('tmc_code')
        if len(df) == 0:
            return None
    
        predictions = {}
        for horizon in range (5, 31, 5):
            if f'sr_pred_{horizon}' in df.columns:
                predictions[f'horizon_{horizon}'] = df[f'sr_pred_{horizon}'].to_dict()
        
        return predictions
    
    def get_json_body_dic(self, df):
        """
        Returns dictionary with a body of the JSON response
        Args:
            df: DataFrame with predictions, as returned by estimate
        Returns:
            dictionary: Dictionary in format:
                E1W1: {
                    results of get_json_predictions_dic for configuration E1W1
                },
                E1W2: {
                    results of get_json_predictions_dic for configuration E1W2
                },
                ...
            for all available configurations
        """
        body = {}
        for east in range (1, 5):
            for west in range (1, 6-east):
                predictions = self.get_json_predictions_dic(df, east, west)
                if predictions is not None:
                    body[f'E{east}W{west}'] = predictions
        return body
        
    