import os
import joblib
from getrawdata import *
from processdata import *


CONFIG_FILE = 'config.yaml'


class ModelZoo:
    def __init__(self, credentials = None):
        
        self.read_configs()
            
        if credentials is not None:
            self.CREDENTIALS = credentials    
        
        self.tmcs_all_dict = read_all_tmcs(data_path=self.DATA_PATH)
        self.target_tmcs_df_dict = read_target_tmcs(data_path=self.DATA_PATH)
        
        
        
        self.load_model_dict()
        
        connect_bb(self.CREDENTIALS)
        
        
    def read_configs(self):
        with open(CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f)
        
        self.CREDENTIALS = config['CREDENTIALS']
        self.MODEL_PATH = config['SETTINGS']['MODEL_PATH']
        self.DATA_PATH = config['SETTINGS']['DATA_PATH']
        self.LINETERMINATOR = config['SETTINGS']['LINETERMINATOR']
                
        if ("USERNAME" in os.environ) and (os.environ["USERNAME"] != ""):        
            self.CREDENTIALS["USERNAME"] = os.environ["USERNAME"]
        if ("PASSWORD" in os.environ) and (os.environ["PASSWORD"] != ""):        
            self.CREDENTIALS["PASSWORD"] = os.environ["PASSWORD"]
            




        
    def load_model_dict(self):
        self.model_dict = {}
        for direction in ['East', 'West']:
            for forecast_horizon in range(5, 31, 5):
                model_path = os.path.join(self.MODEL_PATH, f'model_{direction}_{forecast_horizon}_min.pkl')
                self.model_dict[f'{direction}{forecast_horizon}'] = joblib.load(model_path)
                
                
                
    def get_data_now(self, direction, read_config = False):
        """
        Prepares data for the model

        Args:
            direction (str): West / East
            read_config (bool, optional): If True, the curent
                configuration of BB is read and used. Otherwise uses all
                configurations.
        Returns:
            pd.DataFrame: The ML data for the model
        """      
  
        if read_config:
            lane_data = get_bb_current_status_df(get_bb_data(self.CREDENTIALS))
        else:
            lane_data = get_bb_base_status_df()
            
        speeds = agg_speed_5m(get_speed_data(self.tmcs_all_dict[direction], self.CREDENTIALS))

        print ('Process data...')

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
    
    def estimate_now(self, direction, forecast_horizon='all', read_config=False):
        """
        Provides estimates for current situation
        Args:
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
        
        ml_data = self.get_data_now(direction, read_config)
        return self.estimate(ml_data, direction, forecast_horizon)
        
    
    def estimate(self, ml_data, direction, forecast_horizon='all'):
        """
        Provides estimates for current situation
        Args:
            ml_data (pd.DataFrame): DataFrame with ML data.
            direction (string): Traffic direction (East/West)
            forecast_horizon: Forecast horizon[s] in minutes. Could be an int,
                              a list of integers or a string 'all'
        Returns:
            pd.DataFrame: A dataframe with results. The columsn are 
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
                res_cols = ['tmc_code', 'measurement_tstamp', 'West', 'East', 'sr', 'average_speed', 'reference_speed']
                res_df = ml_data[res_cols].copy()       
                
            res_df[f'sr_pred_{horizon}'] = pred

                
        return res_df
    