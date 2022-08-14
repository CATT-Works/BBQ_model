from cgitb import reset
from email import message
import os
import joblib
from getrawdata import *
from processdata import *


CONFIG_FILE = 'config.yaml'


class ModelZoo:
    def __init__(self):
        
        self.read_configs()
            
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
                
                
                
    def get_data_now(self, direction):

        lane_data = get_bb_current_status_df(get_bb_data(self.CREDENTIALS))
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
        
        return ml_data
    
    def estimate_now(self, direction, forecast_horizon=30, return_df = True):
        """
        Provides estimates for current situation
        Args:
            direction (string): Traffic direction (East/West)
            forecast_horizon (int, optional): Forecast horizon in minutes. 
                                              Defaults to 30.
            return_df (bool, optional): Return format (dataframe / array)

        Returns:
            pd.DataFrame / numpy.ndarray: return format. If df is returned,
            the columsn are ['tmc_code', 'measurement_tstamp', 'sr', 
            'average_speed', 'reference_speed']. Otherwise, pure numpy.ndarray
            is retured. Note: measurement_tstap is the most recent data timestamp
            in UTC. Predictions are for measurement_tstamp+forecast_horizon
        """
        
        ml_data = self.get_data_now(direction)
        model = self.model_dict[f'{direction}{forecast_horizon}']
        pred = model.predict(ml_data)
        
        if return_df:
            res = ml_data[['tmc_code', 'measurement_tstamp', 'sr', 'average_speed', 'reference_speed']].copy()       
            res['sr_pred'] = pred
            return res
        else:
            return pred
        
    
            
