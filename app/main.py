import sys
import os
import argparse
from modelzoo import ModelZoo

parser = argparse.ArgumentParser(description='Generate BayBridge Estimates')

parser.add_argument('-t', '--token',
                    type=str,
                    default=None,
                    help='Baybridge endpoints token'
                   )

parser.add_argument('-s', '--speedendpoint',
                    type=str,
                    default='https://baybridge.ritis.org/speed/recent/',
                    help='Speed endpoint'
                   )

parser.add_argument('-b', '--bbendpoint',
                    type=str,
                    default='https://baybridge.ritis.org/status/',
                    help='Bay Bridge status endpoint'
                   )

parser.add_argument('-d', '--direction',
                    type=str,
                    default='East',
                    choices=['East', 'West'],
                    help='Traffic direction'
                   )

parser.add_argument('-f', '--forecasthorizon',
                    type=str,
                    default='all',
                    choices=['all', '5', '10', '15', '20', '25', '30'],
                    help='Horizon of estimates'
                   )

parser.add_argument('-o', '--outputformat',
                    type=str,
                    default='json',
                    choices=['json', 'csv', 'df'],
                    help='Output format'
                   )



def estimate_now(args):        
    res = modelzoo.estimate_now(
        direction=args.direction, 
        forecast_horizon=args.forecasthorizon, 
        read_config=False,
        outputformat=args.outputformat)
    
    return res



if __name__ == '__main__':
    args = parser.parse_args()
    
    old_stdout = sys.stdout
    with open(os.devnull, 'w') as f:
        sys.stdout = f
        modelzoo = ModelZoo(
            bb_endpoint=args.bbendpoint,
            speed_endpoint=args.speedendpoint,
            token=args.token
        )
        estimates = estimate_now(args)
    
    sys.stdout = old_stdout
    print (estimates)


    