"""
Sampler

Description: Generate a sample of the given data for mining.
"""
import os
import sys
import pandas as pd
import numpy as np
import math
import random
from tqdm import tqdm
import warnings

# ignore warnings
warnings.filterwarnings('ignore')


def main():
	# check if path for weather data exists
    path = 'compiled/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No compiled dataframe directory found.")
        print("Place pickled dataframes in \'compiled\'.")
        print("You may need to compile your dataframes first with preprocessor.py.")
        input("Press enter to continue...")
        sys.exit()
        
    # filename to look for
    file_master = 'df_master.pkl'
    
    # load the data if it exists
    try:
        df_master = pd.read_pickle(path + file_master)
        
    except:
        print("No data found.")
        print("Place pickled dataframes in \'compiled\'.")
        input("Press enter to continue...")
        sys.exit()
        
    
	
	
if __name__ == "__main__":
    main()