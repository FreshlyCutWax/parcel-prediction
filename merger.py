"""
merger

Description: Merge all the dataframes together.
"""

import os
import sys
import pandas as pd
import numpy as np











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
    
    # file names to look for
    file_aggregate = 'df_aggregate.pkl'
    file_merged_history = 'df_merged_history.pkl'
    file_package = 'df_package.pkl'
    
    # load the data if it exists
    try:
        df_aggregate = pd.read_pickle(path + file_aggregate)
        df_history = pd.read_pickle(path + file_merged_history)
        df_package = pd.read_pickle(path + file_package)
        
    except:
        print("No data found.")
        print("Place pickled dataframes in \'compiled\'.")
        input("Press enter to continue...")
        sys.exit()
    

    #-----------------------MERGING BEGINS HERE------------------------>
    print("The master dataframe will begin to be built.")
    print("This will take a while. Sit back and grab a drink.")
    print("\n\n")
    input("<Press enter to begin>")
    # initialize the dataframe
    df_master = df_package.copy()


if __name__ == "__main__":
    main()