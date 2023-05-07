"""
preprocessor-weather
"""

import os
import sys
import pandas as pd
import numpy as np


def main():
    # check if path for weather data exists
    path = 'weather_data/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No data directory found.")
        print("Place weather data in \'weather_data\'.")
        input("Press enter to continue...")
        sys.exit()
    
    # check if the data file exists in the directory
    path = path + 'Weather.csv'
    if not os.path.exists(path):
        print("No data found.")
        print("Place weather data in \'weather_data\'.")
        input("Press enter to continue...")
        sys.exit()
    
    # read the data
    data = pd.read_csv(path)
    
    # drop uneeded columns
    data = data.drop(columns=['STATION', 'NAME', 'WT03', 'WT06', 'WT08'])
    
    # fill missing values with 0
    data = data.fillna(0)
    
    # cast data types
    data['DATE'] = data['DATE'].str.replace('-', '').astype('string')
    data['PRCP'] = data['PRCP'].astype('float')
    data['SNOW'] = data['SNOW'].astype('float')
    data['TMAX'] = data['TMAX'].astype('int')
    data['WT01'] = data['WT01'].astype('int')
    data['WT02'] = data['WT02'].astype('int')
    
    # merge the WT01 and WT02 columns
    for row in data.itertuples():
        if row.WT01 != 1:
            data.at[row.Index, 'WT01'] = row.WT02
            
    # now drop the uneeded WT02 columns
    data = data.drop(columns=['WT02'])
    
    # rename the columns
    data = data.rename(columns={'DATE':'date', 'PRCP':'precip', \
                                'SNOW':'snow', 'TMAX':'temp', 'WT01':'fog'})
    
    # create output directory if needed
    output_path = 'compiled/'
    if not os.path.exists(output_path):
        os.makedirs(path)
    
    # save output to pickle file
    output_path = output_path + 'df_weather.pkl'
    data.to_pickle(output_path)
    
    # print the dataframe
    print("Weather:")
    print(data, end='\n\n')
    print("Weather data complete.")
    input("Press enter to end...")
    


if __name__ == "__main__":
    main()
