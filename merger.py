"""
merger

Description: Merge all the dataframes together.
"""

import os
import sys
import pandas as pd
import numpy as np
from tqdm import tqdm



def compress(df_master):
    # create an array of all our unique package IDs
    history_idx = pd.unique(df_master['package_id'])
    
    # create a list to hold compressed dataframe information
    history_list = []
    
    # days of week dictionary
    day_dict = {'M':0, 'T':1, 'W':2, 'R':3, 'F':4, 'S':5}
    
    for i in tqdm(history_idx):
        # initialize a list to hold compressed package history
        pkg_hist = []
        
        # get a dataframe of the an individual package history
        df_pkg = df_master[df_master['package_id'] == i]
        
        # append the package ID
        pkg_hist.append(df_pkg['package_id'].iloc[0])
        
        # get the class label
        pkg_hist.append(df_pkg['status'].iloc[len(df_pkg)-1])
        
        # get the days at the station
        dates = pd.unique(df_pkg['date'])
        pkg_hist.append(len(dates))
        
        # append how many entires the package has
        pkg_hist.append(max(df_pkg['order'].values)+1)
        
        # get the zipcode
        zips = pd.unique(df_pkg['zipcode'])
        zips = [x for x in zips if x != 0]
        try:
            zipcode = zips[len(zips)-1]
        except:
            zipcode = 0
        pkg_hist.append(zipcode)
        
        # get the provider
        provider = pd.unique(df_pkg['provider'])
        provider = [x for x in provider if x !='']
        try:
            p = provider[len(provider)-1]
        except:
            p = 'None'
        pkg_hist.append(p)
        
        # add the area
        area = pd.unique(df_pkg['assigned_area'])
        area = [x for x in area if x != 0]
        try:
            a = area[len(area)-1]
        except:
            a = 0
        pkg_hist.append(a)
        
        # add days of week frequency
        days = [0, 0, 0, 0, 0, 0]
        for d in dates:
            index = df_pkg[df_pkg['date'] == d].index
            dow = df_master['dow'].iloc[index[0]]
            days[day_dict[dow]] += 1
        for i in days:
            pkg_hist.append(i)
            
        # add code frequency
        s_codes = df_pkg['station_code'].value_counts()
        if 0 in s_codes:
            s_codes = s_codes.drop(0)
        codes = [0, 0, 0, 0, 0, 0, 0, 0, 0]
        for c in s_codes.index:
            codes[c-1] = s_codes[c]
        for i in codes:
            pkg_hist.append(i)
        
        # add address issue reason, if any
        reason = pd.unique(df_pkg['reason'])
        if reason.any():
            reason = reason[reason != 0]
        pkg_hist.append(reason[len(reason)-1])
        
        # append to compressed history list
        history_list.append(pkg_hist)

    df = pd.DataFrame(history_list, columns=['package_id', 'status', 'days_active', 'actions', 'zipcode', \
                                             'provider', 'area', 'dow_m', 'dow_t', 'dow_w', 'dow_r', 'dow_f', \
                                             'dow_s', 'code_1', 'code_2', 'code_3', 'code_4', 'code_5', \
                                             'code_6', 'code_7', 'code_8', 'code_9', 'address_reason'])
                                             
    return df



def add_package(df_master, df_package):
    # THIS FUNCTION ONLY WORKS AFTER COMPRESSION!!!
    # make a copy of the master dataframe
    df = df_master.copy()
    
    # empty arrays to be added as columns
    service_array = np.full((len(df)), '', dtype='str')
    sig_array = np.full((len(df)), '', dtype='str')
    
    # insert the columns in the new master dataframe
    df.insert(loc=len(df.columns), column='service', value=service_array)
    df.insert(loc=len(df.columns), column='signature', value=sig_array)
    
    for i in tqdm(df_master.itertuples()):
        df.at[i.Index, 'service'] = df_package[df_package['package_id'] == i.package_id].service.values[0]
        df.at[i.Index, 'signature'] = df_package[df_package['package_id'] == i.package_id].signature.values[0]
        
        
    return df
    
    
    
    
def add_aggregate(df_master, df_aggregate):
    # get a copy of the master dataframe
    df = df_master.copy()
    
    # create arrays for new columns
    total_count_array = np.full(len(df), 0, dtype='int')
    
    # insert blank column into dataframe
    df.insert(loc=len(df.columns), column='total_day_pkgs', value=total_count_array)
    
    # get the unique package IDs from the dataframe
    master_idx = pd.unique(df['package_id'])
    
    # loop over all the packages and total number of day packages
    for i in tqdm(master_idx):
        # get the package's history
        pkg = df[df['package_id'] == i]
        
        # get the unique dates
        dates = pd.unique(pkg['date'])
        
        for d in dates:
            # get the aggregate data for the date
            df_agg = df_aggregate[df_aggregate['date'] == d]
            
            # get the total number of packages
            total_pkgs = sum(df_agg['pkg_counts'])        
            
            # insert the new information into the dataframe
            pkg_date_index = pkg[pkg['date'] == d].index        
            df.at[pkg_date_index[0], 'total_day_pkgs'] = total_pkgs
            
    return df
    
    
    

def add_weather(df_master, df_weather):
    # get a copy of the master dataframe
    df = df_master.copy()
    
    # make arrays for the new columns
    precip_array = np.full((len(df)), 0.0, dtype='float')
    snow_array = np.full((len(df)), 0.0, dtype='float')
    temp_array = np.full((len(df)), 0, dtype='int')
    fog_array = np.full((len(df)), 0, dtype='int')
    
    # insert the new blank columns
    df.insert(loc=len(df.columns), column='precip', value=precip_array)
    df.insert(loc=len(df.columns), column='snow', value=snow_array)
    df.insert(loc=len(df.columns), column='temp', value=temp_array)
    df.insert(loc=len(df.columns), column='fog', value=fog_array)
    
    # get the unique package IDs
    master_idx = pd.unique(df['package_id'])
    
    # loop over all the packages
    for i in tqdm(master_idx):
        # get the package's history
        pkg = df[df['package_id'] == i]
        
        # get the unique dates
        dates = pd.unique(pkg['date'])
        
        for d in dates:
            # get the weather for date in a series
            weather = df_weather[df_weather['date'] == d]
            
            # sequeeze df row into a series
            weather.squeeze()
            
            # get indices for package's date
            pkg_date_index = pkg[pkg['date'] == d].index
            
            # set the weather values
            df.at[pkg_date_index[0], 'precip'] = weather['precip'].values[0]
            df.at[pkg_date_index[0], 'snow'] = weather['snow'].values[0]
            df.at[pkg_date_index[0], 'temp'] = weather['temp'].values[0]
            df.at[pkg_date_index[0], 'temp'] = weather['temp'].values[0]
            
    return df




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
    file_weather = 'df_weather.pkl'
    
    # load the data if it exists
    try:
        df_aggregate = pd.read_pickle(path + file_aggregate)
        df_history = pd.read_pickle(path + file_merged_history)
        df_package = pd.read_pickle(path + file_package)
        df_weather = pd.read_pickle(path + file_weather)
        
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
    print("\n\n")
    
    # initialize master dataframe
    df_master = df_history.copy()
    
    # add aggregate data
    df_master = add_aggregate(df_master, df_aggregate)
    
    # add weather data
    df_master = add_weather(df_master, df_weather)
    
    print(df_master)


if __name__ == "__main__":
    main()