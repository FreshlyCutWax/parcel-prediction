"""
merger

Description: Merge all the dataframes together.
"""

import os
import sys
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm

# ignore warnings
warnings.filterwarnings('ignore')


def compress(df_master):
    # make a copy of the master dataframe
    df = df_master.copy()
    
    # create an array of all our unique package IDs
    master_idx = pd.unique(df_master['package_id'])
    
    # create a list to hold compressed dataframe information
    dataframe_list = []
    
    # days of week dictionary
    day_dict = {'M':0, 'T':1, 'W':2, 'R':3, 'F':4, 'S':5}
    
    for i in tqdm(master_idx):
        # initialize a list to hold compressed package
        pkg = []
        
        # get a dataframe of the an individual package
        df_pkg = df_master[df_master['package_id'] == i]
        
        # add the package ID
        pkg.append(df_pkg['package_id'].iloc[0])
        
        # add the class label
        class_label = df_pkg['status'].iloc[len(df_pkg)-1]
        if class_label == 'D':
            pkg.append(True)
        else:
            pkg.append(False)
        
        # add the days at the station
        dates = pd.unique(df_pkg['date'])
        pkg.append(len(dates))
        
        # add the zipcode
        zips = pd.unique(df_pkg['zipcode'])
        zips = [x for x in zips if x != 0]
        try:
            zipcode = zips[len(zips)-1]
        except:
            zipcode = 0
        pkg.append(zipcode)
        
        # add the provider
        provider = pd.unique(df_pkg['provider'])
        provider = [x for x in provider if x !='']
        try:
            p = provider[len(provider)-1]
        except:
            p = 'None'
        pkg.append(p)
        
        # add the area
        area = pd.unique(df_pkg['assigned_area'])
        area = [x for x in area if x != 0]
        try:
            a = area[len(area)-1]
        except:
            a = 0
        pkg.append(a)
            
        # get the package codes
        s_codes = df_pkg['station_code'].value_counts()
        d_codes = df_pkg['driver_code'].value_counts()
        if 0 in s_codes:
            s_codes = s_codes.drop(0)
        if 0 in d_codes:
            d_codes = d_codes.drop(0)
        
        # convert to dictionary
        s_codes = s_codes.to_dict()
        d_codes = d_codes.to_dict()
        
        # add drive codes to station codes not in station codes
        for v in d_codes:
            if v not in s_codes:
                s_codes[v] = d_codes[v]
        
        codes = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0}
        for v in s_codes:
            codes[v] = s_codes[v]
            
        # add package delays
        pkg.append(codes[1])
        
        # add number of delivery failures
        failure = codes[5] + codes[7] + codes[9]
        pkg.append(failure)
        
        # SKIP WAITING PACKAGE CODES
        # SKIP PROCESSING PACKAGE CODES
        
        # add if the package had an incorrect address
        if codes[6] > 0:
            # add address issue reason, if any
            reason = pd.unique(df_pkg['reason'])  
            if reason.any():
                all_reasons = reason[reason != 0]            
                pkg.append(reason[len(reason)-1])
            # 8 is general address problem
            else:
                pkg.append(8)
        # 0 if no address issues
        else:
            pkg.append(0)
            
        # add if there were any resolutions to package issues
        if codes[3] > 0:
            pkg.append(True)
        else:
            pkg.append(False)
        
        # add the average package count per day
        pkg_counts = pd.unique(df_pkg[df_pkg['total_day_pkgs'] != 0].total_day_pkgs)
        try:
            pkg_mean = round(np.mean(pkg_counts))
        except:
            pkg_mean = round(np.mean(pd.unique(df.total_day_pkgs)))
        pkg.append(pkg_mean)
        
        # add total amount of precipitation during package's life
        rain = np.sum(df_pkg.precip)
        snow = np.sum(df_pkg.snow)
        precip = rain + snow
        pkg.append(precip)
            
        # add the average temperature during the package's life
        temp = np.mean(pd.unique(df_pkg[df_pkg['temp'] != 0].temp))
        pkg.append(int(temp))
        
        # append to compressed history list
        dataframe_list.append(pkg)
    
    # convert the 2D list into a dataframe
    df = pd.DataFrame(dataframe_list, columns=['package_id', 'delivered', 'days', 'zipcode', \
                                                  'provider', 'area',  \
                                                  'delays', 'failures','address', \
                                                  'resolution', \
                                                  'volume', 'precip', 'temp'])
    
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
    
    # insert the data
    for i in tqdm(df_master.itertuples()):
        df.at[i.Index, 'service'] = df_package[df_package['package_id'] == i.package_id].service.values[0]
        df.at[i.Index, 'signature'] = df_package[df_package['package_id'] == i.package_id].signature.values[0]
        
    # convert signature to boolean values
    df['signature'] = df['signature'].apply(lambda x: True if x == 'Y' else False)
                
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
            df.at[pkg_date_index[0], 'fog'] = weather['fog'].values[0]
            
    return df
    
    
    
    
def finalizer(df_master):
    # get a copy of the master dataframe
    df = df_master.copy()
    
    # remove packages with no zipcode
    zero_zipcode = df[df['zipcode'] == 0].index
    df = df.drop(zero_zipcode)
    
    # remove packages with no provider
    none_provider = df[df['provider'] == 'None'].index
    df = df.drop(none_provider)
    
    # remove packages with no area
    zero_area = df[df['area'] == 0].index
    df = df.drop(zero_area)
    
    # reorder columns
    df = df[['package_id', 'delivered', 'service', 'signature', 'zipcode', \
             'provider', 'area', 'days', 'delays', 'failures','address', \
             'resolution', 'volume', 'precip', 'temp']]
    
    # reset the index
    df = df.reset_index(drop=True)
    
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
    print("Adding package history data...")
    df_master = df_history.copy()
    print("Done.", end='\n\n')  
    
    # add aggregate data
    print("Adding aggregate data...")
    df_master = add_aggregate(df_master, df_aggregate)
    print("Done.", end='\n\n')
    
    # add weather data
    print("Adding weather data...")
    df_master = add_weather(df_master, df_weather)
    print("Done.", end='\n\n')
    
    # compress the master dataframe
    print("Compressing master dataframe...")
    df_master = compress(df_master)
    print("Done.", end='\n\n')
    
    # ADD YO PACKAGE DATA HERE PLEASEE!!11!!1!1!!1!!!!
    print("Adding package data...")
    df_master = add_package(df_master, df_package)
    print("Done.", end='\n\n')
    
    # finalize and last cleaning
    print("Finalizing master dataframe...")
    df_master = finalizer(df_master)
    print("Done.", end='\n\n')
    
    # check save path
    if not os.path.exists(path):
        os.makedirs(path)
      
    # save the master dataframe
    df_master.to_pickle(path + 'df_master.pkl')
    df_master.to_csv(path + 'package_data.csv', index=False)
    
    # print success on completion
    print("MASTER DATAFRAME:")
    print(df_master, end='\n\n')
    
    print("----------------------------------")
    print("---------MERGER SUCCESS-----------")
    print("----------------------------------")
    print("Dataframe saved as a csv and pickle file in:", path)
    input("Press enter to end...")




if __name__ == "__main__":
    main()