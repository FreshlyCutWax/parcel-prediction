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


# global
min_max_columns = {}


def transform(df_master):
    # make a copy for the new dataframe
    df = df_master.copy()

    # drop the package ID, we don't need it anymore
    df = df.drop(columns='package_id')
    
    #----------------SERVICE------------------------->
    # build service array
    service_array = pd.unique(df['service'].values)
    
    # sort the service array
    service_array = np.sort(service_array)
    
    # build to zip code enumerator
    service_enum = {}
    order = 1
    for s in service_array:
        service_enum[s] = order
        order += 1
        
    # enumerate the service
    df['service'] = df['service'].apply(lambda x: service_enum[x])
    
    
    #----------------SIGNATURE---------------------->
    # convert from boolean to integer
    df['signature'] = df['signature'].apply(lambda x: int(x))
    
    
    #----------------ZIPCODE------------------------>
    # build zipcode array
    zip_array = pd.unique(df['zipcode'].values)
    
    # sort the zip array
    zip_array = np.sort(zip_array)
    
    # build to zip code enumerator
    zip_enum = {}
    order = 1
    for z in zip_array:
        zip_enum[z] = order
        order += 1
        
    # enumerate the zipcodes
    df['zipcode'] = df['zipcode'].apply(lambda x: zip_enum[x]) 

    
    #----------------AREA--------------------------->
    # convert trip 9s to 0
    df['area'] = df['area'].apply(lambda x: x if x != 999 else 0)
    
    # build array of areas
    area_array = pd.unique(df['area'].values)
    
    # sort the area array
    area_array = np.sort(area_array)
    
    # build an area enumerator
    area_enum = {}
    order = 1
    for a in area_array:
        area_enum[a] = order
        order += 1
        
    # enumerate the areas
    df['area'] = df['area'].apply(lambda x: area_enum[x])
    
    
    #----------------PROVIDER----------------------->
    # build array of providers
    provider_array = pd.unique(df['provider'].values)
    
    # sort the array
    provider_array = np.sort(provider_array)
    
    # build enumerator of providers
    provider_enum = {}
    order = 1
    for p in provider_array:
        provider_enum[p] = order
        order += 1
        
    # enumerate the providers
    df['provider'] = df['provider'].apply(lambda x: provider_enum[x])
    
    
    #------DELIVERED/RESOLUTION------------------->
    # convert from boolean to integer
    df['delivered'] = df['delivered'].apply(lambda x: int(x))
    df['resolution'] = df['resolution'].apply(lambda x: int(x))
    
    
    return df
    
    


def min_max(value, min_v, max_v):
    norm_v = (value-min_v)/(max_v-min_v)
    return norm_v




    
def min_max_reverse(norm_v, min_v, max_v):
    value = norm_v*(max_v-min_v) + min_v
    return value
    
    
    
    
def normalize(df_original):
    global min_max_columns

    # make copy of original dataframe
    df = df_original.copy()
       
    # apply min-max normalization across the columns
    # save original min-max values to global dictionary
    for c in df.columns[1:]:
        max_v = np.max(df[c])
        min_v = np.min(df[c])
        min_max_columns[c] = [min_v, max_v]
        
        df[c] = df[c].apply(lambda x: min_max(x, min_v, max_v))
        
    return df
        
def reverse_normalize(df_original):
    global min_max_columns
    
    # make copy of original dataframe
    df = df_original.copy()
    
    # de-normalize values
    for c in df.columns[1:]:
        min_v = min_max_columns[c][0]
        max_v = min_max_columns[c][1]
        df[c] = df[c].apply(lambda x: min_max_reverse(x, min_v, max_v))
        
    # return to original values
    for c in df.columns[:11]:
        df[c] = df[c].round()
        df[c] = df[c].astype('int')

    return df


def main():
	# check if path for weather data exists
    path = 'compiled/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No compiled dataframe directory found.")
        print("Place pickled dataframes in \'compiled\'.")
        print("You may need to compile your dataframes first with preprocessor.py/merger.py.")
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
        
    
    # transform/convert the values to numeric
    df_master = transform(df_master)
    
    # normalize the values
    df_master = normalize(df_master)
    
    print(df_master)
    
    
	
	
if __name__ == "__main__":
    main()