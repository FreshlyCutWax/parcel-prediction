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



def separate_class(df_master):
    # get a copy of the original df
    df = df_master.copy()
    
    # delivery class dataframe
    df_d = df[df['delivered'] == 1]
    
    # no delivery class dataframe
    df_n = df[df['delivered'] == 0]
    
    return df_d, df_n
    
    
    
    
def df_to_array(df):
    # convert to array
    arr = df.to_numpy(dtype='float', copy=True)
    return arr
    
    
    
    
def distance(t1, t2):
    # create an empty vector to hold the differences between attributes
    vector = np.zeros(len(t1), dtype='float')
    
    # compute the squared difference between each attribute
    for i in range(len(vector)):
        vector[i] = (t1[i] - t2[i])**2
        
    # take the square root of the sum of the vector
    dist = math.sqrt(np.sum(vector))
    
    return dist
    
    
    

def knn(samples, t, k):
    # list to hold nearest neighbors
    n = {}
    
    # loop over all data and find k nearest neighbors
    for i in range(len(samples)):
        if len(n) < k:
            # add a new i to our nearest neighbors
            n[i] = distance(t, samples[i])
        else:
            # get the distance between t and some i
            dist_i = distance(t, samples[i])
            
            # get the i in n with that maximum distance
            max_n = max(n, key=n.get)
            
            # if the distance is smaller than maximum distance in n, replace
            if dist_i < n[max_n]:
                del n[max_n]
                n[i] = dist_i
                
    # we want the full list of knn
    return n
    
    
    
    
def smote(class_array, nn_x, sample_x):
    # do SMOTE on our minority class!!
    # lets make a copy of our samples for safety
    samples = class_array.copy()
    
    # make a list to hold our new samples
    gen_samples = []
    
    # generate index for sample array
    index = np.arange(0, len(samples), 1, dtype='int')
    
    # build new samples for every minority sample
    for i in tqdm(index):
        # get the sample, remove from sample array
        sample = samples[i]
        knn_array = np.delete(samples, i, axis=0)
        
        # get the nearest neighbors and the index for them
        # we are getting 3 nearest neighbors for our new samples
        nn = knn(knn_array, sample, nn_x)
        nn_index = list(nn.keys())
        
        # now lets make our new samples
        for i in nn_index:
            #generate 3 new samples per nearest neighbor
            for j in range(sample_x):
                # generate a random number between 0 and 1
                gen = random.uniform(0, 1)
                
                # take the distance between attributes and multiply by our random distance
                # generates a new sample
                new_sample = []
                for a in range(len(knn_array[i])):
                    if sample[a] == knn_array[i][a]:
                        new_sample.append(sample[a])
                    else:
                        new_att = abs(sample[a] - knn_array[i][a])
                        new_att = new_att * gen
                        new_sample.append(new_att)
                
                # append new generated sample to our new sample list
                gen_samples.append(new_sample)
            
    return gen_samples
    
    


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
    
    # separate the classes into two different dataframes
    df_d, df_n = separate_class(df_master)
    
    # convert to 2D arrays
    arr_d = df_to_array(df_d)
    arr_n = df_to_array(df_n)
    
    print(arr_d)
    print(arr_n)
    
    
	
	
if __name__ == "__main__":
    main()