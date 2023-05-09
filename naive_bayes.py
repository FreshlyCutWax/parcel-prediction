"""
 Naive-Bayes
 
 Description: Implementation of the Naive-Bayes classifier method.
"""

import pandas as pd
import numpy as np
import sys
import os
import copy
from tqdm import tqdm
import warnings

# ignore warnings
warnings.filterwarnings('ignore')



def avc_service(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]
    
    values = np.sort(pd.unique(sample.service.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['service'] == i])
        n_count = len(sample_n[sample_n['service'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_sig(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.signature.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['signature'] == i])
        n_count = len(sample_n[sample_n['signature'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_zipcode(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.zipcode.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['zipcode'] == i])
        n_count = len(sample_n[sample_n['zipcode'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_provider(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.provider.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['provider'] == i])
        n_count = len(sample_n[sample_n['provider'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_area(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.area.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['area'] == i])
        n_count = len(sample_n[sample_n['area'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_days(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.days.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['days'] == i])
        n_count = len(sample_n[sample_n['days'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[4:].itertuples():
        avc.loc[4, True] += i._1
        avc.loc[4, False] += i._2
    
    avc = avc.drop(index=avc[4:].index)
    
    return avc
    
    
    
    
def avc_delays(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.delays.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['delays'] == i])
        n_count = len(sample_n[sample_n['delays'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[3:].itertuples():
        avc.loc[3, True] += i._1
        avc.loc[3, False] += i._2
    
    avc = avc.drop(index=avc[4:].index)
    
    return avc
    
    
    
    
def avc_failures(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.failures.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['failures'] == i])
        n_count = len(sample_n[sample_n['failures'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[2:].itertuples():
        avc.loc[2, True] += i._1
        avc.loc[2, False] += i._2
    
    avc = avc.drop(index=avc[3:].index)
    
    return avc
    
    
    
    
def avc_address(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.address.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['address'] == i])
        n_count = len(sample_n[sample_n['address'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_res(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.resolution.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['resolution'] == i])
        n_count = len(sample_n[sample_n['resolution'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_vol(sample):
    avc = pd.DataFrame(index=['<20k', '20k-30k', '30k+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        volume = i.volume
        class_v = i.delivered
        
        if volume < 20000:
            avc.loc['<20k', class_v] += 1
            
        elif volume >= 20000 and volume < 29999:
            avc.loc['20k-30k', class_v] += 1
            
        else:
            avc.loc['30k+', class_v] += 1
            
    return avc
    
    
    
    
def avc_precip(sample):
    avc = pd.DataFrame(index=['0','1-3', '3+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        precip = i.precip
        class_v = i.delivered
        
        if precip >= 0.0 and precip < 1.0:
            avc.loc['0', class_v] += 1
            
        elif precip >= 1.0 and precip < 3.0:
            avc.loc['1-3', class_v] += 1
            
        else:
            avc.loc['3+', class_v] += 1
            
    return avc
    
    
    
    
def avc_temp(sample):
    avc = pd.DataFrame(index=['<30','30-50', '50+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        temp = i.temp
        class_v = i.delivered
        
        if temp < 30:
            avc.loc['<30', class_v] += 1
            
        elif temp >= 30 and temp < 50:
            avc.loc['30-50', class_v] += 1
            
        else:
            avc.loc['50+', class_v] += 1
            
    return avc




def build_avc(train_sets):
    # dictionary to hold our avc sets in (per training set)
    avc_sets = {}
    
    # build tables for every training set
    for x in tqdm(range(len(train_sets))):
        # get our sample set, divide into classes
        sample = train_sets[x]
        
        sample_d = sample[sample['delivered'] == True]
        sample_n = sample[sample['delivered'] == False]
        
        # dictionary to hold AVC tables
        avc_tables = {}
        
        # build service table
        avc_tables['service'] = avc_service(sample)
        
        # build signature table
        avc_tables['signature'] = avc_sig(sample)
        
        # build zipcode table
        avc_tables['zipcode'] = avc_zipcode(sample)
        
        # build provider table
        avc_tables['provider'] = avc_provider(sample)
        
        # build area table
        avc_tables['area'] = avc_area(sample)
        
        # build days table
        avc_tables['days'] = avc_days(sample)
        
        # build delays table
        avc_tables['delays'] = avc_delays(sample)
        
        # build failures table
        avc_tables['failures'] = avc_failures(sample)
        
        # build address table
        avc_tables['address'] = avc_address(sample)
        
        # build resolution table
        avc_tables['resolution'] = avc_res(sample)
        
        # build volume table
        avc_tables['volume'] = avc_vol(sample)
        
        # build precip table
        avc_tables['precip'] = avc_precip(sample)
        
        # build temp table
        avc_tables['temp'] = avc_temp(sample)
        
        # apply Laplacian correction  
        for i in avc_tables:
            avc_tables[i] += 1
    
        # add sample AVC tables to dictionary
        avc_sets[x] = avc_tables
        
    return avc_sets




def split(sample_list, p):
    # dataframe to hold all of our testing samples
    test_samples = pd.DataFrame(columns=sample_list[0].columns)
    
    # dictionary to hold our training sets
    train_sets = {}
    
    # loop over all the sets to obtain training sets and testing samples
    for i in range(len(sample_list)):
        # get the sample set from our list
        sample_set = sample_list[i]
        
        # get the spliting index
        split_index = int(len(sample_set) * (1 - p))
        
        # do the train/test spliting
        train = sample_set.iloc[:split_index]
        test = sample_set.iloc[split_index:].reset_index(drop=True)
        
        # add our training set to the dictionary
        train_sets[i] = train
        
        # append test samples to our sample dataframe
        test_samples = pd.concat([test_samples, test])
        
    # reset the index for our test sample dataframe
    test_samples = test_samples.reset_index(drop=True)
        
    return train_sets, test_samples
    
    


def main(args):
    # check if path for weather data exists
    path = 'samples/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No sample directory found.")
        print("Place generated sample sets in \'samples/\'.")
        print("You may need to generate your samples first with sampler.py.")
        input("Press enter to continue...")
        sys.exit()
     
    # check if number of sample sets were passed
    if len(args) != 3:
        print("Number of sample sets were not passed.")
        input("Press enter to continue...")
        sys.exit()
        
    # filenames to look for
    filename = path + 'original_sampleX.csv'
    
    # number of sample sets
    num_sets = int(args[1])
    
    # percentage of train/test split
    split_percent = float(args[2])
    
    # list to contain all the sample sets
    sample_list = []
    
    
    # load the sample sets if they exists
    try:
        for x in range(num_sets):
            s = copy.deepcopy(filename)
            s = s.replace('X', str(x))
            sample_list.append(pd.read_csv(s))
    except:
        print("No data found.")
        print("Place generated sample sets in \'samples/\'.")
        input("Press enter to continue...")
        sys.exit()

    # format attributes volume, precip, and temp
    for i in sample_list:
        i['volume'] = i['volume'].astype('int')
        i['temp'] = i['temp'].astype('int')
        i['precip'] = np.round(i['precip'], decimals=2)
    
    # split our sample sets to have training sets and one testing set
    train_sets, test_samples = split(sample_list, split_percent)

    # build AVC tables for our training sets
    avc_sets = build_avc(train_sets)
    
    print(avc_sets)


if __name__ == "__main__":
    main(sys.argv)