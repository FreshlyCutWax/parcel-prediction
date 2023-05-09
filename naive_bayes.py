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

    print(test_samples)


if __name__ == "__main__":
    main(sys.argv)