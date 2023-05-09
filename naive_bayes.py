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
    if len(args) != 2:
        print("Number of sample sets were not passed.")
        input("Press enter to continue...")
        sys.exit()
        
    # filenames to look for
    filename = path + 'original_sampleX.csv'
    
    # number of sample sets
    num_sets = int(args[1])
    
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
        
    print(sample_list[0])



if __name__ == "__main__":
    main(sys.argv)