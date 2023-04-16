"""
Preprocessor

Description:
Script for preprocessing and cleaning package data. Store
the preprocessed data into a pickle file for mining operations.
"""
import numpy as np
import pandas as pd

import sys
import datetime

from os import listdir
from os.path import isfile, join


def addfiles(path):
    # get some data filenames
    filenames = []
    
    for file in listdir(path):
        name = join(path, file)
        
        if isfile(name):
            filenames.append(name)
            
    return filenames


def parse_date(str_date):
    # convert string date into date object
    date = datetime.date(int(str_date[0:4]), int(str_date[4:6]), int(str_date[6:8]))
    return date


def read_data(start_date, end_date):
    # add some code here
    pass


def main(args):
    # default data path
    data_path = "data/"
    file_list = []
    
    # attempt to get the data file list
    try:
        file_list = addfiles(data_path)
    except:
        print("An error with getting the filenames has occurred.")
        sys.exit()
        
    # attempt to get the range of dates from the files
    dates = np.full(len(file_list), datetime.date(2000, 1, 1))
    try:
        for f in range(len(file_list)):
            dates[f] = parse_date(file_list[f][13:21])
            
        print("Start Date:", dates[0])
        print("End Date:", dates[len(dates)-1])
    except:
        print("An error with getting the dates has occurred.")
        sys.exit()
	

if __name__ == "__main__":
    main(sys.argv)