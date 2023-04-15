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
	# command line input date as 'yyyymmdd'
    # first date is begin date, second date is end date
    data_path = "data/"
    
    try:
        start_date = parse_date(args[1])
        end_date = parse_date(args[2])
        
        if start_date > end_date:
            print("Error: Beginning date is ahead of the end date.")
            sys.exit()
        else:
            print("Start Date:", start_date)
            print("End Date:", end_date)
    except:
        print("An error with the input dates has occurred.")
        sys.exit()
	

if __name__ == "__main__":
    main(sys.argv)