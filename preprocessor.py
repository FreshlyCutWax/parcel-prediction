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
    try:
        begin_date = parse_date(args[1])
        end_date = parse_date(args[2])
        
        if begin_date > end_date:
            print("Error: Beginning date is ahead of the end date.")
            quit()
        else:
            print("Begin Date:", begin_date)
            print("End Date:", end_date)
    except:
        print("An error with input dates has occurred.")
        quit()
	

if __name__ == "__main__":
    main(sys.argv)