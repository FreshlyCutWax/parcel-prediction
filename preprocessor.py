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




def str_to_date(str_date):
    # convert string date into date object
    date = datetime.date(int(str_date[0:4]), int(str_date[4:6]), int(str_date[6:8]))
    return date
    
    
    
    
def date_to_str(date):
    # convert date into a string
    str_date = date.strftime('%Y%m%d')
    return str_date




def create_daily(xlsx_file, date):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'Daily')
    
    # load package totals into a series and drop from dataframe
    df = df.drop(len(df)-1)
    
    # if a provider is not in the dataframe, add it for completion
    providers = np.array(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K'], dtype='str')
    for p in providers:
        if p  not in df['Provider'].values:
            values = np.array([p, 0, 0, 0, 0])
            df.loc[-1] = values
            df.index = df.index + 1
            df = df.sort_index()
    
    # sort dataframe by provider
    df = df.sort_values('Provider')
    
    # create an array for the date representing the column to be added to the dataframe
    array_date = np.full((len(df)), 0)
    array_date[0] = date_to_str(date)
    
    # insert column
    df.insert(loc=0, column='Date', value=array_date)
    
    return df




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
            dates[f] = str_to_date(file_list[f][13:21])
            
        print("Start Date:", dates[0])
        print("End Date:", dates[len(dates)-1])
    except:
        print("An error with getting the dates has occurred.")
        sys.exit()
        
    # initialize dataframes
    xlsx = pd.ExcelFile(file_list[0])
    print(create_daily(xlsx, dates[0]))
    
    
	

if __name__ == "__main__":
    main(sys.argv)