"""
Preprocessor

Description:
Script for preprocessing and cleaning package data. Store
the preprocessed data into a pickle file for mining operations.
"""
import numpy as np
import pandas as pd

import sys
import os
import datetime
import pickle




def get_data_filenames(path):
    # list for holding filenames
    filenames = []
    
    # if the data path exists, try and get the filenames
    if os.path.exists(path):
        try:
            for file in os.listdir(path):
                name = os.path.join(path, file)
                
                if os.path.isfile(name):
                    filenames.append(name)
        except:
            print("An error has occurred with getting the filenames.")        
    else:
        # if no path for the data, create default path and warn user
        os.makedirs(path)
        print("No directory for the data was found.") 
        print("Default data directory \'data/\' has been created.")
            
    return filenames




def get_file_dates(filenames):
    # create an empty array of dates
    dates = []
    
    # try to obtain the dates from the filenames
    try:
        for f in filenames:
            dates.append(str_to_date(f[13:21]))
    except:
        dates = []
        print("An error with getting the dates has occurred.")
        print("Proper filenames needed: PACKAGE_yyyymmdd")
    
    return dates




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
    
    # rename columns
    df = df.rename(columns={"Counts" : "Pkg Counts", "Code 85" : "Missing", "All Codes" : "Pkg Returns"})
    
    return df




def main(args):
    # default data path
    data_path = "data/"
    file_list = []
    
    if len(args) > 1:
        data_path = args[1]
        
    print("Data path:", "\'" + data_path + "\'")
    
    # attempt to get the data file list
    file_list = get_data_filenames(data_path)
 
 
    if not file_list:
        print("No data was found. Preprocessing has ended.")
        sys.exit()
    
    
    # attempt to get the range of dates from the files
    date_list = get_file_dates(file_list)
    
    if not date_list:
        print("There is no dates to display. Preprocessing has ended.")
        sys.exit()
        
    print("Start Date:", date_list[0])
    print("End Date:", date_list[len(date_list)-1])
    
        
    # initialize dataframes
    xlsx = pd.ExcelFile(file_list[0])
    df_daily = create_daily(xlsx, date_list[0])
    df_daily.to_pickle('df_daily.pkl')
    
    
	

if __name__ == "__main__":
    main(sys.argv)