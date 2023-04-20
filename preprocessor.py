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



#  Global Variables
PATH = "data/"                      # Path to data
FILES = []                          # File list
START = datetime.date(2000, 1, 1)   # Start date in date range
END = datetime.date(2000, 1, 1)     # End date in date range



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




def get_file_date(filename):
    date = datetime.date(2000, 1, 1)
    
    # try to obtain the date from the filename
    try:
        date = str_to_date(filename[13:21])
    except:
        print("An error with getting the date for " + filename + " has occurred.")
        print("Proper filename format needed: PACKAGE_yyyymmdd")
    
    return date




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
            
    
    # sort dataframe by provider
    df = df.sort_values('Provider')
    
    # create an array for the date representing the column to be added to the dataframe
    array_date = np.full((len(df)), date_to_str(date))
    # array_date[0] = date_to_str(date)
    
    # insert column
    df.insert(loc=0, column='Date', value=array_date)
    
    # rename columns
    df = df.rename(columns={"Counts" : "Pkg Counts", "Code 85" : "Missing", "All Codes" : "Pkg Returns"})
    
    # reset the index values from 0 to n-1
    df = df.reset_index(drop=True)
    
    return df




def create_package(xlsx_file):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'SVC')
    
    # fix any missing 'Service' values
    df['Service'] = df['Service'].fillna('S')
    
    # fix any missing 'Signature' values
    df['Signature'] = df['Signature'].fillna('N')
    
    return df

    
    
    
def create_history(xlsx_file):
    df = pd.read_excel(xlsx_file, 'HIST')
    
    # drop the time stamp
    df = df.drop('Time', axis=1)
    
    # split the date and Day of Week
    date_split = df['Date'].str.split('\xa0',n=1, expand=True)
    date_split = date_split.rename(columns={0 : 'Date', 1 : 'DoW'})
    
    default_date = '99999999'
    for i in range(len(date_split['Date'])):
        # try to reformat the date
        try:
            old_date = date_split['Date'].iloc[i].split('/')
            
            # concat year
            if old_date[2] == '99':
                new_date = default_date
            else:
                new_date = '20' + old_date[2]
                
                # concat month
                if len(old_date[0]) < 2:
                    new_date = new_date + '0' + old_date[0]
                else:
                    new_date = new_date + old_date[0]
                
                # concat day
                if len(old_date[1]) < 2:
                    new_date = new_date + '0' + old_date[1]
                else:
                    new_date = new_date + old_date[1]
        except:
            # if error reformating date, set to default
            new_date = default_date
            
        date_split['Date'].iloc[i] = new_date
        
    # merge the reformatted date and DoW back into original dataframe
    df = df.merge(date_split, how='left', left_index=True, right_index=True)
    df = df.drop('Date_x', axis=1)
    
    # reorder and rename column names
    order = ['Package ID', 'Type', 'Date', 'DoW', 'Station Code', 'Driver Code']
    df = df.rename(columns={'Date_y' : 'Date'}).loc[:, order]
    
    return df




def build_dataframes():
    # reference global variables
    global FILES
    global START
    
    # initialize dataframes, xlsx is Excel workbook
    xlsx = pd.ExcelFile(FILES[0])
    
    df_daily = create_daily(xlsx, START)
    df_package = create_package(xlsx)
    df_hist = create_history(xlsx)
    
    # build dataframes
    if len(FILES) > 1:
        for file in FILES[1:]:
            xlsx = pd.ExcelFile(file)
            xlsx_date = get_file_date(file)
            
            # build daily dataframe
            df_xlsx = create_daily(xlsx, xlsx_date)
            df_daily = pd.concat([df_daily, df_xlsx])
            
            # build package dataframe
            df_xlsx = create_package(xlsx)
            df_package = pd.concat([df_package, df_xlsx])
            
            # build history dataframe
            df_xlsx = create_history(xlsx)
            df_hist = pd.concat([df_hist, df_xlsx])
    
    # drop any duplicate package entries in 'package' and 'history' dataframes
    df_package = df_package.drop_duplicates(subset=['Package ID'])
    df_hist = df_hist.drop_duplicates()
    
    # reset the indices for each dataframe
    df_daily = df_daily.reset_index(drop=True)
    df_package = df_package.reset_index(drop=True)
    df_hist = df_hist.reset_index(drop=True)
    
    return df_daily, df_package, df_hist




def main(args):
    # reference global variables
    global PATH
    global FILES
    global START
    global END
    
    # check if custom file path is given
    if len(args) > 1:
        PATH = args[1]
        
    print("Data path:", "\'" + PATH + "\'")
    
    # attempt to get the data file list
    FILES = get_data_filenames(PATH)
 
    # check to see if we got the data files we need
    if not FILES:
        print("No data was found. Preprocessing has ended.")
        sys.exit()
    
    
    # attempt to get the range of dates from the files
    START = get_file_date(FILES[0])
    END = get_file_date(FILES[len(FILES)-1])
    print("Start Date:", START)
    print("End Date:", END)

    daily, package, hist = build_dataframes()
    
    daily.to_pickle('df_daily.pkl')
    package.to_pickle('df_package.pkl')
    hist.to_pickle('df_hist.pkl')
    print(daily)
    print(package)
    print(hist)
    
    
	

if __name__ == "__main__":
    main(sys.argv)