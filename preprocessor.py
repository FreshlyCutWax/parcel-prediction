"""
Preprocessor

Description:
Script for preprocessing and cleaning package data. Store
the preprocessed data into a pickle file for mining operations.
"""

# data libraries
import numpy as np
import pandas as pd

# Python libraries
import sys
import os
import datetime
import pickle
import time

# progress bar
from tqdm import tqdm

# console menu
from consolemenu import *
from consolemenu.items import *

# warning handling
import warnings



#  Global Variables
DATA_PATH = "data/"                 # Path to data
OUTPUT_PATH = "compiled_data/"      # path for compiled data (dataframes)
SCRIPT_PATH = "preprocessor/"       # path for error logs and such
FILES = []                          # File list
START = datetime.date(2000, 1, 1)   # Start date in date range
END = datetime.date(2000, 1, 1)     # End date in date range
DATAFRAMES = [[], [], []]           # dataframes [df_aggregate, df_package, df_history]
ERROR_LOGS = [[], [], []]           # Error logs [build_errors, merge_errors, clean_errors]

# ignore warnings
warnings.filterwarnings('ignore')


# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- FILE FUNCTIONS -------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def check_path():
    # get path values
    data_path = get_path('data')
    output_path = get_path('output')
    script_path = get_path('script')
    
    # if no path for the data files, create path
    if not os.path.exists(data_path):      
        os.makedirs(data_path)
     
    # if no path for the output files, create path
    if not os.path.exists(output_path):        
        os.makedirs(output_path)
    
    # if no path for the script files, create path    
    if not os.path.exists(script_path):        
        os.makedirs(script_path)



def capture_filenames():   
    # get the path for the data
    data_path = get_path('data')
    
    
    # if the data path exists, try and get the filenames
    try:
        for file in os.listdir(data_path):
            name = os.path.join(data_path, file)
            
            if os.path.isfile(name):
                append_filename(name)
    except:
        print("An error has occurred with getting the filenames.")




def load_dataframes():    
    # get the output path
    output_path = get_path('output')
    
    # try to read stored data
    success = True
    try:
        # get aggregate dataframe
        name = os.path.join(output_path, 'df_aggregate.pkl')
        if os.path.isfile(name):
            df = pd.read_pickle(name)
            set_dataframe(df, 'aggregate')
        
        # get package dataframe
        name = os.path.join(output_path, 'df_package.pkl')
        if os.path.isfile(name):
            df = pd.read_pickle(name)
            set_dataframe(df, 'package')
        
        # get history dataframe
        name = os.path.join(output_path, 'df_history.pkl')
        if os.path.isfile(name):
            df = pd.read_pickle(name)
            set_dataframe(df, 'history')
    except:
        success = False
        
    return success
    



def store_dataframes(): 
    # get the output path
    output_path = get_path('output')
    
    # try to store the dataframes in a pickle file
    success = True
    try:
        path = os.path.join(output_path, 'df_aggregate.pkl')
        df = get_dataframe('aggregate')
        df.to_pickle(path)
        
        path = os.path.join(output_path, 'df_package.pkl')
        df = get_dataframe('package')
        df.to_pickle(path)
        
        path = os.path.join(output_path, 'df_history.pkl')
        df = get_dataframe('history')
        df.to_pickle(path)
    except:
        success = False
        
    return success
    




def load_error_logs():
    # get our script directory
    script_path = get_path('script')
    
    # try to open pickle file containing our error logs
    success = True
    try:
        # load build log and set to our global error log list, if it exists
        path = os.path.join(script_path, 'build_errors.pkl')
        if os.path.exists(path):
            with open(path, 'rb') as handle:
                ERROR_LOGS[0] = pickle.load(handle)
        
        # load merge log and set to our global error log list, if it exists
        path = os.path.join(script_path, 'merge_errors.pkl')
        if os.path.exists(path):
            with open(path, 'rb') as handle:
                ERROR_LOGS[1] = pickle.load(handle)
    except:
        success = False
        
    return success
    
    
    
def store_error_logs():
    # get our script directory
    script_path = get_path('script')

    # get our error logs
    build_log = get_error_log('build')
    merge_log = get_error_log('merge')
    
    # try to save logs to a pickle file
    success = True
    try:
        # if there are build errors logged, save them to our script directory
        if len(build_log) != 0:
            path = os.path.join(script_path, 'build_errors.pkl')
            with open(path, 'wb') as handle:               
                pickle.dump(build_log, handle)
        
        # if there are merge errors logged, save them to our script directory
        if len(merge_log) != 0:
            path = os.path.join(script_path, 'merge_errors.pkl')
            with open(path, 'wb') as handle:               
                pickle.dump(merge_log, handle)
    except:
        success = False
        
    return success
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END FILE FUNCTIONS -------------------------------------->
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- MENU FUNTION -------------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def menu(start_string, end_string):
    # main menu creation
    main_menu = ConsoleMenu("Preprocessor", start_string + '\n' + end_string)
    
    # menu options
    option_build  = FunctionItem("Build Dataframes", build_data, [])
    option_clean  = MenuItem("Clean Dataframes")
    option_errors = MenuItem("Show Errors")
    option_show   = MenuItem("Show Current Dataframes")
    
    # add options to the menu
    main_menu.append_item(option_build)
    main_menu.append_item(option_clean)
    main_menu.append_item(option_errors)
    main_menu.append_item(option_show)
    
    main_menu.show()
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END MENU FUNTION ---------------------------------------->
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- HELPER FUNCTIONS ------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def capture_file_date(filename):
    # set default date
    date = datetime.date(2000, 1, 1)
    
    # try to obtain the date from the filename
    try:
        date = str_to_date(filename[13:21])
    except:
        print("\n")
        print("An error with getting the date for " + filename + " has occurred.")
        print("Proper filename format needed: PACKAGE_yyyymmdd")
        print("\n")
    
    return date




def str_to_date(str_date):
    # convert string date into date object
    date = datetime.date(int(str_date[0:4]), int(str_date[4:6]), int(str_date[6:8]))
    return date
    
    
    
    
def date_to_str(date):
    # convert date into a string
    str_date = date.strftime('%Y%m%d')
    return str_date




def display_errors():
    global ERROR_LOGS
    
    # print the error report for data building
    for i in ERROR_LOGS[0]:
        print("\n") 
        print("Error while building data for:", i[0])
        print("Error from file:", i[1])
        print(type(i[2]), ':', i[2], end='\n')
        
    print("\nThis data was not build!", end='\n\n')
    
    # print error report for data merging
    for i in ERROR_LOGS[1]:
        print('\n')
        print('Error with package:', i[0])
        print('For date:', i[1], end='\n\n')
        
    print("\nThis data was not merged!", end='\n\n')
    
    
    

def display_dataframes():
    # get the dataframes
    df_aggregate = get_dataframe('aggregate')
    df_package = get_dataframe('package')
    df_history = get_dataframe('history')
    
    # display the dataframes on the console
    print("Dataframe Aggregate:\n", df_aggregate, end='\n')
    print("Dataframe Package:\n", df_package, end='\n')
    print("Dataframe History:\n", df_history, end='\n') 
    input("\nPress enter to continue...")
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END HELPER FUNCTIONS ------------------------------------>
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- GETTERS AND SETTERS ------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def set_path(path, path_type):
    """
    set_path(path, path_type) -> None
    
    args:
    path (string) -> directory path
    path_type (string) -> 'data', 'output', 'script'
    
    returns:
    None
    
    Desc:
    Update the global value for a specified path in the directory.
    """
    global DATA_PATH
    global OUTPUT_PATH
    global SCRIPT_PATH
    
    # match path type and set path
    # nothing if wrong path type
    if path_type == 'data':
        DATA_PATH = path
    elif path_type == 'output':
        OUTPUT_PATH = path
    elif path_type == 'script':
        SCRIPT_PATH = path
    else:
        pass
    
    
    
    
def get_path(path_type):
    """
    get_path(path_type) -> path (string)
    
    args:
    path_type (string) -> 'data', 'output', 'script'
    
    returns:
    path (string) -> directory path
    
    Desc:
    Get the global value for a specified path in the directory.
    """
    global DATA_PATH
    global OUTPUT_PATH
    global SCRIPT_PATH
    
    # match path type and set path to return value
    # empty if incorrect path type
    path = ''
    if path_type == 'data':
        path = DATA_PATH
    elif path_type == 'output':
        path = OUTPUT_PATH
    elif path_type == 'script':
        path = SCRIPT_PATH
    else:
        path = ''
    
    return path
    
    
    
    
def set_filenames(file_list):
    """
    set_filenames(file_list) -> None
    
    args:
    file_list (string list) -> list of filenames
    
    returns:
    None
    
    Desc:
    Set the global list of filenames.
    """
    global FILES
    
    # set the global file list
    FILES = file_list


    
    
    
    
def append_filename(filename):
    """
    append_filename(filename) -> None
    
    args:
    filename (string) -> a file name
    
    returns:
    None
    
    Desc:
    Append filename to the global list of filenames.
    """
    global FILES
    
    # append the file name to global file list
    FILES.append(filename)
    
    
    
    
def get_filenames():
    """
    get_filename() -> file_list (string tuple)
    
    args:
    None
    
    returns:
    file_list (string) -> list of filenames
    
    Desc:
    Get the list of filenames from the global filename list.
    """
    global FILES
    
    # return the global filename list
    file_list = tuple(FILES)
    return file_list
    
    
    
    
def set_start_date(date):
    """
    set_start_date(date) -> None
    
    args:
    date (datetime object) -> date yyyy-mm-dd
    
    returns:
    None
    
    Desc:
    Set the global start date for the file list.
    """
    global START
    
    # if given date is a string, convert to date object
    if type(date) == str:
        date = str_to_date(date)
    
    # set global start variable
    START = date
    
    
    
    
def get_start_date():
    """
    get_start_date() -> start_date (datetime object)
    
    args:
    None
    
    returns:
    start_date (datetime object) -> date yyyy-mm-dd
    
    Desc:
    Get the global start date for the file list.
    """
    global START
    
    # return the global start date
    start_date = START
    return start_date
    
    
    
 
def set_end_date(date):
    """
    set_end_date(date) -> None
    
    args:
    date (datetime object) -> date yyyy-mm-dd
    
    returns:
    None
    
    Desc:
    Set the global end date for the file list.
    """
    global END
    
    # if given date is a string, convert to date object
    if type(date) == str:
        date = str_to_date(date)
    
    # set global start variable
    END = date
    
    
    
    
def get_end_date():
    """
    get_end_date() -> end_date (datetime object)
    
    args:
    None
    
    returns:
    end_date (datetime object) -> date yyyy-mm-dd
    
    Desc:
    Get the global end date for the file list.
    """
    global END
    
    # return the global end date
    end_date = END
    return end_date

    
    
    
def set_dataframe(df, df_type):
    """
    set_dataframes(df, df_type) -> None
    
    args:
    df (dataframe object) -> dataframe
    df_type (string) -> 'aggregate', 'package', or 'history'
    
    returns:
    None
    
    Desc:
    Set the global list that stores the built dataframes.
    """
    global DATAFRAMES
    
    if df_type == 'aggregate':
        DATAFRAMES[0] = df        
    elif df_type == 'package':
        DATAFRAMES[1] = df      
    elif df_type == 'history':
        DATAFRAMES[2] = df
    else:
        pass
    
    
    
    
def get_dataframe(df_type):
    """
    get_dataframes() -> dataframe (dataframe object)
    
    args:
    df_type (string) -> 'aggregate', 'package', or 'history'
    
    returns:
    dataframe (datetime object) -> selected dataframe
    
    Desc:
    Get a dataframe from the global list that stores the built dataframes.
    """
    global DATAFRAMES
    
    df = None
    if df_type == 'aggregate':
        df = DATAFRAMES[0]
    elif df_type == 'package':
        df = DATAFRAMES[1]
    elif df_type == 'history':
        df = DATAFRAMES[2]
    else:
        df = None
        
    return df
    
    
    

def set_error_log(log, log_type):
    """
    set_error_logs(log, log_type) -> None
    
    args:
    log (list) -> strings and dates
    log_type (string) -> 'build', 'merge', or 'clean'
    
    returns:
    None
    
    Desc:
    Set the global list that stores the error logs.
    """
    global ERROR_LOGS
    
        
    if log_type == 'build':
        ERROR_LOGS[0] = log
    elif log_type == 'merge':
        ERROR_LOGS[1] = log
    elif log_type == 'clean':
        ERROR_LOGS[2] = log
    else:
        pass
        
        
        
        
def get_error_log(log_type):
    """
    get_error_logs(log, log_type) -> log (tuple)
    
    args:
    log_type (string) -> 'build', 'merge', or 'clean'
    
    returns:
    log (tuple) -> selected log
    
    Desc:
    Get an error log from the global list that stores the error logs.
    """
    global ERROR_LOGS
    
    # get the specified error log from the global list
    log = []
    if log_type == 'build':
        log = ERROR_LOGS[0]
    elif log_type == 'merge':
        log =  ERROR_LOGS[1]
    elif log_type == 'clean':
        log = ERROR_LOGS[2]
    else:
        log = []
        
    return log
    pass
# -------------------------------------------------------------------------------------------------------->
# ------------------------------------------- END GETTERS AND SETTERS ------------------------------------>
# -------------------------------------------------------------------------------------------------------->

# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- DATAFRAME FUNCTIONS --------------------------------->
# -------------------------------------------------------------------------------------------------------->

def make_aggregate_dataframe(xlsx_file, date):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'Daily')
    
    # drop total row from dataframe
    df = df.drop(len(df)-1)
    
    # if a provider is not in the dataframe, add it for completion
    providers = np.array(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K'], dtype='str')
    for p in providers:
        if p not in df['Provider'].values:
            values = np.array([p, 0, 0, 0, 0])
            df.loc[-1] = values
            df.index = df.index + 1
            
    
    # sort dataframe by provider
    df = df.sort_values('Provider')
    
    # create an array for the date representing the column to be added to the dataframe
    array_date = np.full((len(df)), date_to_str(date))
    
    # insert the date column column
    df.insert(loc=0, column='Date', value=array_date)
    
    # rename columns
    df = df.rename(columns={"Areas" : "Area Counts", "Counts" : "Pkg Counts", \
                            "Code 85" : "Missing", "All Codes" : "Pkg Returns"})
    
    # reset the index values from 0 to n-1
    df = df.reset_index(drop=True)
    
    return df




def make_package_dataframe(xlsx_file, which):
    # read Excel sheets
    df = pd.read_excel(xlsx_file, which)
    
    # fill any missing 'Service' values as 'S' (Standard service)
    df['Service'] = df['Service'].fillna('S')
    
    # fill any missing 'Signature' values as 'N' (No signature)
    df['Signature'] = df['Signature'].fillna('N')
    
    return df

    
    
    
def make_history_dataframe(xlsx_file, which):
    # read Excel sheets
    df = pd.read_excel(xlsx_file, which)

    # drop the time stamp
    df = df.drop('Time', axis=1)
    
    # ---------------------------- CONVERT DATES ---------------------------> 
    # split the date and Day of Week
    date_split = df['Date'].str.split('\xa0',n=1, expand=True)
    date_split = date_split.rename(columns={0 : 'Date', 1 : 'DoW'})
    
    # loop over all the dates and convert them
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
    
    # drop the original date column
    df = df.drop('Date', axis=1)
    
    # merge the reformatted date and DoW back into original dataframe
    df = df.merge(date_split, how='left', left_index=True, right_index=True)
    # ------------------------- END CONVERT DATES -------------------------->  
    
    # ------------------------- SPLIT STATION CODES ------------------------>          
    # replace weird escape sequence and split codes and subcodes
    station_split = df['Station Code'].str.replace('\xa0\xa0', ' ')
    station_split = station_split.str.split(' ', n=1, expand=True)
    
    # drop un-needed Station Subcodes
    station_split = station_split.drop(1, axis=1)
    
    # rename columns
    station_split = station_split.rename(columns={0 : 'Station Code'})
    
    # replace empty code markers with zero
    station_split.loc[station_split['Station Code'] == '---', 'Station Code'] = '0'
    
    # fill in missing/NaN values and cast as integer type
    for i in station_split.columns:
        station_split[i] = station_split[i].str.replace('[a-zA-Z]', '', regex=True)
        station_split[i] = station_split[i] = station_split[i].fillna(0)
        station_split[i] = station_split[i].astype('int')
    
    # drop the original Station Codes column
    df = df.drop(('Station Code'), axis=1)
    
    # merge the codes back into the original dataframe
    df = df.merge(station_split, how='left', left_index=True, right_index=True)
    # ------------------------- STATION CODES COMPLETE --------------------->
    
    
    # ------------------------- SPLIT DRIVER CODES ------------------------->  
    # replace weird escape sequence and split codes and subcodes
    driver_split = df['Driver Code'].str.replace('\xa0\xa0', ' ')
    driver_split = driver_split.str.split(' ', n=1, expand=True)
    
    # rename columns
    driver_split = driver_split.rename(columns={0 : 'Driver Code', 1 : 'Reason'})
    
    # replace empty code markers with zero
    driver_split.loc[driver_split['Driver Code'] == '---', 'Driver Code'] = '0'
    driver_split.loc[driver_split['Reason'] == '---', 'Reason'] = '0'
    
    
    # fill in missing/NaN values and cast as integer type
    for i in driver_split.columns:
        driver_split[i] = driver_split[i].str.replace('[a-zA-Z]', '', regex=True)
        driver_split[i] = driver_split[i] = driver_split[i].fillna(0)
        driver_split[i] = driver_split[i].astype('int')
     
    # drop the original Station Codes column
    df = df.drop(('Driver Code'), axis=1)
     
    # merge the codes back into the original dataframe
    df = df.merge(driver_split, how='left', left_index=True, right_index=True)
    # ------------------------- DRIVER CODES COMPLETE ---------------------->
    
    # reorder and rename column names
    column_order = ['Package ID', 'Date', 'DoW', 'Type', 'Station Code', 'Driver Code', 'Reason']
    df = df[column_order]
    
    # cast column types
    for i in df[['Package ID', 'Date', 'DoW', 'Type']].columns:
        df[i] = df[i].astype('string')
    
    return df




def make_pld_dataframe(xlsx_file, date):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'PLD')
    df_missing = pd.read_excel(xlsx_file, '85')
    
    # add missing packages to the rest of the packages
    df = pd.concat([df, df_missing])
    
    # drop Count and Time columns
    df = df.drop(['Count', 'Time'], axis=1)
    
    # create an array for the date representing the column to be added to the dataframe
    array_date = np.full((len(df)), date_to_str(date))
    
    # insert the date column column
    df.insert(loc=0, column='Date', value=array_date)
    
    # reorder columns and rename columns
    column_order = ['Package ID', 'Zipcode', 'Provider', \
                    'Assigned Area', 'Loaded Area', 'Station Code', \
                    'Driver Code', 'Date']
    df = df.rename(columns={'Area' : 'Loaded Area'}).loc[:, column_order]
    
    
    # ---------------- FILL AND CAST VALUE TYPES ----------------------->
    df['Package ID'] = df['Package ID'].astype('string')   
    df['Zipcode'] = df['Zipcode'].fillna(0)
    df['Zipcode'] = df['Zipcode'].astype('int')  
    df['Provider'] = df['Provider'].fillna('None')
    df['Provider'] = df['Provider'].astype('string') 
    df['Assigned Area'] = df['Assigned Area'].fillna(0)
    df['Assigned Area'] = df['Assigned Area'].astype('int')
    df['Loaded Area'] = df['Loaded Area'].fillna(0)
    df['Loaded Area'] = df['Loaded Area'].astype('int')
    df['Station Code'] = df['Station Code'].fillna(0)
    df['Station Code'] = df['Station Code'].astype('int')  
    df['Driver Code'] = df['Driver Code'].fillna(0)
    df['Driver Code'] = df['Driver Code'].astype('int')
    
    
    df['Date'] = df['Date'].astype('string')
    # ------------------- END FILL AND CAST ---------------------------->
    
    return df
    
    
    

def index_history(df_history):
    df = df_history
    
    # add empty column for the order of events
    array_order = np.full((len(df)), 0, dtype='int')
    df.insert(loc=0, column='Order', value=array_order)
    
    # get unique package IDs
    pkgs = pd.unique(df['Package ID'])
    
    # progress bar
    pbar = tqdm(pkgs)
    pbar.set_description("Event indexing")
    
    # loop over individual package IDs and index history entries
    for i in pbar:
        df_pkg = df[df['Package ID'] == i]
        pkg_indexer = 0
        for j in df_pkg.index:
            df.at[j, 'Order'] = pkg_indexer
            pkg_indexer += 1
    
    # reorder columns
    column_order = ['Package ID', 'Order', 'Date', 'DoW', 'Type', 'Station Code', 'Driver Code', 'Reason']
    df = df[column_order]
    
    return df




def compare_dataframe(df_target, df_concat):
    target_pkgs = pd.unique(df_target['Package ID'])
    concat_pkgs = pd.unique(df_concat['Package ID'])
    
    df = df_concat.copy()
    
    for i in target_pkgs:
        if i in concat_pkgs:
            df = df[df['Package ID'] != i]
            
    return df

    
    
    
def history_merge_pld(df_history, df_pld):
    # initialize new dataframe for the merge
    merged_dataframe = df_history

    # all the unique package IDs from the history dataframe
    history_idx = pd.unique(merged_dataframe['Package ID'])
    
    # add PLD columns to the history dataframe
    pld_columns = df_pld[['Provider', 'Assigned Area', 'Loaded Area', 'Zipcode']].columns
    
    # build blank columns for PLD data
    array_provider = np.full((len(merged_dataframe)), '', dtype='str')
    array_assigned_area = np.full((len(merged_dataframe)), 0, dtype='int')
    array_loaded_area = np.full((len(merged_dataframe)), 0, dtype='int')
    array_zipcode = np.full((len(merged_dataframe)), 0, dtype='int')
    
    # insert new columns into package's history dataframe
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='Provider', value=array_provider)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='Assigned Area', value=array_assigned_area)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='Loaded Area', value=array_loaded_area)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='Zipcode', value=array_zipcode)
    
    # ------------------------------------------- MERGING CODE ------------------------------>
    
    # error tracking for the loop below
    merge_error_log = []
    errors = 0
    pkg_error = False
    
    # loop over all the individual packages in history
    for i in tqdm(history_idx):
        # collect and reset error
        if pkg_error == True:
            errors += 1
            pkg_error = False
    
        # get the package's history
        df_pkg_hist = df_history[df_history['Package ID'] == i]
        
        # get the package's pld data
        df_pkg_pld = df_pld[df_pld['Package ID'] == i]
        
        # for each entry for the package in the PLD dataframe
        for j in df_pkg_pld['Date']:
            # dataframe from package's history for this date
            df_hist_dates = df_pkg_hist[df_pkg_hist['Date'] == j]
            # print(df_hist_dates)
            
            try:
                # tuple in package PLD for the date
                df_pld_date = df_pkg_pld[df_pkg_pld['Date'] == j]
                
                # driver code form the PLD date tuple
                driver_code = df_pld_date['Driver Code'].values[0]
                
        
                # first attempt to get an index
                index = df_hist_dates[df_hist_dates['Driver Code'] == driver_code].first_valid_index()
                
                # second attempt
                if index == None:
                    index = df_hist_dates[df_hist_dates['Station Code'] == driver_code].first_valid_index()
                
                # third attempt
                if index == None:
                    index = df_pkg_hist[df_pkg_hist['Date'] == j].first_valid_index()
                
                merged_dataframe.at[index, 'Provider'] = df_pld_date['Provider'].values[0]
                merged_dataframe.at[index, 'Assigned Area'] = df_pld_date['Assigned Area'].values[0]
                merged_dataframe.at[index, 'Loaded Area'] = df_pld_date['Loaded Area'].values[0]
                merged_dataframe.at[index, 'Zipcode'] = df_pld_date['Zipcode'].values[0]
                
            except:
                # set package error to true
                pkg_error = True
                
                # log error
                merge_error_log.append([i, j])                
    # --------------------------------------- END MERGING CODE ------------------------------>
    
    # ----------------------------- CLEANUP AND TYPE CASTING -------------------------------->
    # drop missing values
    merged_dataframe = merged_dataframe.dropna()
    
    # type casting for columns
    merged_dataframe.index = merged_dataframe.index.astype('int')    
    merged_dataframe['Package ID'] = merged_dataframe['Package ID'].astype('string')    
    merged_dataframe['Order'] = merged_dataframe['Order'].astype('int')    
    merged_dataframe['Date'] = merged_dataframe['Date'].astype('string')   
    merged_dataframe['DoW'] = merged_dataframe['DoW'].astype('string')   
    merged_dataframe['Station Code'] = merged_dataframe['Station Code'].astype('int')   
    merged_dataframe['Driver Code'] = merged_dataframe['Driver Code'].astype('int')   
    merged_dataframe['Reason'] = merged_dataframe['Reason'].astype('int')            
    merged_dataframe['Zipcode'] = merged_dataframe['Zipcode'].astype('int')    
    merged_dataframe['Provider'] = merged_dataframe['Provider'].astype('string')    
    merged_dataframe['Assigned Area'] = merged_dataframe['Assigned Area'].astype('int')
    merged_dataframe['Loaded Area'] = merged_dataframe['Loaded Area'].astype('int')
    # ------------------------- END CLEANUP AND TYPE CASTING -------------------------------->
            
    return merged_dataframe, merge_error_log
# -------------------------------------------------------------------------------------------------------->    
# ---------------------------------------------- END DATAFRAME FUNCTIONS --------------------------------->
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- BUILD DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def build_data():
    global ERROR_LOGS
    
    # ------------------- initialize dataframes -------------------->
    print("\nInitializing dataframes...")
    
    # get filenames
    files = get_filenames()
    
    # get the start date
    start_date = get_start_date()
    
    # load Excel Workbook
    xlsx = pd.ExcelFile(files[0])
    
    # initialize all dataframes
    df_aggregate = make_aggregate_dataframe(xlsx, start_date)
    df_package = make_package_dataframe(xlsx, 'SVC')
    df_history = make_history_dataframe(xlsx, 'HIST')
    df_pld = make_pld_dataframe(xlsx, start_date)
    
    print("Dataframe initialization complete.", end='\n\n')
    # ------------------- initialization complete ------------------>
    
    # list that hold errors for dataframe building
    build_error_log = []
    # ---------------- build the aggregate dataframe --------------->   
    # loop over the rest of the files and append aggregate data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(files) > 1:
        print("\nBuilding aggregate dataframe...")
        for file in tqdm(files[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to aggregate dataframe
                df_xlsx = make_aggregate_dataframe(xlsx, xlsx_date)
                df_aggregate = pd.concat([df_aggregate, df_xlsx])
                
            except Exception as err:
                    df_name = 'df_aggregate'
                    build_error_log.append([df_name, file, err])
    
    # reset indices for the dataframe
    df_aggregate = df_aggregate.reset_index(drop=True)
    
    # completion message
    print("Aggregate dataframe complete.", end='\n\n')
    # ----------------- aggregate dataframe complete --------------->
    
    
    # ----------------- build the package dataframe ---------------->
    # loop over the rest of the files and append package data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(files) > 1:
        print("Building package dataframe...")
        # process for PLD data
        pbar = tqdm(files[1:])
        pbar.set_description('SVC')
        for file in pbar:
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                
                # create dataframe from file and append to package dataframe
                df_xlsx = make_package_dataframe(xlsx, 'SVC')
                df_xlsx = compare_dataframe(df_package, df_xlsx)
                df_package = pd.concat([df_package, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_package'
                    build_error_log.append([df_name, file, err])
                    
        # process for 85 data
        pbar = tqdm(files[1:])
        pbar.set_description('85_SVC')
        for file in pbar:     
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                # xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to package dataframe
                df_xlsx = make_package_dataframe(xlsx, '85_SVC')
                df_xlsx = compare_dataframe(df_package, df_xlsx)
                df_package = pd.concat([df_package, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_package'
                    build_error_log.append([df_name, file, err])
                
                    
    # drop any duplicate in the dataframe
    df_package = df_package.drop_duplicates(subset=['Package ID'])
    
    # reset indices for the dataframe
    df_package = df_package.reset_index(drop=True)
    
    # completion message
    print("Package dataframe complete.", end='\n\n')
    # ----------------- package dataframe complete ----------------->
    
    
    # ----------------- build the history dataframe ---------------->   
    # loop over the rest of the files and append history data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(files) > 1:
        print("Building history dataframe...")
        pbar = tqdm(files[1:])
        pbar.set_description('HIST')
        for file in pbar:
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                
                # create dataframe from file and append to history dataframe
                df_xlsx = make_history_dataframe(xlsx, 'HIST')
                df_xlsx = compare_dataframe(df_history, df_xlsx)
                df_history = pd.concat([df_history, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_history'
                    build_error_log.append([df_name, file, err])
                    
        # process for 85 data
        pbar = tqdm(files[1:])
        pbar.set_description('85_HIST')
        for file in pbar:     
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                
                # create dataframe from file and append to package dataframe
                df_xlsx = make_history_dataframe(xlsx, '85_HIST')
                df_xlsx = compare_dataframe(df_history, df_xlsx)
                df_history = pd.concat([df_history, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_history'
                    build_error_log.append([df_name, file, err])
    
    # drop any duplicate in the dataframe
    df_history = df_history.drop_duplicates()
    
    # reset indices for the dataframe
    df_history = df_history.reset_index(drop=True)
    
    # !!! HISTORY EVENT INDEXING HERE !!!
    df_history = index_history(df_history)
    
    # completion message
    print("History dataframe complete.", end='\n\n')
    # ----------------- history dataframe complete ----------------->
    
    
    # ----------------- build the PLD dataframe -------------------->
    # loop over the rest of the files and append history data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(files) > 1:
        print("Building PLD dataframe...")
        for file in tqdm(files[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to history dataframe
                df_xlsx = make_pld_dataframe(xlsx, xlsx_date)
                df_pld = pd.concat([df_pld, df_xlsx])
                
            except Exception as err:
                df_name = 'df_pld'
                build_error_log.append([df_name, file, err])
                
    # drop any duplicate in the dataframe
    df_pld = df_pld.drop_duplicates()
    
    # reset indices for the dataframe
    df_pld = df_pld.reset_index(drop=True)
    
    # completion message
    print("PLD dataframe complete.", end='\n\n')
    # ----------------- PLD dataframe complete --------------------->
    
    
    # !!! MERGE PLD WITH HISTORY !!!
    print("Merging df_history and df_pld...")
    df_history, merge_error_log = history_merge_pld(df_history, df_pld)
    print("Merging complete.", end='\n\n')
    
    # !!! BUILDING DATA IS NOW COMPLETE !!!
    
    # ----------------- Finishing Processes ------------------------>
    # store built dataframes in our global list
    set_dataframe(df_aggregate, 'aggregate')
    set_dataframe(df_package, 'package')
    set_dataframe(df_history, 'history')
    
    # save the dataframes in a file
    df_save_success = store_dataframes()
    
    # Get the error counts
    build_error_count = len(build_error_log)
    merge_error_count = len(merge_error_log)
    
    # store the error logs in our global list
    set_error_log(build_error_log, 'build')
    set_error_log(merge_error_log, 'merge')
            
    # save the errors in a file
    err_save_success = store_error_logs()
    
    # prompt user with success and error counts
    print("----------------------------------")
    print("----------BUILD ERRORS------------")
    print("----------------------------------")
    print("Build errors:", build_error_count)
    print("Merge errors:", merge_error_count)
    print("\n\n")
    
    print("----------------------------------")
    print("-------DATA BUILD SUCCESS---------")
    print("----------------------------------")
    print("Dataframes have been built.")
    print("Dataframes successfully saved:", df_save_success)
    print("Error logs successfully saved:", err_save_success)
    print("\n\n")
    
    # hold screen until pressing enter
    input("Press enter to continue...")
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END BUILD DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# ------------------------------------------ CLEANING FUNCTIONS ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->
# PUT SOME CODE HERE

# -------------------------------------------------------------------------------------------------------->
# -------------------------------------- END CLEANING FUNCTIONS ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- CLEAN DATA ---------------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def clean_data():
    pass
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END CLEAN DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->

def main(args):    
    # check if custom file path is given
    if len(args) > 1:
        set_path(args[1], 'data')
    
    # check file paths
    check_path()
    
    # attempt to capture a file list from directory
    capture_filenames()
    
    # get the file list
    files = get_filenames()
 
    # check to see if we got the data files we need
    if not files:
        print("No data was found. Preprocessor could not start.", end='\n\n')
        
        # get the path and show to user
        data_path = get_path('data')
        print("The appropriate directories have now been created.")
        print("Please add your data to the current data directory:", data_path)
        sys.exit()
    
    
    # attempt to get the range of dates from the files
    start_date = capture_file_date(files[0])
    end_date = capture_file_date(files[len(files)-1]) 
    start_string = "Start Date: " + str(start_date)
    end_string = "End Date: " + str(end_date)
    
    # prompt the main menu
    menu(start_string, end_string)

    
	

if __name__ == "__main__":
    main(sys.argv)