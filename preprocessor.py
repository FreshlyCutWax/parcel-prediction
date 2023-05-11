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
from collections import defaultdict

# progress bar
from tqdm import tqdm

# console menu
from consolemenu import *
from consolemenu.items import *

# warning handling
import warnings




#  Global Variables
DATA_PATH = "data/"                 # Path to data
OUTPUT_PATH = "compiled/"           # path for compiled data (dataframes)
SCRIPT_PATH = "logs/"               # path for error logs and such
FILES = []                          # File list
START = datetime.date(2000, 1, 1)   # Start date in date range
END = datetime.date(2000, 1, 1)     # End date in date range
DATAFRAMES = [[], [], [], [], []]   # dataframes [df_aggregate, df_package, df_history]
ERROR_LOGS = [[], [], []]           # Error logs [build_errors, merge_errors, clean_errors]

# ignore warnings
warnings.filterwarnings('ignore')


# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- FILE FUNCTIONS -------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def check_path():
    """
    check_path() -> None
    
    args:
    None
    
    returns:
    None
    
    Desc:
    Check the file path. Creates directories as needed.
    """
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
    """
    capture_filenames() -> None
    
    args:
    None
    
    returns:
    None
    
    Desc:
    Get the data filenames from the data directory.
    """
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
    """
    load_dataframes() -> success (bool)
    
    args:
    None
    
    returns:
    success (bool) -> if operation was successful
    
    Desc:
    Load the dataframes from the pickle files.
    """
    # get the output path
    output_path = get_path('output')
    
    # if file load is successful or not
    success = True
    
    # get aggregate dataframe
    path = os.path.join(output_path, 'df_aggregate.pkl')
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        set_dataframe(df, 'aggregate')
    else:
        success = False
    
    # get package dataframe
    path = os.path.join(output_path, 'df_package.pkl')
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        set_dataframe(df, 'package')
    else:
        success = False
    
    # get history dataframe
    path = os.path.join(output_path, 'df_history.pkl')
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        set_dataframe(df, 'history')
    else:
        success = False
        
    # get pld dataframe
    path = os.path.join(output_path, 'df_pld.pkl')
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        set_dataframe(df, 'pld')
    else:
        success = False
        
    # get merged dataframe
    path = os.path.join(output_path, 'df_merged_history.pkl')
    if os.path.isfile(path):
        df = pd.read_pickle(path)
        set_dataframe(df, 'merged')
        
    # set empty if no success in getting dataframes
    if success == False:
        set_dataframe([], 'aggregate')
        set_dataframe([], 'package')
        set_dataframe([], 'history')
        set_dataframe([], 'pld')
        set_dataframe([], 'merged')
        
    return success
    



def store_dataframes(): 
    """
    store_dataframes() -> success (bool)
    
    args:
    None
    
    returns:
    success (bool) -> if operation was successful
    
    Desc:
    Store the loaded dataframes into a pickle file.
    """
    # get the output path
    output_path = get_path('output')
    
    # if save is successful or not
    success = False
    
    
    if os.path.exists(output_path):
        # save aggregate
        try:
            path = os.path.join(output_path, 'df_aggregate.pkl')
            df = get_dataframe('aggregate')
            df.to_pickle(path)            
            success = True
        except:
            success = False
         
        # save package
        try:    
            path = os.path.join(output_path, 'df_package.pkl')
            df = get_dataframe('package')
            df.to_pickle(path)
            success = True
        except:
            success = False
        
        # save history
        try:
            path = os.path.join(output_path, 'df_history.pkl')
            df = get_dataframe('history')
            df.to_pickle(path)
            success = True
        except:
            success = False
        
        # save pld
        try:
            path = os.path.join(output_path, 'df_pld.pkl')
            df = get_dataframe('pld')
            df.to_pickle(path)
            success = True
        except:
            success = False
        
        # save merged history
        try:
            path = os.path.join(output_path, 'df_merged_history.pkl')
            df = get_dataframe('merged')
            df.to_pickle(path)
        except:
            print('')
        
    return success
    




def load_error_logs():
    """
    load_error_logs() -> success (bool)
    
    args:
    None
    
    returns:
    success (bool) -> if operation was successful
    
    Desc:
    Loads the error logs from pickle files.
    """
    # get our script directory
    script_path = get_path('script')
    
    # if save is successful or not
    success = False
    
    # load build log and set to our global error log list, if it exists
    path = os.path.join(script_path, 'build_errors.pkl')
    if os.path.exists(path):
        with open(path, 'rb') as handle:
            log = pickle.load(handle)
            set_error_log(log, 'build')
            success = True
    
    # load merge log and set to our global error log list, if it exists
    path = os.path.join(script_path, 'merge_errors.pkl')
    if os.path.exists(path):
        with open(path, 'rb') as handle:
            log = pickle.load(handle)
            set_error_log(log, 'merge')
            success = True
        
    return success
    
    
    
def store_error_logs():
    """
    store_error_logs() -> success (bool)
    
    args:
    None
    
    returns:
    success (bool) -> if operation was successful
    
    Desc:
    Loads error logs from pickle files.
    """
    # get our script directory
    script_path = get_path('script')

    # get our error logs
    build_log = get_error_log('build')
    merge_log = get_error_log('merge')
    
    # if save is successful or not
    success = True
    
    # try to save logs to a pickle file
    if os.path.exists(script_path):
        # if there are build errors logged, save them to our script directory
        path = os.path.join(script_path, 'build_errors.pkl')
        if len(build_log) != 0:
            with open(path, 'wb') as handle:               
                pickle.dump(build_log, handle)
        
        # if there are merge errors logged, save them to our script directory
        path = os.path.join(script_path, 'merge_errors.pkl')
        if len(merge_log) != 0:          
            with open(path, 'wb') as handle:               
                pickle.dump(merge_log, handle)
                
    else:
        success = False
        
    return success
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END FILE FUNCTIONS -------------------------------------->
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- MENU FUNTION -------------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def menu(start_string, end_string):
    """
    menu() -> None
    
    args:
    start_string (str) -> file starting date
    end_string (str) -> file ending date
    
    returns:
    None
    
    Desc:
    The script main menu.
    """
    # main menu creation
    main_menu = ConsoleMenu("Preprocessor", start_string + '\n' + end_string)
    
    # menu options
    option_build  = FunctionItem("Build Dataframes", build_data, [])
    option_merge  = FunctionItem("Merge Dataframes", history_merge_pld, [])
    option_clean  = FunctionItem("Clean Dataframes", clean_data, [])
    option_dates  = FunctionItem("Display All File Dates", display_dates, [])
    option_errors = FunctionItem("Show Errors", display_errors, [])
    option_show   = FunctionItem("Show Built Dataframes", display_dataframes, [])
    
    # add options to the menu
    main_menu.append_item(option_build)
    main_menu.append_item(option_merge)
    main_menu.append_item(option_clean)
    main_menu.append_item(option_dates)
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
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END HELPER FUNCTIONS ------------------------------------>
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------- USER DISPLAY FUNCTIONS ------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def display_errors():
    # get the error logs
    build_log = get_error_log('build')
    merge_log = get_error_log('merge')
    
    if len(build_log) != 0 or len(merge_log) != 0:
        # ask the user which errors to see
        print("Which errors would you like to see?")
        print("Options: B = Build, M = Merge")
        print("\n")
        option = input(">: ")
        
        # get the errors for the build data
        if option.upper() == 'B':
            if len(build_log) != 0:
                
                error_index = 1
                # print the error report for data building
                for i in build_log:
                    print("\n")
                    print("Error #", error_index)
                    print("Error while building data for:", i[0])
                    print("Error from file:", i[1])
                    print(type(i[2]), ':', i[2])
                    print('\n')
                    
                    error_index += 1
                    
                print("Build errors found:", len(build_log))
                print("This data was not built properly!", end='\n\n')
            else:
                print("No build errors to display.", end='\n\n')
                
        # get the errors for the  merge data    
        elif option.upper() == 'M':
            if len(merge_log) != 0:
                error_index = 1
                # print error report for data merging
                for i in merge_log:
                    print('\n')
                    print("Error #", error_index)
                    print('Error with package:', i[0])
                    print('Merging PLD date:', i[1])
                    print(type(i[2]), ':', i[2])
                    print('\n')
                    
                    error_index += 1
                
                print("Merge errors found:", len(merge_log))
                print("This data was not merged properly!", end='\n\n')
            else:
                print("No merge errors to display.", end='\n\n')
                
        else:
            print('\n')
            print("Invalid selection.", end='\n\n')
    else:
        print("No error to display.", end='\n\n')
        
    # Wait until the user clears screen
    input("Press enter to continue...")
    
    
    

def display_dataframes():
    # get the dataframes
    df_aggregate = get_dataframe('aggregate')
    df_package = get_dataframe('package')
    df_history = get_dataframe('history')
    df_pld = get_dataframe('pld')
    df_merged = get_dataframe('merged')
    
    # display dataframes if there are any built
    if len(df_aggregate) != 0 or len(df_package) != 0 or len(df_history) != 0:
        # display the dataframes on the console
        print("Dataframe Aggregate:\n", df_aggregate, end='\n\n')
        print("Dataframe Package:\n", df_package, end='\n\n')
        print("Dataframe History:\n", df_history, end='\n\n')
        print("Dataframe PLD:\n", df_pld, end='\n\n')
        print("Dataframe Merged History:\n", df_merged, end='\n\n')
    else:
        print("No dataframes to display.", end='\n\n')

    # Wait until the user clears screen
    input("Press enter to continue...")
    
    
    
    
def display_dates():
    # get file list
    filenames = get_filenames()
    
    # capture date and display
    for f in filenames:
        date = capture_file_date(f)
        print(date)
      
    print("\n\n")
    input("Press enter to continue...")
# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------- END DISPLAY FUNCTIONS ------------------------------------->
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
        print("", end="")
    
    
    
    
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
    elif df_type == 'pld':
        DATAFRAMES[3] = df
    elif df_type == 'merged':
        DATAFRAMES[4] = df
    else:
        print("", end="")
    
    
    
    
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
        df = DATAFRAMES[0].copy()
    elif df_type == 'package':
        df = DATAFRAMES[1].copy()
    elif df_type == 'history':
        df = DATAFRAMES[2].copy()
    elif df_type == 'pld':
        df = DATAFRAMES[3].copy()
    elif df_type == 'merged':
        df = DATAFRAMES[4].copy()
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
        print("", end="")
        
        
        
        
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
    df = df.rename(columns={"Date" : "date", \
                            "Provider" : "provider", \
                            "Areas" : "area_counts", \
                            "Counts" : "pkg_counts", \
                            "Code 85" : "pkg_missing", \
                            "All Codes" : "pkg_returns"})
                            
    # retain order and approriate columns
    df = df[['date', 'provider', 'area_counts', 'pkg_counts', 'pkg_returns', 'pkg_missing']]
    
    # cast types
    df['date'] = df['date'].astype('string')
    df['provider'] = df['provider'].astype('string')
    df['area_counts'] = df['area_counts'].astype('int')
    df['pkg_counts'] = df['pkg_counts'].astype('int')
    df['pkg_returns'] = df['pkg_returns'].astype('int')
    
    # reset the index values from 0 to n-1
    df = df.reset_index(drop=True)
    
    return df




def make_package_dataframe(xlsx_file, sheet_name):
    # read Excel sheets
    df = pd.read_excel(xlsx_file, sheet_name)
    
    # fill any missing 'Service' values as 'S' (Standard service)
    df['Service'] = df['Service'].fillna('S')
    
    # fill any missing 'Signature' values as 'N' (No signature)
    df['Signature'] = df['Signature'].fillna('N')
    
    # rename columns
    df = df.rename(columns={'Package ID' : 'package_id', 'Service' : 'service', 'Signature' : 'signature'})
    
    # retain order and appropriate columns
    df = df[['package_id', 'service', 'signature']]
    
    return df

    
    
    
def make_history_dataframe(xlsx_file, sheet_name):
    # read Excel sheets
    df = pd.read_excel(xlsx_file, sheet_name)

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
    
    # rename columns
    df = df.rename(columns={'Package ID' : 'package_id', 'Date' : 'date', 'DoW' : 'dow', \
                           'Type' : 'type',  'Station Code' : 'station_code', \
                           'Driver Code' : 'driver_code', 'Reason' : 'reason'})
    
    # reorder and retain appropriate columns
    column_order = ['package_id', 'date', 'dow', 'type', 'station_code', 'driver_code', 'reason']
    df = df[column_order]
    
    # cast column types
    for i in df[['package_id', 'date', 'dow', 'type']].columns:
        df[i] = df[i].astype('string')

    # recodedDF = recode_history(df)
    # compress_history(recodedDF)
    return df




def make_pld_dataframe(xlsx_file,  sheet_name, date):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, sheet_name)
    
    # drop Count and Time columns
    df = df.drop(['Count', 'Time'], axis=1)
    
    # create an array for the date representing the column to be added to the dataframe
    array_date = np.full((len(df)), date_to_str(date))
    
    # insert the date column column
    df.insert(loc=0, column='Date', value=array_date)
    
    # rename columns
    column_names = {'Package ID' : 'package_id', 'Zipcode' : 'zipcode', 'Provider' : 'provider', \
                    'Assigned Area' : 'assigned_area', 'Area' : 'loaded_area', \
                    'Station Code' : 'station_code', 'Driver Code' : 'driver_code', 'Date' : 'date'}
                    
    df = df.rename(columns=column_names)
    
    # reorder columns
    column_order = ['package_id', 'zipcode', 'provider', \
                    'assigned_area', 'loaded_area', 'station_code', \
                    'driver_code', 'date']
                    
    df = df[column_order]
    
    
    # ---------------- FILL AND CAST VALUE TYPES ----------------------->
    df['package_id'] = df['package_id'].astype('string')   
    df['zipcode'] = df['zipcode'].fillna(0)
    df['zipcode'] = df['zipcode'].astype('int')  
    df['provider'] = df['provider'].fillna('None')
    df['provider'] = df['provider'].astype('string') 
    df['assigned_area'] = df['assigned_area'].fillna(0)
    df['assigned_area'] = df['assigned_area'].astype('int')
    df['loaded_area'] = df['loaded_area'].fillna(0)
    df['loaded_area'] = df['loaded_area'].astype('int')
    df['station_code'] = df['station_code'].fillna(0)
    df['station_code'] = df['station_code'].astype('int')  
    df['driver_code'] = df['driver_code'].fillna(0)
    df['driver_code'] = df['driver_code'].astype('int')
    
    
    df['date'] = df['date'].astype('string')
    # ------------------- END FILL AND CAST ---------------------------->
    
    return df
    
    
    

def index_history(df_history):
    df = df_history.copy()
    
    # add empty column for the order of events
    array_order = np.full((len(df)), 0, dtype='int')
    df.insert(loc=0, column='order', value=array_order)
    
    # get unique package IDs
    pkgs = pd.unique(df['package_id'])
    
    # progress bar
    pbar = tqdm(pkgs)
    pbar.set_description("Event indexing")
    
    # loop over individual package IDs and index history entries
    for i in pbar:
        df_pkg = df[df['package_id'] == i]
        pkg_indexer = 0
        for j in df_pkg.index:
            df.at[j, 'order'] = pkg_indexer
            pkg_indexer += 1
    
    # reorder columns
    column_order = ['package_id', 'order', 'date', 'dow', 'type', 'station_code', 'driver_code', 'reason']
    df = df[column_order]
    
    return df




def check_df_is_empty(xlsx_file, df_type):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, df_type)
    
    # True if df is empty, False if populated
    is_empty = False
    if df.empty:
        is_empty = True
        
    return is_empty




def compare_dataframe(df_target, df_concat):
    target_pkgs = pd.unique(df_target['package_id'])
    concat_pkgs = pd.unique(df_concat['package_id'])
    
    df = df_concat.copy()
    
    # remove packages from the dataframe that you want to concat
    for i in target_pkgs:
        if i in concat_pkgs:
            df = df[df['package_id'] != i]
            
    return df
# -------------------------------------------------------------------------------------------------------->    
# ---------------------------------------------- END DATAFRAME FUNCTIONS --------------------------------->
# -------------------------------------------------------------------------------------------------------->



# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- BUILD DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def build_data():
    # ------------------- initialize dataframes -------------------->
    print("\nInitializing dataframes...")
    
    # get filenames
    files = get_filenames()
    
    # initialize all dataframes
    df_aggregate = pd.DataFrame(columns=["date", "area_counts", "pkg_counts", "pkg_returns", "pkg_missing"])
    
    df_package = pd.DataFrame(columns=['package_id', 'service', 'signature'])
    
    df_history = pd.DataFrame(columns=['package_id', 'date', 'dow', 'type', 'station_code', \
                                       'driver_code', 'reason'])
                                       
    df_pld = pd.DataFrame(columns=['package_id', 'zipcode', 'provider', \
                                   'assigned_area', 'loaded_area', 'station_code', \
                                   'driver_code', 'date'])
    
    print("Dataframe initialization complete.", end='\n\n')
    # ------------------- initialization complete ------------------>
    
    # list that hold errors for dataframe building
    build_error_log = []
    # ---------------- build the aggregate dataframe --------------->   
    # loop over the rest of the files and append aggregate data to the dataframe
    print("\nBuilding aggregate dataframe...")
    pbar = tqdm(files)
    pbar.set_description('Daily')
    for file in pbar:
        try:
            # load Excel file and file date
            xlsx = pd.ExcelFile(file)
            xlsx_date = capture_file_date(file)
            
            # create dataframe from file and append to aggregate dataframe
            df_xlsx = make_aggregate_dataframe(xlsx, xlsx_date)
            df_aggregate = pd.concat([df_aggregate, df_xlsx])
            
        except Exception as err:
                df_name = 'df_aggregate (Daily)'
                build_error_log.append([df_name, file, err])
    
    # reset indices for the dataframe
    df_aggregate = df_aggregate.reset_index(drop=True)
    
    # completion message
    print("Aggregate dataframe complete.", end='\n\n')
    # ----------------- aggregate dataframe complete --------------->
    
    
    # ----------------- build the package dataframe ---------------->
    # loop over the rest of the files and append package data to the dataframe
    print("Building package dataframe...")
    # process for PLD data
    pbar = tqdm(files)
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
                df_name = 'df_package (SVC)'
                build_error_log.append([df_name, file, err])
                
    # process for 85 data
    pbar = tqdm(files)
    pbar.set_description('85_SVC')
    for file in pbar:     
        try:
            # load Excel file and file date
            xlsx = pd.ExcelFile(file)
            
            # check to see if the dataframe is empty first
            is_empty = check_df_is_empty(xlsx, '85_SVC')
            
            if is_empty != True:
                # create dataframe from file and append to package dataframe
                df_xlsx = make_package_dataframe(xlsx, '85_SVC')
                df_xlsx = compare_dataframe(df_package, df_xlsx)
                df_package = pd.concat([df_package, df_xlsx])
        
        except Exception as err:
                df_name = 'df_package (85_SVC)'
                build_error_log.append([df_name, file, err])
                
                    
    # drop any duplicate in the dataframe
    df_package = df_package.drop_duplicates(subset=['package_id'])
    
    # reset indices for the dataframe
    df_package = df_package.reset_index(drop=True)
    
    # completion message
    print("Package dataframe complete.", end='\n\n')
    # ----------------- package dataframe complete ----------------->
    
    
    # ----------------- build the history dataframe ---------------->   
    # loop over the rest of the files and append history data to the dataframe
    print("Building history dataframe...")
    pbar = tqdm(files)
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
                df_name = 'df_history (HIST)'
                build_error_log.append([df_name, file, err])
                
    # process for 85 data
    pbar = tqdm(files)
    pbar.set_description('85_HIST')
    for file in pbar:     
        try:
            # load Excel file and file date
            xlsx = pd.ExcelFile(file)
            
            # check to see if the dataframe is empty first
            is_empty = check_df_is_empty(xlsx, '85_HIST')
            
            if is_empty != True:
                # create dataframe from file and append to package dataframe
                df_xlsx = make_history_dataframe(xlsx, '85_HIST')
                df_xlsx = compare_dataframe(df_history, df_xlsx)
                df_history = pd.concat([df_history, df_xlsx])
        
        except Exception as err:
            df_name = 'df_history (85_HIST)'
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
    print("Building PLD dataframe...")
    pbar = tqdm(files)
    pbar.set_description('PLD')
    for file in pbar:
        try:
            # load Excel file and file date
            xlsx = pd.ExcelFile(file)
            xlsx_date = capture_file_date(file)
            
            # create dataframe from file and append to history dataframe
            df_xlsx = make_pld_dataframe(xlsx, 'PLD', xlsx_date)
            df_pld = pd.concat([df_pld, df_xlsx])
            
        except Exception as err:
            df_name = 'df_pld (PLD)'
            build_error_log.append([df_name, file, err])
        
    # build 85 data
    pbar = tqdm(files)
    pbar.set_description('85')
    for file in pbar:
        try:
            # load Excel file and file date
            xlsx = pd.ExcelFile(file)
            xlsx_date = capture_file_date(file)
            
            # create dataframe from file and append to history dataframe
            df_xlsx = make_pld_dataframe(xlsx, '85', xlsx_date)
            df_pld = pd.concat([df_pld, df_xlsx])
            
        except Exception as err:
            df_name = 'df_pld (85)'
            build_error_log.append([df_name, file, err])
                
    # drop any duplicate in the dataframe
    df_pld = df_pld.drop_duplicates()
    
    # reset indices for the dataframe
    df_pld = df_pld.reset_index(drop=True)
    
    # completion message
    print("PLD dataframe complete.", end='\n\n')
    # ----------------- PLD dataframe complete --------------------->
    
    """
    # !!! MERGE PLD WITH HISTORY !!!
    print("Merging df_history and df_pld...")
    df_history, merge_error_log = history_merge_pld(df_history, df_pld)
    print("Merging complete.", end='\n\n')
    """
    
    # !!! BUILDING DATA IS NOW COMPLETE !!!
    
    # ----------------- Finishing Processes ------------------------>
    # store built dataframes in our global list
    set_dataframe(df_aggregate, 'aggregate')
    set_dataframe(df_package, 'package')
    set_dataframe(df_history, 'history')
    set_dataframe(df_pld, 'pld')
    
    # save the dataframes in a file
    df_save_success = store_dataframes()
    
    # Get the error counts
    build_error_count = len(build_error_log)
    #merge_error_count = len(merge_error_log)
    
    # store the error logs in our global list
    set_error_log(build_error_log, 'build')
    #set_error_log(merge_error_log, 'merge')
            
    # save the errors in a file
    err_save_success = store_error_logs()
    
    # prompt user with success and error counts  
    print("----------------------------------")
    print("-------DATA BUILD SUCCESS---------")
    print("----------------------------------")
    print("Dataframes have been built.", end='\n')
    print("Total sample size (Packages):", len(df_package))
    print("Build errors:", build_error_count)
    #print("Merge errors:", merge_error_count, end='\n')
    print("Dataframes successfully saved:", df_save_success)
    print("Error logs successfully saved:", err_save_success, end='\n\n')
    
    # hold screen until pressing enter
    input("Press enter to continue...")
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END BUILD DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# -------------------------------------------------- MERGE DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def history_merge_pld():
    # get existing dataframes
    df_history = get_dataframe('history')
    df_pld = get_dataframe('pld')
    
    # new dataframe for merging
    merged_dataframe = df_history.copy()

    # all the unique package IDs from the history dataframe
    history_idx = pd.unique(df_history['package_id'])
    
    # add PLD columns to the history dataframe
    pld_columns = df_pld[['provider', 'assigned_area', 'loaded_area', 'zipcode']].columns
    
    # build blank columns for PLD data
    array_provider = np.full((len(merged_dataframe)), '', dtype='str')
    array_assigned_area = np.full((len(merged_dataframe)), 0, dtype='int')
    array_loaded_area = np.full((len(merged_dataframe)), 0, dtype='int')
    array_zipcode = np.full((len(merged_dataframe)), 0, dtype='int')
    
    # insert new columns into package's history dataframe
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='provider', value=array_provider)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='assigned_area', value=array_assigned_area)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='loaded_area', value=array_loaded_area)
    merged_dataframe.insert(loc=len(merged_dataframe.columns), column='zipcode', value=array_zipcode)
    
    # ------------------------------------------- MERGING CODE ------------------------------>
    print("Merging df_history and df_pld...")
    
    # error tracking for the loop below
    merge_error_log = []
    errors = 0
    pkg_error = False
    
    pbar = tqdm(history_idx)
    pbar.set_description('Errors 0')
    # loop over all the individual packages in history
    for i in pbar:
        # collect and reset error
        if pkg_error == True:           
            pkg_error = False
            pbar_error = 'Errors ' + str(errors)
            pbar.set_description(pbar_error)
    
        # get the package's history
        pkg_hist = df_history[df_history['package_id'] == i]
        
        # get the package's pld data
        pkg_pld = df_pld[df_pld['package_id'] == i]
        
        # for each entry for the package in the PLD dataframe
        for pld_row in pkg_pld.itertuples():
            
            # get the date for the PLD entry
            date = pld_row.date
          
            try:
                # get dataframe from package's history for the PLD date
                pkg_hist_date = pkg_hist[pkg_hist['date'] == date]
                
                # get the first entry for date in package's history
                index = pkg_hist_date.index[0]
                
                # merge data into the dataframe
                merged_dataframe.at[index, 'provider'] = pld_row.provider
                merged_dataframe.at[index, 'assigned_area'] = pld_row.assigned_area
                merged_dataframe.at[index, 'loaded_area'] = pld_row.loaded_area
                merged_dataframe.at[index, 'zipcode'] = pld_row.zipcode
                
            except Exception as err:
                # set package error to true
                pkg_error = True
                errors += 1
                
                # log error
                merge_error_log.append([i, date, err])                
    # --------------------------------------- END MERGING CODE ------------------------------>
    
    # ----------------------------- CLEANUP AND TYPE CASTING -------------------------------->
    # drop missing values
    merged_dataframe = merged_dataframe.dropna()
    
    # type casting for columns
    merged_dataframe.index = merged_dataframe.index.astype('int')    
    merged_dataframe['package_id'] = merged_dataframe['package_id'].astype('string')    
    merged_dataframe['order'] = merged_dataframe['order'].astype('int')    
    merged_dataframe['date'] = merged_dataframe['date'].astype('string')   
    merged_dataframe['dow'] = merged_dataframe['dow'].astype('string')   
    merged_dataframe['station_code'] = merged_dataframe['station_code'].astype('int')   
    merged_dataframe['driver_code'] = merged_dataframe['driver_code'].astype('int')   
    merged_dataframe['reason'] = merged_dataframe['reason'].astype('int')            
    merged_dataframe['zipcode'] = merged_dataframe['zipcode'].astype('int')    
    merged_dataframe['provider'] = merged_dataframe['provider'].astype('string')    
    merged_dataframe['assigned_area'] = merged_dataframe['assigned_area'].astype('int')
    merged_dataframe['loaded_area'] = merged_dataframe['loaded_area'].astype('int')
    # ------------------------- END CLEANUP AND TYPE CASTING -------------------------------->
    
    # set and save dataframe
    set_dataframe(merged_dataframe, 'merged')
    save_success = store_dataframes()
    
    #set and save error logs
    set_error_log(merge_error_log, 'merge')
    save_log_success = store_error_logs()
    
    print("----------------------------------")
    print("---------MERGE SUCCESS------------")
    print("----------------------------------")
    print("Merging completed.")
    print("Merge errors:", errors)
    print("\n\n")            
    print("Dataframe saved successfully:", save_success)
    print("Error log saved successfully:", save_log_success)
    print("\n\n")
    input("Press enter to continue...")
   
# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- END MERGE DATA ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# ------------------------------------------ CLEANING FUNCTIONS ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->
def recode_history(df_history):
    # make a copy of the history dataframe
    df = df_history.copy()
    
    # Sub recodes List
    sub_recode_dict = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 73, 7: 74}
    
    # Use default_dict so that codes other than recodes default to 0
    default_sub_dict = defaultdict(lambda: 0)
    for k, v in sub_recode_dict.items():
        default_sub_dict[v] = k
        
    # do the sub recoding
    for row in df_history.itertuples():
        if row.driver_code == 2:
            new_code = default_sub_dict[row.reason]
            df.at[row.Index, 'reason'] = new_code
        else:
            df.at[row.Index, 'reason'] = 0


    # Recodes List
    recode_dict = {1: {1, 4, 7, 11, 36, 57, 59, 82, 83}, 2: {15, 34, 47, 94, 100}, 3: {40, 300},
                   4: {33, 35, 42, 43, 51, 52, 53, 54, 56, 63, 67, 68}, 5: {12, 16, 27, 37},
                   6: {2, 3, 17}, 7: {85}, 8: {6, 81}, 9: {10}}

    # Use default_dict so that codes other than recodes default to 0
    default_dict = defaultdict(lambda: 0)
    for k, v in recode_dict.items():
        for vv in v:
            default_dict[vv] = k

    # Do the recoding
    for col in ["driver_code", "station_code"]:
        df[col] = df[col].astype(int).apply(lambda x: default_dict[x])

    return df
  
    
    
    
def package_align_history(df_package, df_history): 
    # make copies of the dataframes
    df_package_aligned = df_package.copy()
    df_history_aligned = df_history.copy()
    
    # get the unique package IDs in both df_package and df_history
    hist_ids = pd.unique(df_history['package_id'])
    package_ids = pd.unique(df_package['package_id'])
    
    # for every unique package id in df_package
    pbar = tqdm(package_ids)
    pbar.set_description("df_package")
    for pkg in pbar:
        # remove packages from df_package that are not in the df_history
        if pkg not in hist_ids:
            index = df_package_aligned[df_package_aligned['package_id'] == pkg].index
            df_package_aligned = df_package_aligned.drop(index, axis=0)
            
    # for every unique package id in df_history
    pbar = tqdm(hist_ids)
    pbar.set_description("df_history")
    for pkg in pbar:
        # remove packages from df_history that are not in the df_package
        if pkg not in package_ids:
            indices = df_history_aligned[df_history_aligned['package_id'] == pkg].index        
            df_history_aligned = df_history_aligned.drop(indices, axis=0)
    
    # reset the indices for both dataframes
    df_history_aligned = df_history_aligned.reset_index(drop=True)
    df_package_aligned = df_package_aligned.reset_index(drop=True)
    
    return df_package_aligned, df_history_aligned
    
    
    
    
def remove_empty_pkg(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()
    
    # get all the unique package IDs
    history_idx = pd.unique(df_history['package_id'])
    
    # for every unique package in the history dataframe
    for i in tqdm(history_idx):
        # get the history for the package
        df_pkg = df_history[df_history['package_id'] == i]
        
        # if only entry or less is present in the package's history, remove package
        if len(df_pkg) <= 1:
            indices = df_pkg.index
            df = df.drop(indices, axis=0)
            
    # reset the dataframe indices
    df = df.reset_index(drop=True)
    
    return df
    
    
    
    
def remove_history_dates(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()

    # get the date ranges
    start_date = get_start_date()
    end_date = get_end_date()
    
    # get all the unique package IDs
    history_idx = pd.unique(df_history['package_id'])
    
    # for every unique package in the history dataframe
    for i in tqdm(history_idx):
        # get the history for the package
        df_pkg = df_history[df_history['package_id'] == i]
        
        # check all the dates for the package
        for d in df_pkg['date']:           
            pkg_date = str_to_date(d)   #datetime date from package history
            
            # if package contains dates not in data range, remove package
            if pkg_date < start_date or pkg_date > end_date:
                indices = df_pkg.index
                df = df.drop(indices, axis=0)
                break
                
    # reset the dataframe indices
    df = df.reset_index(drop=True)
                
    return df
    
    
    
    
def remove_history_order(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()

    # get the unique package IDs
    history_idx = pd.unique(df_history['package_id'])
    
    # if the package 'order' index 0 is not present, remove package
    for i in tqdm(history_idx):
        # df of the current package's history
        df_pkg = df_history[df_history['package_id'] == i]
               
        if 0 not in df_pkg['order'].values:
            indices = df_pkg.index
            df = df.drop(indices, axis=0)
            
    # reset the dataframe indices
    df = df.reset_index(drop=True)
    
    return df
    
    
    
    

def truncate_pkg_history(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()
    
    # get the unique package IDs
    history_idx = pd.unique(df_history['package_id'])
    
    for i in tqdm(history_idx):
        # df of the current package's history
        df_pkg = df_history[df_history['package_id'] == i]
        
        # if there is any 'Delivery' status for the package
        if 'Delivery' in df_pkg['type'].values:
            #get order indices of when the package has 'Delivery' status
            pkg_delivery = df_pkg.query("type=='Delivery'")['order'].values
            
            # get the last order index with 'Delivery' status
            last_delivery = pkg_delivery[len(pkg_delivery)-1]
            
            # get any indices that might occur after last 'Delivery' status
            index_after_delivery = df_pkg[df_pkg['order'] > last_delivery].index
            
            # remove those indices after the last 'Delivery' status
            df = df.drop(index_after_delivery, axis=0)
    
    # reset the dataframe indices
    df = df.reset_index(drop=True)
    
    return df
    
    
    
    
def fix_area_digits(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()
    
    # narrow down to rows with a 'loaded_area' value
    df_area = df[df['loaded_area'].values != 0]
    
    # narrow down to rows with 'loaded_area' digits is more than 3
    df_area = df_area[(np.floor(np.log10(df_area['loaded_area'].values))+1).astype('int') > 3]
    
    # narrow down to rows that also has a 'assigned_area' value
    df_area = df_area[df_area['assigned_area'].values != 0]
    
    # narrow down to rows where 'assigned_area' and 'loaded_area' has matching 3 digits
    df_area = df_area[df_area['loaded_area'].values // 10 == df_area['assigned_area'].values]
    
    # correct 'assigned_area' by replacing with 'loaded_area' values
    for i in df_area.itertuples():
        index = i.Index
        value = i.loaded_area
        
        df.at[index, 'assigned_area'] = value
        
    return df
    
    
    
    
def fix_zipcode_provider(df_history):
    # get a copy of the history dataframe
    df = df_history.copy()
    
    # original zip code dictionary
    zip_dict = { \
        'H': [65604, 65605, 65612, 65613, 65617, 65635, 65645, 65646, 65661, \
              65663, 65664, 65674, 65682, 65707, 65710, 65712, 65721, 65725, \
              65727, 65752, 65769, 65770, 65767, 65803, 65804], \
        'B': [65590, 65622, 65632, 65644, 65648, 65685, 65706, 65713, 65722, \
              65757, 65764, 65783, 65786, 65787, 65667, 65662, 65767], \
        'C': [65608, 65614, 65618, 65620, 65627, 65629, 65637, 65638, 65653, \
              65655, 65657, 65666, 65676, 65679, 65680, 65701, 65715, 65720, \
              65729, 65731, 65733, 65741, 65744, 65753, 65755, 65759, 65760, \
              65761, 65762, 65768], \
        'F': [65610, 65631, 65705, 65738, 65781, 65633, 65619, 65802, 65803, 65807], \
        'A': [65611, 65615, 65616, 65624, 65630, 65641, 65656, 65658, 65669, \
              65672, 65675, 65681, 65686, 65726, 65728, 65737, 65739, 65740, \
              65747, 65754, 65771], \
        'K': [65609, 65626, 65660, 65689, 65766, 65775, 65776, 65777, 65788, \
              65789, 65790, 65793], \
        'D': [64738, 64776, 65324, 65326, 65355, 65601, 65603, 65634, 65640, \
              65649, 65650, 65668, 65724, 65732, 65735, 65774, 65779, 65785, \
              64781, 65607], \
        'G': [65742, 65765, 65809, 65802, 65807, 65804], \
        'J': [65714, 65801, 65806, 65808, 65810, 65897, 65898, 65899, 65619, \
            65802, 65803, 65804, 65807], \
        'E': [65636, 65652, 65702, 65704, 65711, 65717, 65746]}
    
    #<---------------------------BUILD DICTIONARIES---------------------------->
    # the base zip code list
    zip_list = []
    
    # build the zip list
    for p, z in zip_dict.items():
        for zz in z:
            zip_list.append(zz)
    
    # dictionary <zip code : area >
    zip_area_dict = {}
    
    # dictionary <zip code : area >
    area_provider_dict = {}
    
    # build the zip_area_dict and area_provider_dict
    # based on the zipcodes and areas inside the data
    for row in df_history.itertuples():
        if row.zipcode in zip_list:
            
            if row.assigned_area != 999 and row.assigned_area != 0:
                zip_area_dict[row.zipcode] = row.assigned_area
            
            if row.loaded_area != 0:
                zip_area_dict[row.zipcode] = row.loaded_area 
                
        if row.assigned_area and row.provider:
            if row.assigned_area != 999:
                area_provider_dict[row.assigned_area] = row.provider
                
    # dictionary < area : zip code >
    area_zip_dict = dict([(value, key) for key, value in zip_area_dict.items()])
    
    # dictionary < zip code : provider >
    zip_provider_dict = {}
    for p, z in zip_dict.items():
        for zz in z:
            zip_provider_dict[zz] = p
        
    # dictionary < provider : zip code >
    provider_zip_dict = {}    
    pbar = tqdm(zip_dict.items())
    for p, z in pbar:
        best_frequency = 0
        best_zipcode = 0
        
        for zz in z:
            pbar.set_description(str(zz))
            frequency = 0
            
            for row in df_history.itertuples():
                if row.zipcode == zz:
                    frequency += 1
                    
            if frequency > best_frequency:
                best_frequency = frequency
                best_zipcode = zz
                
        provider_zip_dict[p] = best_zipcode
    #<---------------------------DICTIONARIES COMPLETE------------------------->    
    
    
    #<---------------------------FIX ZIPCODES/PROVIDERS------------------------>
    # iterate over all the rows in history dataframe
    for row in df_history.itertuples():
        # if the zipcode is not in the area
        if row.zipcode and row.zipcode not in zip_list:
            # if we have an assigned area and it's in our dictionary
            if row.assigned_area and row.assigned_area in area_zip_dict:
                # get estimated zipcode
                zipcode = area_zip_dict[row.assigned_area]
                
                # set zipcode for the right index
                df.at[row.Index, 'zipcode'] = zipcode
                
            # if we have a loaded area and it's in our dictionary    
            elif row.loaded_area and row.loaded_area in area_zip_dict:
                # get estimated zipcode
                zipcode = area_zip_dict[row.loaded_area]
                
                # set zipcode for the right index
                index = row.Index
                df.at[row.Index, 'zipcode'] = zipcode
            
            # if all else fails, use the provider
            elif row.provider != 'None':
                # set zipcode most frequent for provider
                zipcode = provider_zip_dict[row.provider]
                
                # set zipcode for the right index
                index = row.Index
                df.at[row.Index, 'zipcode'] = zipcode
        
        # if we are missing the zipcode
        if not row.zipcode and row.provider != '':
            # if we have an assigned area and it's in our dictionary
            if row.assigned_area and row.assigned_area in area_zip_dict:
                # get estimated zipcode
                zipcode = area_zip_dict[row.assigned_area]
                
                # set zipcode for the right index
                df.at[row.Index, 'zipcode'] = zipcode
                
            # if we have a loaded area and it's in our dictionary    
            elif row.loaded_area and row.loaded_area in area_zip_dict:
                # get estimated zipcode
                zipcode = area_zip_dict[row.loaded_area]
                
                # set zipcode for the right index
                index = row.Index
                df.at[row.Index, 'zipcode'] = zipcode
            
            # if all else fails, use the provider
            elif row.provider != 'None':
                # set zipcode most frequent for provider
                zipcode = provider_zip_dict[row.provider]
                
                # set zipcode for the right index
                index = row.Index
                df.at[row.Index, 'zipcode'] = zipcode
        
        # if we are missing the provider
        if row.provider == 'None':
            
            if row.assigned_area and row.assigned_area in area_provider_dict:
                # get estimated provider
                provider = area_provider_dict[row.assigned_area]
                
                # set provider for the right index
                df.at[row.Index, 'provider'] = provider
                
            elif row.loaded_area and row.loaded_area in area_provider_dict:
                # get estimated provider
                provider = area_provider_dict[row.loaded_area]
                
                # set provider for the right index
                df.at[row.Index, 'provider'] = provider
                
            elif row.zipcode:
                # get estimated provider
                provider = zip_provider_dict[row.zipcode]
                
                # set provider for the right index
                df.at[row.Index, 'provider'] = provider
        
        # if we are missing the assigned area
        if not row.assigned_area and row.provider != '':
            
            if row.zipcode and row.zipcode in zip_area_dict:
                # get estimated area
                area = zip_area_dict[row.zipcode]
                
                # set area for the right index
                df.at[row.Index, 'assigned_area'] = area
    #<----------------------ZIPCODES/PROVIDERS COMPLETE------------------------>
    
    # get unique package IDs
    df_idx = pd.unique(df_history['package_id'])

    # remove packages with their provider as 'None'
    pbar = tqdm(df_idx)
    pbar.set_description("Removing 'None' Provider")
    for i in pbar:
        df_pkg = df[df['package_id'] == i]
        
        if 'None' in df_pkg['provider'].values:
            indices = df_pkg.index
            df = df.drop(indices, axis=0)
            
            
    # remove packages with no zipcodes
    pbar = tqdm(df_idx)
    pbar.set_description("Removing No Zipcode")
    for i in pbar:
        df_pkg = df[df['package_id'] == i]
        
        zips = pd.unique(df_pkg['zipcode'])
        zips = [x for x in zips if x != 0]
        
        if len(zips) == 0:
            indices = df_pkg.index
            df = df.drop(indices, axis=0)
            
    # reset the index
    df = df.reset_index(drop=True)
    
    return df 
    
    
    
    
def type_to_status(df_history):
    # make a copy of the history dataframe
    df = df_history.copy()
    
    # rename the columns
    df = df.rename(columns={'type' : 'status'})
    
    # change 'Status Code' value to 'code'
    df['status'] = df['status'].str.replace('Status Code', 'S')
    
    # change 'Delivery' value to 'delivery'
    df['status'] = df['status'].str.replace('Delivery', 'D')
    
    # get all the unique package IDs
    history_idx = pd.unique(df_history['package_id'])
    
    # add a no delivery status as 'X' if no delivery status
    for i in tqdm(history_idx):
        # dataframe of the current package
        df_pkg = df[df['package_id'] == i]
        
        # all the status values of the current package
        status = df_pkg['status'].values
        
        # add no delivery status 'X' if delivery status not present
        if 'D' not in status:
            # get the index of the last entry for the package
            indices = df_pkg.index
            index = indices[len(indices)-1]
            
            # set no delivery status in our dataframe
            df.at[index, 'status'] = 'X'
            
    return df
# -------------------------------------------------------------------------------------------------------->
# -------------------------------------- END CLEANING FUNCTIONS ------------------------------------------>
# -------------------------------------------------------------------------------------------------------->




# -------------------------------------------------------------------------------------------------------->
# ---------------------------------------------- CLEAN DATA ---------------------------------------------->
# -------------------------------------------------------------------------------------------------------->
def clean_data():
    # get copies of the current built dataframes
    df_package = get_dataframe('package')
    df_history = get_dataframe('merged')
    
    print("Original df_package length:", len(df_package))
    print("Original df_history length:", len(df_history))
    print("\n\n")
    
    # remove and resolve non-delivery area zipcodes
    print("Process #1: Fixing zipcodes and Providers...")
    df_history = fix_zipcode_provider(df_history)
    print("Process #1 completed.", end='\n\n') 
    
    # remove packages that don't have a history
    print("Process #2: Removing packages with unusable histories...")
    df_history = remove_empty_pkg(df_history)
    print("Process #2 completed.", end='\n\n')
    
    # enforce date range of all packages to be within the start and end date range
    print("Process #3: Enforcing date range for the history dataframe...")
    df_history = remove_history_dates(df_history)
    print("Process #3 completed.", end='\n\n')
    
    # remove packages that have incorrect history ordering (missing 0th index)
    # some packages had weird 'Delivery' at first index, with date 9999/99/99
    # those packages are now missing the first index, we need to remove them
    print("Process #4: Removing packages with ordering issues...")
    df_history = remove_history_order(df_history)
    print("Process #4 completed.", end='\n\n')
    
    # truncate package histories to not show history after 'Delivery' status
    print("Process #5: Truncating package histories after 'Delivery' status...")
    df_history = truncate_pkg_history(df_history)
    print("Process #5 completed.", end='\n\n')    
    
    # convert the codes in the history dataframe
    print("Process #6: Recoding the codes...")
    df_history = recode_history(df_history)
    print("Process #6 completed.", end='\n\n')     
    
    # fix the loaded_area attribute in history dataframe
    print("Process #7: Fixing loaded_area digits in history dataframe...")
    df_history = fix_area_digits(df_history)
    print("Process #7 completed.", end='\n\n')    
    
    # modify history 'type' attribute to show status
    print("Process #8: Transforming 'type' attribute and adding no delivery status...")
    df_history = type_to_status(df_history)
    print("Process #8 completed.", end='\n\n')
    
    # we want to align df_package and df_history to have the same packages in them
    print("Process #9: Aligning the package and history dataframes...")
    df_package, df_history = package_align_history(df_package, df_history)
    print("Process #9 completed.", end='\n\n')
    
    # set and save dataframe
    set_dataframe(df_package, 'package')
    set_dataframe(df_history, 'merged')
    save_success = store_dataframes()
    
    print("----------------------------------")
    print("---------CLEAN SUCCESS------------")
    print("----------------------------------")
    print("Cleaning completed.")
    print("\n\n")            
    print("Dataframe saved successfully:", save_success)
    print("New df_package length:", len(df_package))
    print("New df_history length:", len(df_history))
    print("\n\n")
    input("Press enter to continue...")
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
    
    set_start_date(start_date)
    set_end_date(end_date)
    
    start_string = "Start Date: " + str(start_date)
    end_string = "End Date: " + str(end_date)
    
    
    # load dataframes and error logs
    load_df_success = load_dataframes()
    load_log_success = load_error_logs()
    
    # check if file loading was successful
    if load_df_success == True:
        print("Dataframes loaded successfully.", end='\n\n')
    else:
        print("No dataframes loaded.", end='\n\n')
    
    if load_log_success == True:
        print("Error logs loaded successfully.", end='\n\n')
    else:
        print("No Error logs loaded.", end='\n\n')
        
    # Wait for user to continue
    input("Press enter to continue...")
    
    # prompt the main menu
    menu(start_string, end_string)

    
    
    
if __name__ == "__main__":
    main(sys.argv)