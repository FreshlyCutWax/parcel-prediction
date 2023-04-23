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

# other libraries
from tqdm import tqdm



#  Global Variables
PATH = "data/"                      # Path to data
FILES = []                          # File list
START = datetime.date(2000, 1, 1)   # Start date in date range
END = datetime.date(2000, 1, 1)     # End date in date range


# -------------------------------------------------- FILE FUNCTIONS ----------------->

def capture_filenames(path):
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
# ---------------------------------------------- END FILE FUNCTIONS ----------------->



# -------------------------------------------------- HELPER FUNCTIONS --------------->
def str_to_date(str_date):
    # convert string date into date object
    date = datetime.date(int(str_date[0:4]), int(str_date[4:6]), int(str_date[6:8]))
    return date
    
    
    
    
def date_to_str(date):
    # convert date into a string
    str_date = date.strftime('%Y%m%d')
    return str_date




def make_dataframes_exception_message(df_name, file, error):
    print("\n\n") 
    print("Error while building data for:", df_name)
    print("Error from file:", file)
    print(type(error), ':', error)
    print("This data will be ignored!", end='\n\n')
# ---------------------------------------------- END HELPER FUNCTIONS --------------->



# -------------------------------------------------- DATAFRAME FUNCTIONS ------------>

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




def make_package_dataframe(xlsx_file):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'SVC')
    
    # fix any missing 'Service' values
    df['Service'] = df['Service'].fillna('S')
    
    # fix any missing 'Signature' values
    df['Signature'] = df['Signature'].fillna('N')
    
    return df

    
    
    
def make_history_dataframe(xlsx_file):
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
    
    # drop the original date column
    df = df.drop('Date', axis=1)
    
    # merge the reformatted date and DoW back into original dataframe
    df = df.merge(date_split, how='left', left_index=True, right_index=True)
    
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
    
    
    # ------------------------- HISTORY ENTRY ORDERING --------------------->
    # add empty column for the order of events
    array_order = np.full((len(df)), 0, dtype='int')
    df.insert(loc=0, column='Order', value=array_order)
    
    # get unique package IDs
    pkgs = pd.unique(df['Package ID'])
    
    # loop over individual package IDs and index history entries
    for i in pkgs:
        df_pkg = df[df['Package ID'] == i]
        pkg_indexer = 1
        for j in df_pkg.index:
            df.at[j, 'Order'] = pkg_indexer
            pkg_indexer += 1            
    # ------------------------- HISTORY ENTRY ORDERING --------------------->
    
    # reorder and rename column names
    column_order = ['Package ID', 'Order', 'Date', 'DoW', 'Type', 'Station Code', 'Driver Code', 'Reason']
    df = df[column_order]
    
    # cast column types
    for i in df[['Package ID', 'Date', 'DoW', 'Type']].columns:
        df[i] = df[i].astype('string')
    
    return df




def make_pld_dataframe(xlsx_file, date):
    # read Excel sheet
    df = pd.read_excel(xlsx_file, 'PLD')
    
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
    
    
    
def history_merge_pld(df_history, df_pld):
    # all the unique package IDs from the history dataframe
    history_idx = pd.unique(df_history['Package ID'])
    
    # initialize the new merged dataframe
    column_names = np.append(df_history.columns, df_pld)
    df = pd.DataFrame(df_history.columns) 
    
    # loop over all individual packages
    for i in history_idx:
        # make a dataframe for the package's history
        df_pkg_hist = df_history[df_history['Package ID'] == i]
        
        # loop over all the rows in the package's history...
        for r in range(len(df_pkg_hist)):
            
            # when the Driver Code is not 0...
            if df_pkg_hist['Driver Code'].iloc[r] > 0:
                # get the date from current row
                hist_date = df_pkg_hist['Date'].iloc[i]
                
                # get the row from PLD matching the current row
                pld_row_data = df_pld[['Package ID'] == i]
                # pld_row_data = pld_row_data['Date'] == hist_date]
            
            
    pass
# ---------------------------------------------- END DATAFRAME FUNCTIONS ------------>




# -------------------------------------------------- BUILD DATA --------------------->
def build_data():
    global FILES
    global START
    
    # ------------------- initialize dataframes -------------------->
    print("Initializing dataframes...")
    
    # load Excel Workbook
    xlsx = pd.ExcelFile(FILES[0])
    
    # initialize all dataframes
    df_aggregate = make_aggregate_dataframe(xlsx, START)
    df_package = make_package_dataframe(xlsx)
    df_history = make_history_dataframe(xlsx)
    df_pld = make_pld_dataframe(xlsx, START)
    
    print("Dataframe initialization complete.")
    # ------------------- initialization complete ------------------>
    
    
    # ---------------- build the aggregate dataframe --------------->   
    # loop over the rest of the files and append aggregate data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(FILES) > 1:
        print("Building aggregate dataframe...")
        for file in tqdm(FILES[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to aggregate dataframe
                df_xlsx = make_aggregate_dataframe(xlsx, xlsx_date)
                df_aggregate = pd.concat([df_aggregate, df_xlsx])
                
            except Exception as err:
                    df_name = 'df_aggregate'
                    make_dataframes_exception_message(df_name, file, err)
    
    # reset indices for the dataframe
    df_aggregate = df_aggregate.reset_index(drop=True)
    
    # completion message
    print("Aggregate dataframe complete.")
    # ----------------- aggregate dataframe complete --------------->
    
    
    # ----------------- build the package dataframe ---------------->
    # loop over the rest of the files and append package data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(FILES) > 1:
        print("Building package dataframe...")
        for file in tqdm(FILES[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                # xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to package dataframe
                df_xlsx = make_package_dataframe(xlsx)
                df_package = pd.concat([df_package, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_package'
                    make_dataframes_exception_message(df_name, file, err)
                    
    # drop any duplicate in the dataframe
    df_package = df_package.drop_duplicates(subset=['Package ID'])
    
    # reset indices for the dataframe
    df_package = df_package.reset_index(drop=True)
    
    # completion message
    print("Package dataframe complete.")
    # ----------------- package dataframe complete ----------------->
    
    
    # ----------------- build the history dataframe ---------------->   
    # loop over the rest of the files and append history data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(FILES) > 1:
        print("Building history dataframe...")
        for file in tqdm(FILES[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                # xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to history dataframe
                df_xlsx = make_history_dataframe(xlsx)
                df_history = pd.concat([df_history, df_xlsx])
            
            except Exception as err:
                    df_name = 'df_history'
                    make_dataframes_exception_message(df_name, file, err)
    
    # drop any duplicate in the dataframe
    df_history = df_history.drop_duplicates()
    
    # reset indices for the dataframe
    df_history = df_history.reset_index(drop=True)
    
    # completion message
    print("History dataframe complete.")
    # ----------------- history dataframe complete ----------------->
    
    
    # ----------------- build the PLD dataframe -------------------->
    # loop over the rest of the files and append history data to the dataframe
    # only triggers when there is more than one file to read!!
    if len(FILES) > 1:
        print("Building PLD dataframe...")
        for file in tqdm(FILES[1:]):
            try:
                # load Excel file and file date
                xlsx = pd.ExcelFile(file)
                xlsx_date = capture_file_date(file)
                
                # create dataframe from file and append to history dataframe
                df_xlsx = make_pld_dataframe(xlsx, xlsx_date)
                df_pld = pd.concat([df_pld, df_xlsx])
                
            except Exception as err:
                df_name = 'df_pld'
                make_dataframes_exception_message(df_name, file, err)
                
    # drop any duplicate in the dataframe
    df_pld = df_pld.drop_duplicates()
    
    # reset indices for the dataframe
    df_pld = df_pld.reset_index(drop=True)
    
    # completion message
    print("PLD dataframe complete.")
    # ----------------- PLD dataframe complete --------------------->
    
    return df_aggregate, df_package, df_history, df_pld
# ---------------------------------------------- END BUILD DATA --------------------->




def main(args):
    # reference global variables
    global PATH
    global FILES
    global START
    global END
    
    # console space
    print('\n')
    
    # check if custom file path is given
    if len(args) > 1:
        PATH = args[1]
        
    print("Data path:", "\'" + PATH + "\'", end="\n\n")
    
    # attempt to get the data file list
    FILES = capture_filenames(PATH)
 
    # check to see if we got the data files we need
    if not FILES:
        print("No data was found. Preprocessing has ended.")
        sys.exit()
    
    
    # attempt to get the range of dates from the files
    START = capture_file_date(FILES[0])
    END = capture_file_date(FILES[len(FILES)-1])
    print("Start Date:", START)
    print("End Date:", END)

    # start preprocessing data
    while True:
        process_inst = input("\n\nReady to preprocess data. Countinue? [Y/N]: ")
        if process_inst.upper() == "Y":
            df_aggregate, df_package, df_history, df_pld = build_data()
            
            df_aggregate.to_pickle('df_aggregate.pkl')
            df_package.to_pickle('df_package.pkl')
            df_history.to_pickle('df_history.pkl')
            df_pld.to_pickle('df_pld.pkl')
            
            print("Dataframe Aggregate:\n", df_aggregate, end='\n')
            print("Dataframe Package:\n", df_package, end='\n')
            print("Dataframe History:\n", df_history, end='\n')
            print("Dataframe PLD:\n", df_pld, end='\n')
            
            sys.exit()
        elif process_inst.upper() == "N":
            print("Ending Preprocessing. Goodbye.")
            sys.exit()
        else:
            print("Invalid selection.")
    
    
	

if __name__ == "__main__":
    main(sys.argv)