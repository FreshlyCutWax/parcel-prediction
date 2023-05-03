"""

"""

import os
import sys
import pandas as pd
import numpy as np


def main():
    # check if path for weather data exists
    path = 'weather_data/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No data directory found.")
        print("Place weather data in \'weather_data\'.")
        input("Press enter to continue...")
        sys.exit()
    
    # check if the data file exists in the directory
    path = path + 'Weather.csv'
    if not os.path.exists(path):
            print("No data found.")
            print("Place weather data in \'weather_data\'.")
            input("Press enter to continue...")
            sys.exit()
   



if __name__ == "__main__":
    main()
