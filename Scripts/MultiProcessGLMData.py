from multiprocessing import Pool, Value
import os
import pandas as pd
import numpy as np
import sys
import xarray as xr
from glmtools.io.glm import GLMDataset
import time

# Usage
# MultiProcessGLMData.py (num_processes) (name_want_for_final_file) (what_data) (folder_path) (area_of_interest (OPTIONAL))
                                                                                              # Left_Lon, Right_Lon, Top_Lat, Bot_Lat
# Example: MultiProcessGLMData.py 14 240 "./NCPython/NCPython/229" groups
#          MultiProcessGLMData.py 14 250 "./NCPython/NCPython/229" groups -100,100,90,-90

# Location to focus on
if(len(sys.argv) >= 6):
    area = [float(i) for i in sys.argv[5].split(',')]
else:
    area = [-180,180,90,-90]

#Init the file counter
def init(args):
    global counter
    counter = args

# Loads in the GLM data
# Gets the data in the area to focus on
# Creates a DataArray
def getGroups(x):
    global counter
    with counter.get_lock():
        counter.value += 1
    print(f'{(counter.value / 43.2):.2f}%',flush=True,end='\r')
    try:
        # Load glm dataset from file
        temp = GLMDataset(x).dataset
        # Create a mask that's within the area
        groups = temp[['group_energy','group_area']]
        # If there is data, return the dataframe
        if len((compute := groups.where((temp.group_lon >= area[0]) & (temp.group_lon <= area[1]) & (temp.group_lat >= area[3]) & (temp.group_lat <= area[2])))) > 0:
            groups = compute.drop(['group_parent_flash_id','lightning_wavelength','product_time','group_time_threshold','flash_time_threshold','lat_field_of_view','lon_field_of_view'])
            return groups.to_dataframe()
    except:
        print(f"Error in file:{x}")

def getFlashes(x):
    global counter
    with counter.get_lock():
        counter.value += 1
    print(f'{(counter.value / 43.2):.2f}',flush=True,end='\r')
    # Load glm dataset from file
    temp = GLMDataset(x).dataset
    # Create a mask that's within the area
    try:
        mask = np.ones(shape=temp.dims['number_of_flashes'],dtype=bool) & ((temp.flash_lon >= area[0]) & (temp.flash_lon <= area[1]) & (temp.flash_lat >= area[3]) & (temp.flash_lat <= area[2])).values
        # If there is data, return the dataframe
        if (q := len((compute := temp.flash_energy[mask]))) != 0:
            flshs = compute.to_dataframe('flash_energy').reset_index().drop(['product_time','lightning_wavelength','group_time_threshold','flash_time_threshold','lat_field_of_view','lon_field_of_view'],axis=1)
            flshs['flash_time_ms'] = pd.to_datetime(flshs['flash_time_offset_of_last_event']) - pd.to_datetime(flshs['flash_time_offset_of_first_event'])
            flshs['flash_time_ms'] = flshs['flash_time_ms'].dt.microseconds
            flshs.drop(['flash_time_offset_of_last_event'],axis=1,inplace=True)
            return flshs
    except:
        print(f"Error in file:{x}")

def getEvents(x):
    global counter
    with counter.get_lock():
        counter.value += 1
    print(f'{(counter.value / 43.2):.2f}',flush=True,end='\r')
    # Load glm dataset from file
    temp = GLMDataset(x).dataset
    # Create a mask that's within the area
    try:
        mask = np.ones(shape=temp.dims['number_of_events'],dtype=bool) & ((temp.event_lon >= area[0]) & (temp.event_lon <= area[1]) & (temp.event_lat >= area[3]) & (temp.event_lat <= area[2])).values
        # If there is data, return the dataframe
        if (q := len((compute := temp.event_energy[mask]))) != 0:
            evnts = compute.to_dataframe('event_energy').reset_index().drop(['event_parent_group_id','product_time','lightning_wavelength','group_time_threshold','flash_time_threshold','lat_field_of_view','lon_field_of_view'],axis=1)
            return evnts
    except:
        print(f"Error in file:{x}")

# Call this once at start of runtime
def start():
    num = int(sys.argv[1]) # Number of Processes
    folderNum = str(sys.argv[2]) # Which folder to run it on
    what = str(sys.argv[3]).lower() # What thing we are searching for
    baseFolder = str(sys.argv[4]) + "/" # Create path to basefolder

    # Location string
    st = f"{area[0]}_{area[1]}_{area[2]}_{area[3]}_"
    # File output string formatted
    fOut = f"./Data/{folderNum}_{st}{what}.nc"

    # Determine which function to use
    if what == 'groups':    func = getGroups
    elif what == 'flashes': func = getFlashes
    elif what == 'events':  func = getEvents


    # Make a list of all file paths
    allPaths = []
    for folder in os.listdir(baseFolder):
        for file in os.listdir(baseFolder + folder):
            allPaths.append(f'{baseFolder}{folder}/{file}')
    # Return the number of processes, which function, the paths, and the output name
    return num, func, allPaths, fOut


# Runs multiple processes on all the paths and puts all the dataframes in a list
# Combines them all into one big csv, outputs as a file
if __name__ == '__main__':
    global counter
    counter = Value('i',0)
    # Get necessary data
    num, func, allPaths, fOut = start()

    print(fOut)
    # Start time of process
    starttime = time.time()
    # Create a pool and run the function on all files in paths
    with Pool(processes=num,initializer = init,initargs=(counter,)) as p:
        output = p.map_async(func,allPaths).get()
    print(f"\nDone with multiple processes,starting combination. Took {time.time() - starttime} seconds.",flush=True)
    # Create and concatenate all of the outputs together
    big = pd.concat(output)
    out = big.to_xarray()
    out.to_netcdf(fOut)
    print(f"Took {time.time() - starttime} total seconds")
