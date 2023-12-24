"""
This script is used to combine data from multiple pulls provided by WRD for individual permits.

"""


import os
import pandas as pd
import numpy as np
import glob
import itertools
import matplotlib.pyplot as plt


permits = ['ABP WDR', 'MFB', 'ARC NPDES', 'ARC WDR', 'ABP2', 'ARC2', 'ARC3','ARC4']


##set workspace
cwd = os.getcwd()
ws = os.path.join(os.path.dirname(cwd), 'Permits')              ## raw data provided by WRD
# output_dir = os.path.join(ws, '..', 'output')
input_dir = os.path.join(ws, '..', 'generated_input')           ## raw data structured for input into filtering and stats workflow


##1a. locate raw data within individual permit sub-directories + additional WRD data pulls
permit_list = os.listdir(ws)
permit_files = []
for permit in permit_list:
    permit_files.append(glob.glob(os.path.join(ws, permit, '*data*.xlsx')))
permit_files = [x for x in permit_files if x != []]  ## remove empty lists
permit_files = list(itertools.chain(*permit_files))  ##transforms list of lists into single list

print('begin reading data files')
permit_dfs = {}
for i in permit_files:
    permit_dfs[i] = pd.read_excel(i, header=0) #, parse_dates=[[3, 4]])
print('done reading data files')
## 1b. Once all dfs are stored in the dictionary, change their keys to acronyms listed in the "permits" list.
## Make sure the data are assigned to the correct acronyms!
newkeys = permits
dfs = dict(zip(newkeys, permit_dfs.values()))


## isolate dataframes of interest, concatenate, drop duplicates, and write out to CSV
t = dfs['ARC NPDES']
t2 = dfs['ARC2']
con = pd.concat([t, t2]).drop_duplicates(keep=False)

# con.to_csv(os.path.join(input_dir, 'ABP WDR_concatenated.csv'), index = False)
