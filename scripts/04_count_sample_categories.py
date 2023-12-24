# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 22:03:36 2023

@author: RWeatherl
"""

import pandas as pd
import numpy as np
import os


cwd = os.getcwd()

ws = os.path.join(os.path.dirname(cwd), 'output')


output = pd.read_csv(os.path.join(ws, 'ABP WDR_sorted_longform_v08.csv'))
filt = pd.read_csv(os.path.join(ws, 'permitconstituents.csv'))

permit = output.loc[output['Constituent'].isin(list(filt['Const']))]

#%%

## flags to use in 'Notes' col: 
    ## 1. All samples are ND  --> cat 2
    ## 2. Three or fewer samples --> cat 1
    ## 3. Pending manual review --> cat 3
    ## 4. Pending statistical analysis --> cat 4
    
sites = list(permit['Site'].unique())
for s in sites:
    t = permit.loc[permit['Site'] == s]
    print(f'{s} value counts:') 
    print(t['Notes'].value_counts())