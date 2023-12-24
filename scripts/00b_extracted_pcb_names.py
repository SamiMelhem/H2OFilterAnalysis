"""Quick script to extract names of all PCBs contained in permit db of interest """

import os
import numpy as np
import pandas as pd
import glob
import itertools
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')


# ws = 'C:\\Users\\rweatherl\\OneDrive - INTERA Inc\\_California WRD\\Amendment 5\\Permits\\12. CI-10317 ARC NPDES'
# ws = 'C:\\Users\\rweatherl\\OneDrive - INTERA Inc\\_California WRD\\Amendment 5\\Permits\\13. CI-10318 ARC WDR'
ws = 'C:\\Users\\rweatherl\\OneDrive - INTERA Inc\\_California WRD\\Amendment 5\\Permits\\11. CI-5728 MFSG WDR'
# file = 'ARC NPDES historical data.csv'
# file = 'ARC WDR historical data.csv'
file = '221223--MF historical data 2013-2022.csv'

df = pd.read_csv(os.path.join(ws, file), header = 0)
df.dropna(subset = 'CONSTITUENT STANDARDIZED', inplace=True)


pcb = df.loc[df['CONSTITUENT STANDARDIZED'].str.contains('chlorobiphenyl')]['CONSTITUENT STANDARDIZED'].unique()
pcb2 = df.loc[df['CONSTITUENT STANDARDIZED'].str.contains('PCB')]['CONSTITUENT STANDARDIZED'].unique()

c = pd.DataFrame(np.concatenate((pcb,pcb2)))
# c.to_csv(os.path.join(ws, 'extracted_pcb_names.csv'), index = False)