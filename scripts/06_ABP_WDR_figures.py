


import os
import numpy as np
import pandas as pd
import glob
import itertools
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
import csv
matplotlib.use('Qt5Agg')



lib = plt.style.library
plt.rcParams.update({'font.size': 12})


## SET USER WORKING DIRECTORY
cwd = os.getcwd()
ws = os.path.join(os.path.dirname(cwd), 'Permits')

## SET USER INPUT AND OUTPUT DIRS
input_dir = os.path.join(ws, '..', 'generated_input')
output_dir = os.path.join(ws, '..', 'output')



## Create a dictionary of permit numbers (as provided by WRD) and acronyms. This will expand as more data becomes available.
p = {9: 'ABP WDR'}

## Create a dictionary of measurement methods present in database to be removed from analysis
excluded_methods = {'Gross Alpha particle activity (excluding radon and uranium)':['SM 7110C'],
                    'Radium 226':['Ra-226 GA', 'SM7500-Ra B'],
                    'Radium 228':['RA-228 GA','SM7500-Ra D'],
                    'N-Nitrosodimethylamine (NDMA)':['EPA 625', 'EEA Agilent 521.1', 'NDMA, NDEA, NDPA', 'NDMA, NDEA, NDPA, NMOR'],
                    'N-Nitrosodi-N-Propylamine (NDPA)': ['EPA 625', 'EPA 625.1 SIM', 'EEA Agilent 521.1', 'NDMA, NDEA, NDPA', 'NDMA, NDEA, NDPA, NMOR'],
                    'N-Nitrosodiphenylamine': ['EPA 625', 'EPA 625.1 SIM', 'EEA Agilent 521.1'],
                    'N-Nitrosodiethylamine (NDEA)': ['EEA Agilent 521.1', 'NDMA, NDEA, NDPA', 'NDMA, NDEA, NDPA, NMOR'],
                    'Di(2-ethylhexyl)phthalate (DEHP)':['EPA 625'],
                    'Copper':['EPA 200.7'],
                    'Lead':['EPA 200.7'],
                    'Arsenic':['EPA 200.7'],
                    'Antimony':['EPA 200.7'],
                    'Beryllium':['EPA 200.7'],
                    'Cadmium':['EPA 200.7'],
                    'Nickel':['EPA 200.7'],
                    'Silver':['EPA 200.7'],
                    'Thallium':['EPA 200.7'],
                    '1,4-Dioxane':['EPA 8270C (M)Isotope Dil'],
                    '1,2,3-Trichloropropane':['EPA 8260'],
                    'Benzene':['EPA 624.1'],
                    'Carbon Tetrachloride':['EPA 624.1'],
                    'Hexachlorobenzene':['EPA 608.3'],
                    '1,1-Dichloroethane':['EPA 624.1'],
                    'o-Dichlorobenzene (1,2-DCB)':['EPA 624.1'],
                    '1,3-Dichlorobenzene':['EPA 624.1'],
                    'p-Dichlorobenzene':['EPA 624.1'],
                    'Chlordane':['EPA 608.3'],
                    'Endrin':['EPA 608.3'],
                    'Heptachlor':['EPA 608.3'],
                    'Heptachlor Epoxide':['EPA 608.3'],
                    'Lindane (Gamma-BHC)': ['EPA 608, EPA 608.3'],
                    'PCB 1242 Aroclor':['EPA 608.3'],
                    'PCB 1254 Aroclor':['EPA 608.3'],
                    'PCB 1221 Aroclor':['EPA 608.3'],
                    'PCB 1232 Aroclor':['EPA 608.3'],
                    'PCB 1248 Aroclor':['EPA 608.3'],
                    'PCB 1260 Aroclor':['EPA 608.3'],
                    'PCB 1016 Aroclor':['EPA 608.3'],
                    'Toxaphene':['EPA 608.3'],
                    'Methyl Tert Butyl Ether (MTBE)':['EPA 624'],
                    'Carbon Disulfide':['EPA 624'],
                    'Di(2-ethylhexyl)phthalate (DEHP)': ['EPA 625.1 SIM'],
                    'PFOS': ['PFOS']}

def read_data():

    ## 1a. Read in data and structure into dict of dfs. If year2010 = True, only data post-2010 is considered
    year2010 = True
    permit_list = os.listdir(ws)
    permit_files = []
    for permit in permit_list:
        permit_files.append(glob.glob(os.path.join(ws, permit, '*data*.csv')))
    permit_files = [x for x in permit_files if x != []]  ## remove empty lists
    permit_files = list(itertools.chain(*permit_files))  ##transforms list of lists into single list

    print('begin reading data files')
    permit_dfs = {}
    for i in permit_files:
        permit_dfs[i] = pd.read_csv(i, header=0)
    print('done reading data files')

    ## 1b. Change permit keys to acronyms listed in the "permits" dictionary (original keys are very long)
    newkeys = list(p.values())      ## New keys to be assigned to dict of data. Quadruple-check that the proper keys are called!
    dfs = dict(zip(newkeys, permit_dfs.values()))

    ## 2. pre-process/structure
    for k in dfs.keys():
        dfs[k]['UNITS'] = dfs[k]['UNITS'].str.strip()           ##remove hidden spaces (except single spaces between words)
        dfs[k]['METHOD'] = dfs[k]['METHOD'].str.strip()
        dfs[k]['SAMP_DATE'] = pd.to_datetime(dfs[k]['SAMP_DATE'])
        dfs[k]['year'] = dfs[k].loc[:, 'SAMP_DATE'].dt.year
        ## some data entry inconsistent. When value <= DL, make sure it is set to DL, and flag as < (non-detect)
        dfs[k].loc[dfs[k]['VALUE'] <= dfs[k]['DL'], ['VALUE']] = dfs[k]['DL']   ## When value < DL, make sure it is set to DL
        dfs[k].loc[dfs[k]['VALUE'] <= dfs[k]['DL'], ['FLAG']] = '<'             ## When value <= DL, flag as <
        ##create a unique ID of constituent and its units (some constituents are listed with multiple units over time)
        dfs[k]['C_Units'] = dfs[k]['CONSTITUENT STANDARDIZED'] + " " + "(" + dfs[k]['UNITS'] + ")"
        dfs[k].dropna(subset = 'CONSTITUENT STANDARDIZED', inplace = True)

    if year2010:
        dfs['ABP WDR'] = dfs['ABP WDR'].loc[dfs['ABP WDR']['year'] >= 2010].reset_index(drop=True)
    else:
        pass

    ## Drop excluded methods and duplicate entries
    for k, v in dfs.items():
        for k2, v2 in excluded_methods.items():
            temp_df = dfs[k].loc[dfs[k]['CONSTITUENT STANDARDIZED'] == k2].loc[dfs[k]['METHOD'].isin(v2)]
            dfs[k] = pd.concat([dfs[k], temp_df]).drop_duplicates(keep = False)
            ## drop duplicate values remaining AFTER methods accounted for.
            dfs[k].drop_duplicates(['WRD_ID', 'C_Units', 'SAMP_DATE'],
                                   inplace=True)

    # structure into nested dict of sites
    sites_raw = {}
    for k in dfs.keys():
        # print(k)
        sites_raw[k] = {k2: i for k2, i in dfs[k].groupby('WRD_ID')}
        for k2 in sites_raw[k]:
            # print(sites[k][k2])
            sites_raw[k][k2] = sites_raw[k][k2].loc[:,['SAMP_DATE', 'year', 'WRD_ID', 'C_Units', 'CONSTITUENT STANDARDIZED', 'STORET',
                                          'FLAG', 'VALUE', 'UNITS', 'TXT_VALUE', 'METHOD', 'DL']]


    return permit_list, permit_files, dfs, sites_raw

def plot_removals():
    ## These compounds proposed for removal
    # surfactants = foaming agents
    compounds = ['Boron (mg/L)', 'Calcium (mg/L)', 'Chloride (mg/L)', 'Copper (ug/L)', 'Iron (mg/L)',
                 'Manganese (ug/L)',
                 'Potassium (mg/L)', 'Sodium (mg/L)', 'Sulfate (mg/L)', 'Zinc (ug/L)', 'Apparent Color (ACU)',
                 'Aggressive Index (Corrosivity) (None)', 'Surfactants (mg/L)', 'Odor (TON)',
                 'Specific Conductance (umho/L)',
                 'Total Dissolved Solids (TDS) (mg/L)', 'Hardness (Total, as CaCO3) (mg/L)', 'Odor (TON)']
    ## without units in names
    compounds2 = ['Boron', 'Calcium', 'Chloride', 'Copper', 'Iron', 'Manganese',
                  'Potassium', 'Sodium', 'Sulfate', 'Zinc', 'Apparent Color',
                  'Aggressive Index (Corrosivity)', 'Foaming Agents', 'Odor', 'Specific Conductance',
                  'Total Dissolved Solids (TDS)', 'Hardness (Total, as CaCO3)', 'Odor']
    k = 'ABP WDR'

    sites = dfs[k]['WRD_ID'].unique()[:-5]
    abp = dfs[k].loc[dfs[k]['WRD_ID'].isin(sites)]
    # abp.drop(abp.loc[abp['WRD_ID'].isin(['100253', '100254'])], axis = 1, inplace = True)
    abp = abp[abp['WRD_ID'] != 100253]
    abp = abp[abp['WRD_ID'] != 100254]
    abp = abp.replace('Surfactants', 'Foaming Agents')

    # for compound in compounds2:
    for compound in dfs[k]['CONSTITUENT STANDARDIZED'].unique():
        print(compound)
        fig, ax = plt.subplots(figsize=(10, 5))
        toplot = abp.loc[abp['CONSTITUENT STANDARDIZED'] == compound]
        sns.lineplot(data=toplot, x='SAMP_DATE', y='VALUE',
                     hue=toplot['WRD_ID'].astype(str), marker='o', ax=ax)
        ax.grid(True, which='both')
        ax.grid(True, which='minor', color='grey', alpha=0.2)
        plt.minorticks_on()
        plt.xlabel('Sampling Date')
        units = toplot.UNITS.iloc[0]
        plt.ylabel(f'Concentration ({units})')
        ax.tick_params(axis='x', rotation=45)
        plt.title(f'{compound}')
        compound = str(compound).replace('/', '_')
        plt.legend(bbox_to_anchor=(1, 1), loc="upper left", title='Site')
        fig.tight_layout()
        plt.savefig(os.path.join(output_dir, 'figures', 'ABP_TM_sitesremoved', f'{k}_{compound}_TM.png'))

    return None

if __name__ == '__main__':

    permit_list, permit_files, dfs, sites_raw = read_data()


## These compounds proposed for removal
#surfactants = foaming agents
    compounds = ['Boron (mg/L)', 'Calcium (mg/L)', 'Chloride (mg/L)', 'Copper (ug/L)', 'Iron (mg/L)', 'Manganese (ug/L)',
                 'Potassium (mg/L)', 'Sodium (mg/L)', 'Sulfate (mg/L)', 'Zinc (ug/L)', 'Apparent Color (ACU)',
                 'Aggressive Index (Corrosivity) (None)', 'Surfactants (mg/L)', 'Odor (TON)', 'Specific Conductance (umho/L)',
                 'Total Dissolved Solids (TDS) (mg/L)', 'Hardness (Total, as CaCO3) (mg/L)', 'Odor (TON)']
    ## without units in names
    compounds2 = ['Boron', 'Calcium', 'Chloride', 'Copper', 'Iron', 'Manganese',
                 'Potassium', 'Sodium', 'Sulfate', 'Zinc', 'Apparent Color',
                 'Aggressive Index (Corrosivity)', 'Foaming Agents', 'Odor', 'Specific Conductance',
                 'Total Dissolved Solids (TDS)', 'Hardness (Total, as CaCO3)', 'Odor']
    k = 'ABP WDR'

    sites = dfs[k]['WRD_ID'].unique()[:-5]
    abp = dfs[k].loc[dfs[k]['WRD_ID'].isin(sites)]
    # abp.drop(abp.loc[abp['WRD_ID'].isin(['100253', '100254'])], axis = 1, inplace = True)
    abp = abp[abp['WRD_ID'] != 100253]
    abp = abp[abp['WRD_ID'] != 100254]
    abp = abp.replace('Surfactants', 'Foaming Agents')

    # for compound in compounds2:
    #     print(compound)
    for compound in dfs[k]['C_Units'].unique():
        print(compound)
        fig, ax = plt.subplots(figsize=(10,5))
        # toplot = abp.loc[abp['CONSTITUENT STANDARDIZED'] == compound]
        toplot = dfs[k].loc[dfs[k]['C_Units'] == compound]
        sns.lineplot(data=toplot, x='SAMP_DATE', y='VALUE',
                     hue=toplot['WRD_ID'].astype(str), marker='o', ax = ax)
        ax.grid(True, which = 'both')
        ax.grid(True, which = 'minor', color = 'grey', alpha=0.2)
        plt.minorticks_on()
        plt.xlabel('Sampling Date')
        units = toplot.UNITS.iloc[0]
        plt.ylabel(f'Concentration ({units})')
        ax.tick_params(axis='x', rotation=45)
        plt.title(f'{compound}')
        compound = str(compound).replace('/', '_')
        plt.legend(bbox_to_anchor=(1, 1), loc="upper left", title = 'Site')
        fig.tight_layout()
        # plt.savefig(os.path.join(output_dir, 'figures', f'{k}_{compound}_TM.png'))
