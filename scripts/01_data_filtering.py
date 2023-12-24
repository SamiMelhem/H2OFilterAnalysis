"""
This script processes databases provided by the WRD by reading in data and applying filters for statistical analysis and decision-making.
Filters set aside:
	1. Data that should continue to be monitored at current frequencies, or manually reviewed
	2. Data where monitoring frequency may be reduced (due to most or all values <DL)
Remaining data are output for further analysis w/statistical measures and trend analysis in script 03
These remaining data are also plotted for visual analysis.

User input: WRD provided water quality data
            define path to data and where to write generated input (for further processing) output

Author: rweatherl

"""

import os
import numpy as np
import pandas as pd
import glob
import itertools
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
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
p = {9: 'ABP WDR',
    11: 'MFSG',
    12: 'ARC NPDES',
    13: 'ARC WDR'
     }

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

# pd.DataFrame.from_dict([excluded_methods]).T.to_csv(os.path.join(cwd, 'excluded measurement methods.csv'))

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

##2. FILTERING
def filter_data(sites_raw):

    """
    Function to filter data by number of measurements, number above/below DL, and by DL ratio
    Takes nested dict of sites as input, returns filtered dataset and lists of each filter
    Each output contains appended information on constituent, WRD identifier, year range,
    and other relevant information
    """

    first_filter = []   # if 3 or less measurements are available ---> KEEP MONITORING
    second_filter = []   # if all remaining measurements are <DL ---> REDUCE MONITORING
    third_filter = []  # if all remaining measurements are 0.0 ---> append to second filter and REDUCE MONITORING
    fourth_filter = []  # if less than four of remaining measurements are quantified (>DL) ---> COMPARE MAX TO REG LIMIT

    sites = sites_raw.copy()

    print('start filtering')
    for k in sites.keys():
        for k2 in sites[k].keys():
            for c in sites[k][k2]['C_Units'].unique():

            ## 1. Filter site/constituent pairs with 3 or less measurements
            # --> rec sampling frequency stays the same
                temp = sites[k][k2].loc[sites[k][k2]['C_Units'] == c]
                if len(temp) <= 3:
                    first_filter.append([f'{k}', f'{k2}', f'{c}',
                                         temp['CONSTITUENT STANDARDIZED'].iloc[0],
                                         temp['STORET'].iloc[0],
                                         temp['UNITS'].unique()[0],
                                         temp['year'].min(),
                                         temp['year'].max(),
                                         temp['VALUE'].max(),
                                         temp['METHOD'].unique(),
                                         temp['DL'].min(),
                                         temp['DL'].max(),
                                         len(temp.loc[temp['FLAG'] == '<']),
                                         len(temp)])
                    sites[k][k2] = sites[k][k2].drop(temp.index)

            ## 2. For remaining data, filter site/constituent pairs whose flag values are ALL "<" for a given constituent/compound
                ##  (i.e. all < DL) --> reduce filtering
            for c in sites[k][k2]['C_Units'].unique():
                temp = sites[k][k2].loc[sites[k][k2]['C_Units'] == c]
                if ((sites[k][k2]['C_Units'] == c) & (sites[k][k2]['FLAG'] != '<')).any():
                    continue
                else:
                    second_filter.append([f'{k}', f'{k2}', f'{c}',
                                          temp['CONSTITUENT STANDARDIZED'].iloc[0],
                                          temp['STORET'].iloc[0],
                                          temp['UNITS'].unique()[0],
                                          temp['year'].min(),
                                          temp['year'].max(),
                                          temp['VALUE'].max(),  ## remember that this sometimes gives DL if the max quantified value is lower than any DL value
                                          temp['METHOD'].unique(),
                                          temp['DL'].min(),
                                          temp['DL'].max(),
                                          len(temp.loc[temp['FLAG'] == '<']),
                                          len(temp)])
                    sites[k][k2] = sites[k][k2].drop(
                        sites[k][k2].loc[(sites[k][k2]['C_Units'] == c) &
                                         (sites[k][k2]['FLAG'] == '<')].index)

            ## 3. For remaining data, filter sites where ALL detected values are 0
            ## --> reduce filtering (this often ties with step 2)
            for c in sites[k][k2]['C_Units'].unique():
                temp = sites[k][k2].loc[sites[k][k2]['C_Units'] == c]
                if len(temp) != 0 and ((temp['VALUE']).max() <= 0.0):
                    third_filter.append([f'{k}', f'{k2}', f'{c}',
                                         temp['CONSTITUENT STANDARDIZED'].iloc[0],
                                         temp['STORET'].iloc[0],
                                         temp['UNITS'].unique()[0],
                                         temp['year'].min(),
                                         temp['year'].max(),
                                         temp['VALUE'].max(),
                                         temp['METHOD'].unique(),
                                         temp['DL'].min(),
                                         temp['DL'].max(),
                                         len(temp.loc[temp['FLAG'] == '<']),
                                         len(temp)])
                    sites[k][k2] = sites[k][k2].drop(temp.index)

            ## 4. For remaining data, filter sites where a low number of data are above the DL
            # ## --> check max and reduce filtering(?)
            for c in sites[k][k2]['C_Units'].unique():
                temp = sites[k][k2].loc[sites[k][k2]['C_Units'] == c]
                if (len(temp.loc[temp['FLAG'] == '<']) != 0.0 and temp['FLAG'].isna().sum() <= 4):
                    fourth_filter.append([f'{k}', f'{k2}', f'{c}',
                                          temp['CONSTITUENT STANDARDIZED'].iloc[0],
                                          temp['STORET'].iloc[0],
                                          temp['UNITS'].unique()[0],
                                          temp['year'].min(),
                                          temp['year'].max(),
                                          temp['VALUE'].max(),
                                          temp['METHOD'].unique(),
                                          temp['DL'].min(),
                                          temp['DL'].max(),
                                          len(temp.loc[temp['FLAG'] == '<']),
                                          len(temp)])
                    sites[k][k2] = sites[k][k2].drop(temp.index)

    print('finished filtering data')

    return sites, first_filter, second_filter, third_filter, fourth_filter


def structure_filter_data(sites, first_filter, second_filter, third_filter, fourth_filter):
    ##for now some filters are empty. May fill as we obtain more data.

    columns = ['Permit', 'Site', 'Constituent', 'C_wo_units', 'STORET', 'Units',
               'min year', 'max year', 'Max', 'Methods', 'DL min', 'DL max', 'Nr. Non-Detects', 'Count']

    ## transform lists into dataframes
    firstfilter_df = pd.DataFrame(first_filter, columns=columns) ## continue monitoring
    secondfilter_df = pd.DataFrame(second_filter, columns=columns) ## reduce monitoring
    thirdfilter_df = pd.DataFrame(third_filter, columns=columns) ## reduce monitoring
    fourthfilter_df = pd.DataFrame(fourth_filter, columns=columns)  ## assess, if max < k can drop

    ## dataset remaining with all filter constituents removed
    dfs_filt = {}
    for k in sites.keys():
        dfs_filt[k] = pd.concat(sites[k].values())

    ##manipulation of constituents within individual filters for reporting
    for df in [firstfilter_df, secondfilter_df, thirdfilter_df, fourthfilter_df]:
        df['Date Range'] = df['min year'].astype(str) + '-' + df['max year'].astype(str)
        ##set DL column depending if DL min = DL max or not
        df['DL'] = np.where(df['DL min'] == df['DL max'], df['DL max'], df['DL min'].astype(str) + '-' + df['DL max'].astype(str))

    ## <=3 samples
    first_dfs = {}
    for p in firstfilter_df['Permit'].unique():
        first_dfs[p] = firstfilter_df.loc[firstfilter_df['Permit'] == p]
        first_dfs[p].drop(['min year', 'max year'], axis=1, inplace=True)
        first_dfs[p]['Detection Rate'] = 100 - (first_dfs[p]['Nr. Non-Detects']/first_dfs[p]['Count'])*100
        first_dfs[p]['Action'] = 'Keep same frequency'
        first_dfs[p]['Notes'] = 'Three or fewer samples'

    ## all <DL
    second_dfs = {}
    for p in secondfilter_df['Permit'].unique():
        second_dfs[p] = secondfilter_df.loc[secondfilter_df['Permit'] == p]
        second_dfs[p].drop(['min year', 'max year'], axis=1, inplace=True)
        second_dfs[p]['Detection Rate'] = 100 - (second_dfs[p]['Nr. Non-Detects'] / second_dfs[p]['Count']) * 100
        second_dfs[p]['Action'] = 'Reduce frequency'
        second_dfs[p]['Notes'] = 'All samples are ND'


    ## all 0 (empty for now, may fill as we receive more data)
    # third_dfs = {}
    # for p in third_filter['Permit'].unique():
    #     third_dfs[p] = fourth_filter.loc[third_filter['Permit'] == p]
    #     third_dfs[p].drop(['min year', 'max year'], axis=1, inplace=True)
    #     third_dfs[p]['Detection Rate'] = 100 - (third_dfs[p]['Nr. Non-Detects'] / third_dfs[p]['Count']) * 100
    #     third_dfs[p]['Action'] = 'Reduce frequency'
    #     third_dfs[p]['Notes'] = 'All samples are 0'

    ## <=4 above DL
    fourth_dfs = {}
    for p in fourthfilter_df['Permit'].unique():
        fourth_dfs[p] = fourthfilter_df.loc[fourthfilter_df['Permit'] == p]
        fourth_dfs[p].drop(['min year', 'max year'], axis=1, inplace=True)
        fourth_dfs[p]['Detection Rate'] = 100 - (fourth_dfs[p]['Nr. Non-Detects'] / fourth_dfs[p]['Count']) * 100
        fourth_dfs[p]['Action'] = 'TO REVIEW'
        fourth_dfs[p]['Notes'] = 'Pending manual review'

    return dfs_filt, first_dfs, second_dfs, fourth_dfs

## 3. TS plots of remaining data. Work in Progress.
def plot_timeseries(dfs_filt):

    ## define color palettes
    abp_wdr_colors = {100242:'xkcd:blue',
                100243:'xkcd:teal',
                100249:'xkcd:burnt orange',
                100250:'xkcd:dark red',
                100251:'xkcd:purple blue',
                100252:'xkcd:steel',
                # 100253:'xkcd:marigold', ## removed from sampling
                # 100254:'xkcd:swamp green', ## removed from sampling
                100257:'xkcd:darkish green',
                100258:'xkcd:sepia',
                550011:'xkcd:bright lavender',
                550013:'xkcd:bright lavender',
                550017:'xkcd:bright lavender',
                550020:'xkcd:bright lavender'}

    arc_npdes_colors = {570092:'xkcd:deep magenta',
                        570093:'xkcd:gunmetal',
                        570094:'xkcd:tree green'}

    arc_wdr_colors = {570001:'xkcd:tomato',
                    570082:'xkcd:muted blue',
                    570088:'xkcd:chestnut'}

    from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)
    print('begin plotting')
    for k in dfs_filt.keys():
    # for k in ['ARC WDR']:
        print(k)
        for compound in dfs_filt[k]['C_Units'].unique():
        # for compound in ['Chloride']:
            print(compound)
            fig, ax = plt.subplots(figsize=(10,5))
            toplot = dfs_filt[k].loc[dfs_filt[k]['C_Units'] == compound]
            # for color in ['abp_wdr_colors', 'arc_npdes_colors', 'arc_wdr_colors']:
            sns.lineplot(data=toplot, x='SAMP_DATE', y='VALUE',
                         hue=toplot['WRD_ID'].astype(str), marker='o', ax = ax)
            # ax.plot(toplot['SAMP_DATE_SAMP_TIME'], toplot['VALUE'], marker='o', label=k2)
            ax.grid(True, which = 'both')
            ax.grid(True, which = 'minor', color = 'grey', alpha=0.2)
            plt.minorticks_on()
            plt.xlabel('Sampling Date')
            units = toplot.UNITS.iloc[0]
            plt.ylabel(f'Value [ {units} ]')
            ax.tick_params(axis='x', rotation=45)
            plt.title(f'{k} - {compound}')
            compound = str(compound).replace('/', '_')
            plt.legend(bbox_to_anchor=(1, 1), loc="upper left", title = 'Site')
            fig.tight_layout()
            # plt.show()
            # plt.savefig(os.path.join(output_dir, 'figures', f'{k}_{compound}_V02.png'))
            # plt.close()
    # print('plotting finished')

    return None

if __name__ == "__main__":

    permit_list, permit_files, dfs, sites_raw = read_data()

    sites, first_filter, second_filter, third_filter, fourth_filter = filter_data(sites_raw)

    dfs_filt, first_dfs, second_dfs, fourth_dfs = structure_filter_data(sites, first_filter, second_filter, third_filter, fourth_filter)

    ## write out filtered dataset for statistical analysis
    # writer = pd.ExcelWriter(os.path.join(input_dir, 'data_filtered_example.xlsx'))
    # for k, v in dfs_filt.items():
    #     v.to_excel(writer, sheet_name=f'{k}', index = False)
    # writer.close()

   ## concatenate all data filtered out and write out to CSV
    # t = {}
    # for k in dfs.keys():
    #     t[k] = pd.concat([first_dfs[k], second_dfs[k], fourth_dfs[k]], join = 'outer')
    #     t[k].sort_index(inplace=True)
        # t[k] = t[k].reindex(order, axis=1, level=1)
        # t[k] = t[k].groupby(t[k].index).sum()
#
    # writer = pd.ExcelWriter(os.path.join(output_dir, 'allfilters_summary_longform_example.xlsx'))
    # for k, v in t.items():
    #     v.to_excel(writer, sheet_name=f'{k}', index = False)
    # writer.close()


## Individual plots for spot checking
# abp = dfs['ABP WDR'].loc[dfs['ABP WDR']['CONSTITUENT STANDARDIZED'] == 'Radium 226']
# # abp = dfs['ABP WDR'].loc['Radium 226']
# for id in abp['WRD_ID'].unique():
#     fig, ax = plt.subplots()
#     ax.scatter(abp.loc[abp['WRD_ID'] == id]['SAMP_DATE_SAMP_TIME'], abp.loc[abp['WRD_ID'] == id]['VALUE'])
#     ax.grid(True)
#     ax.set_title(f'{id} - Radium 226')
#     plt.savefig(os.path.join(output_dir, 'figures', 'Radium226ex', f'{id}.png'))