
"""
This script combines information from the following output Categories:

- Data that was filtered out of the raw input excel files for defined scenarios (N < 3, N < 20, all C < detection limit (DL), etc)
    (this file generated in script 01_data_filtering.py)
- The remaining data that passed each filter from the original raw input that was then passed through trend analysis and summary stats
    (file generated in script 02_trend_analysis_filtered_data.py)
- Information coupling constituents to their groupings (as defined in permit documents): VOCs, Primary MCLs, Notification Levels, CECs, etc...

Resulting data can be used to assess and propose an Action based on stats and counts.
Can also be used to construct Categorys as deliverables to client

V02 LONGFORM


author: rweatherl

"""

import os
import numpy as np
import pandas as pd
import glob
import itertools
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')


cwd = os.getcwd()
input_dir = os.path.join(os.path.dirname(cwd), 'generated_input')

output_dir = os.path.join(os.path.dirname(cwd), 'output')


### bring in Group and legal limit data for sorting
sorting = pd.read_csv(os.path.join(input_dir, 'class_constituent_ordering_v02.csv'), index_col='Constituent')
unitslist = pd.read_csv(os.path.join(input_dir, 'class_constituent_ordering.csv'), index_col='DB Name', encoding= 'unicode_escape')
# sorting['Constituents'] = sorting['Constituents'].str.strip()
sorting.index = sorting.index.str.strip()
sorting.index = sorting.index.rename('Constituent_sort')
#sorting['Regulatory Limit']=pd.to_numeric(sorting['Regulatory Limit'], errors='coerce')
# sorting.index = sorting.index.str.strip()

def read_filterdata():

    ##import data
    fd_raw = pd.read_excel(os.path.join(output_dir, 'allfilters_summary_longform_example.xlsx'),
                               sheet_name = ['ABP WDR', 'MFSG', 'ARC NPDES', 'ARC WDR'], index_col = 'C_wo_units')

    fd = fd_raw.copy()
    fdsort = {}
    for k in fd.keys():
        fd[k]['Site'] = fd[k]['Site'].astype('int64')
        fd[k].index = fd[k].index.rename('Constituent_sort')
        # fd[k].rename({'C_wo_units': 'Constituent_sort'}, inplace=True)
        fdsort[k] = sorting.join(fd[k], on='Constituent_sort', how='inner', lsuffix='_limit', rsuffix='_data')
        fdsort[k].dropna(subset=['Category'], inplace=True)
        # cols = fdsort[k].select_dtypes(include=[float]).columns
        # fdsort[k].loc[:,cols] = fdsort[k].loc[:,cols].applymap('{:,.2f}'.format)
        # fdsort[k].rename({'site': 'Site'}, inplace=True)

    return fd, fdsort

def read_statsdata():

    sd_raw = pd.read_excel(os.path.join(output_dir, 'filtered_data_summary_stats_longform_example.xlsx'),
                               sheet_name = ['ABP WDR', 'MFSG', 'ARC NPDES', 'ARC WDR'], index_col = 'c_nounits')

    sd = sd_raw.copy()
    sdsort = {}
    for k in sd.keys():
        sd[k].index = sd[k].index.rename('Constituent_sort')
        sd[k]['Site'] = sd[k]['Site'].astype('int64')
        sdsort[k] = sorting.join(sd[k], on='Constituent_sort', how='inner', lsuffix='_legal', rsuffix='_data')
        # sdsort[k].dropna(subset=['Category'], inplace=True)
        sdsort[k].drop(['mann-kendall stats'], axis=1, inplace=True)
        # cols = sdsort[k].select_dtypes(include=[float]).columns[:-1]   ## -1 drops p from the cut (we want p to have all sig figs)
        # sdsort[k].loc[:,cols] = sdsort[k].loc[:,cols].applymap('{:,.2f}'.format)

        # sdsort[k].sort_values(by=['Category', 'Site'], inplace=True)
        # sdsort[k].rename({'site':'Site'}, inplace = True)

    return sd, sdsort

def merge_data(fdsort, sdsort):  ##fd, sd

    #all data merged
    dfs = {}

    order = ['Permit', 'Category', 'Site', 'Constituent', 'Date Range', 'Count', 'Nr. Non-Detects', 'Detection Rate', 'DL', 'Methods',
             'Regulatory Limit', 'Regulatory Unit', 'Min', '25th %ile', 'Mean', 'Median', '75th %ile', 'Max', 'p', 's', 'trend', 'STORET', 'Notes', 'Action']

    for k in sdsort.keys():
        dfs[k] = pd.concat([sdsort[k], fdsort[k]])
        dfs[k].sort_values(by=['Category', 'Constituent', 'Site'], inplace=True)
        # dfs[k].drop('slope', axis = 1, inplace = True)
        dfs[k] = dfs[k].reindex(order, axis = 1)
        dfs[k].set_index(['Constituent', 'Site'], inplace = True)
        dfs[k]['Regulatory Limit'] = pd.to_numeric(dfs[k]['Regulatory Limit'], errors='coerce')

    return dfs


def decision_function(dfs):

    for k, v in dfs.items():

        ## add condition for when constituent is not listed in class_constituent_ordering.csv
            ## Add in notes that these are "type 0" constituents and "Action" will be "Remove"
            ## (class_constituent_ordering.csv is a living doc and subject to changes. Need to identify naturally occuring constituents
            ## and add in constituents with NLs)

        ## when detection frequency is low
        lowfreq_na = (v['Regulatory Limit'].isna()) & (v['Notes'] == 'Pending manual review') ## no reg. limit
        v.loc[lowfreq_na, 'Notes'] = 'Low detection rate + no reg. limit'
        v.loc[lowfreq_na, 'Action'] = 'Reduce Frequency'

        lowfreq_notna = (v['Regulatory Limit'].notna()) & (v['Notes'] == 'Pending manual review') & (v['Max'] < v['Regulatory Limit']) ## max < reg. limit
        v.loc[lowfreq_notna, 'Notes'] = 'Low detection rate + max < reg. limit'
        v.loc[lowfreq_notna, 'Action'] = 'Reduce Frequency'

        lowfreq_rl = (v['Regulatory Limit'].notna()) & (v['Notes'] == 'Pending manual review') & (v['Max'] > v['Regulatory Limit'])  ## max > reg. limit
        v.loc[lowfreq_rl, 'Notes'] = 'Low detection rate + max > reg. limit'
        v.loc[lowfreq_rl, 'Action'] = 'Keep Same Frequency'

        # add condition for when <4 samples are above detection limit (type 3 data)
        # when <4 samples are detected:
        #   reduce frequency when max val is < regulatory limit, or when there is no regulatory limit
        #   keep same frequency when max val is >= regulatory limit

        ## NO REGULATORY LIMIT EXISTS

        ## statistical analysis, no reguatory limit
        stats_na_inc = (v['Regulatory Limit'].isna()) & (v['trend'] == 'increasing')      ## increasing trend
        v.loc[stats_na_inc, 'Notes'] = 'Increasing trend, no reg. limit'
        v.loc[stats_na_inc, 'Action'] = 'Keep Same Frequency'

        stats_na_notrend = (v['Regulatory Limit'].isna()) & (v['trend'] == 'no trend')  ## no trend
        v.loc[stats_na_notrend, 'Notes'] = 'No trend, no reg. limit'
        v.loc[stats_na_notrend, 'Action'] = 'Reduce Frequency'

        stats_na_dec = (v['Regulatory Limit'].isna()) & (v['trend'] == 'decreasing')   ## decreasing trend
        v.loc[stats_na_dec, 'Notes'] = 'Decreasing trend, no reg. limit'
        v.loc[stats_na_dec, 'Action'] = 'Reduce Frequency'

        ## REGULATORY LIMIT EXISTS

        ## max > 50% reg limit, not outlier (DateofMax < 5 yrs ago -- 01/01/2018)
        ## ---> keep current frequency

        ## max > 50% reg limit, outlier (DateofMax > 5 yrs ago -- 01/01/2018)
        # high_max_inc =
        # high_max_notrend =
        # high_max_dec =

        ## max < 50% reg limit -- stats
        # low_max_inc =
        # low_max_notrend =
        # low_max_dec =

    return dfs

if __name__ == "__main__":

    fd, fdsort = read_filterdata()
    sd, sdsort = read_statsdata()
    dfs = merge_data(fdsort,sdsort)

    dfs_decision = decision_function(dfs)

    ##quality control
    # q = dfs['MFSG'].loc[dfs['MFSG']['Category'] == 'Acid Extractables']


    ## write to file
    # for k in dfs.keys():
        # dfs[k].to_csv(os.path.join(output_dir, f'{k}_sorted_longform_v11.csv'), float_format="%.5f")
    # dfs['ARC WDR'].to_csv(os.path.join(output_dir, f'ARC WDR_sorted_longform_v11.csv'), float_format="%.6f")

    # for i in final_df['Site'].unique():
    #     print(f'{i} length:', len(final_df.loc[final_df['Site'] == i]))


### GRAVEYARD ###


# for site in x.columns.get_level_values(0).unique():
#     # print(site)
#     # temp_df = pd.DataFrame(x[site].loc[x[site]['Action'] == 25]['Action'])          ##can drop
#     temp_df = pd.DataFrame(x[site]['Action'].value_counts(), dtype = np.int64)
#     temp_df.columns = [site]
#     # temp_df.drop(temp_df.loc[0], inplace = True)
#     # temp2 = pd.DataFrame(temp_df.count())
#     # temp2.columns = [site]
#     df = pd.concat([df, temp_df], axis = 1)
#     # df2 = pd.concat([df2, temp2], axis = 1).sum(axis = 1)
# df.drop(0, inplace = True)
#
# temp2 = []
# df2 = pd.DataFrame()
# x2 = sd['ABP WDR']
# x2.drop(x2.iloc[:,0:2], axis = 1, inplace = True)
# for site in x2.columns.get_level_values(0).unique():
#     print(x2[site].iloc[:,0])
#     temp2.append([site, x2[site].iloc[:,0].count()])
#     # df2 = pd.concat([df2, temp2], axis=1)
#     df2 = pd.DataFrame(temp2)
# df2.columns = ['Site', 'Constituent Count']
# df2.to_excel(os.path.join(output_dir, 'Stat_Constituents_To_Review_Count.xlsx'))



# df.T.to_excel(os.path.join(output_dir, 'Cumulative_Actions.xlsx'))




##### XTRAS #####


    ## originally in merge_data() function, split to read sorting into beginning of script

    # sorting = pd.read_csv(os.path.join(input_dir, 'class_constituent_p9.csv'), index_col = 1)
    # sorting['Constituents']= sorting['Constituents'].str.strip()
    # sorting.sort_values(by = 'Constituents', inplace = True)
    # dfs['ABP WDR'].index = dfs['ABP WDR'].iloc[:,0].str.split('(').str[0].str.strip()


    # dfs['ABP WDR'][0].fillna(dfs['ABP WDR']['index'], inplace=True)
    # dfs['ABP WDR'].sort_values(by = ['Category',0], inplace = True)
    # dfs['ABP WDR'].index = pd.MultiIndex.from_frame(dfs['ABP WDR'].loc[:,['Category', 0]])
    # dfs['ABP WDR'].drop(['index', 'Category', 0], axis = 1, inplace=True)
