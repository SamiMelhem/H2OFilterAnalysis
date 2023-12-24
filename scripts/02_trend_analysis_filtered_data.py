'''

This script reads in filtered datasets for trend analysis

author: RWeatherl
'''

import os
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.utils import restricted_mean_survival_time, median_survival_times
import pymannkendall as mk
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')


lib = plt.style.library
plt.rcParams.update({'font.size': 12})

cwd = os.getcwd()
input_dir = os.path.join(os.path.dirname(cwd), 'generated_input')
output_dir = os.path.join(os.path.dirname(cwd), 'output')


## Read in filtered-down data and structure (output of script 02_data_filtering_plotting.py)
def read_data():

    data_dict = pd.read_excel(os.path.join(input_dir, 'data_filtered_example.xlsx'), sheet_name=['ABP WDR',
                                                                                             'MFSG',
                                                                                             'ARC NPDES',
                                                                                             'ARC WDR'])

    # structure into nested dict of sites
    sites = {}
    for k in data_dict.keys():
        print(k)
        # data_dict[k].drop('Unnamed: 0', axis = 1,inplace = True)
        sites[k] = {k2: i for k2, i in data_dict[k].groupby('WRD_ID')}


    return data_dict, sites

def calculate_stats(sites):

    """
    3. Summary stats including censoring using the Kaplan-Meier Estimator
    Note that percentiles are reversed for the survival function, which counts from highest to lowest (decreasing increments)
    So traditional (increasing) 25th percentile = 75th percentile in survival function
    """

    print('start Stats')

    kmf_dict = sites.copy()
    for k in kmf_dict.keys():
        for k2 in kmf_dict[k].keys():
            #A new 'quan' column is created: uses 1s and 0s to flag a sample as 'detected' or 'non-detected'
            kmf_dict[k][k2]['quan'] = 1
            kmf_dict[k][k2].loc[kmf_dict[k][k2]['FLAG'] == '<', 'quan'] = 0

    stats = []
    fitted = []
    for k in kmf_dict.keys():
        for k2 in kmf_dict[k].keys():
            for c in kmf_dict[k][k2]['C_Units'].unique():
                kmf = KaplanMeierFitter()
                print(k)
                print(k2)
                print(c)
                const = kmf_dict[k][k2].loc[kmf_dict[k][k2]['C_Units'] == c]
                stats.append([k, k2, c, const['CONSTITUENT STANDARDIZED'].iloc[0],
                              const['SAMP_DATE'].max(),
                              const['STORET'].iloc[0],
                              const['UNITS'].unique()[0],
                              len(const),
                              const['VALUE'].min(),
                              const['VALUE'].max(),
                              len(const.loc[const['quan'] == 0]),
                              100 - (len(const.loc[const['quan'] == 0]) / len(const.index))*100,
                              const['year'].min(),
                              const['year'].max(),
                              const['METHOD'].unique(),
                              # const['DL'].min(),
                              # const['DL'].max(), #])
                              np.where(const['DL'].min() == const['DL'].max(), const['DL'].max(), str(const['DL'].min()) + '-' + str(const['DL'].max()))])
                fitted.append([k, k2, c, const['CONSTITUENT STANDARDIZED'].iloc[0], kmf.fit_left_censoring(const['VALUE'], const['quan'])])
            stats_df = pd.DataFrame(stats, columns=['Permit', 'Site', 'Constituent', 'c_nounits', 'Max Date', 'STORET', 'Units', 'Count',
                                                    'Min', 'Max', 'Nr. Non-Detects', 'Detection Rate',
                                                    'first year', 'last year', 'Methods', 'DL']) ##basic info, no K-M needed
            stats_df['Date Range'] = stats_df['first year'].astype(str) + '-' + stats_df['last year'].astype(str)
            # stats_df['DL Range'] = stats_df['DL min'].astype(str) + '-' + stats_df['DL max'].astype(str)
            # fitted_df = pd.DataFrame(fitted, columns = ['permit', 'site', 'constituent', 'c_nounits', 'kmf'])


    censored_stats = []
    for i in range(len(fitted)):
        censored_stats.append([fitted[i][0], fitted[i][1], fitted[i][2],
                               fitted[i][4].median_survival_time_,
                               restricted_mean_survival_time(fitted[i][4]),
                               fitted[i][4].percentile(0.75), fitted[i][4].percentile(0.25)])   ##note the K-M 75th %ile is counting from high to low. = 25th %ile counting from low to high (same with all %iles)
    km_df = pd.DataFrame(censored_stats, columns=['Permit', 'Site', 'Constituent', 'Median', 'Mean', '25th %ile', '75th %ile'])
    print('Finished Stats')

    final_stats = pd.merge(stats_df, km_df, on = ['Permit', 'Site', 'Constituent'])
    final_stats = final_stats[['Permit', 'Site', 'Constituent', 'c_nounits', 'STORET', 'Units', 'Date Range', 'Count', 'DL', 'Methods',
                               'Nr. Non-Detects', 'Detection Rate', 'Min', '25th %ile', 'Mean', 'Median', '75th %ile', 'Max']]

    return final_stats

def trend_tests(sites):

    result = []
    trend = []
    s = []
    p = []
    slope = []
    intercept = []

    print('beginning trend test')

    for k in sites.keys():
        for k2 in sites[k].keys():
            print(k2)
            for c in sites[k][k2]['C_Units'].unique():
                print(c)
                const = sites[k][k2].loc[sites[k][k2]['C_Units'] == c]
                data = const.loc[:,['SAMP_DATE','VALUE']]
                result.append([k, k2, c, mk.original_test(data['VALUE'])])
                trend.append(mk.original_test(data['VALUE']).trend)
                s.append(mk.original_test(data['VALUE']).s)
                p.append(mk.original_test(data['VALUE']).p)
                slope.append(mk.original_test(data['VALUE']).slope)
                intercept.append(mk.original_test(data['VALUE']).intercept)
            mann_k = pd.DataFrame(result)
            mann_k['trend'], mann_k['s'], mann_k['p'], mann_k['slope'], mann_k['intercept'] = trend, s, p, slope, intercept
            mann_k.columns = ['Permit', 'Site', 'Constituent', 'mann-kendall stats', 'trend', 's', 'p', 'slope', 'intercept']

    print('finished trend test')

    return result, mann_k


## If anyone is ever interested in digging deeper...autocorrelation test...
def check_autocorrelation(permit, site, constituent):
## inputs: permit and constituent are string, site is an integer

    data = sites[permit][site].loc[sites[permit][site]['C_Units'] == constituent]['VALUE']
    fig, ax = plt.subplots()
    sm.graphics.tsa.plot_acf(data, ax = ax, lags = 40)

    return None

if __name__ == '__main__':

    data_dict, sites = read_data()
    final_stats = calculate_stats(sites)
    # result, mann_k = trend_tests(sites)
    # #
    # final_df = pd.merge(final_stats, mann_k, on = ['Permit', 'Site', 'Constituent'])
    # final_df['Action'] = 'TO REVIEW'
    # final_df['Notes'] = 'Pending statistical analysis'
    # #
    # #
    # final_dict = {}
    # for p in final_df['Permit'].unique():
    #     final_dict[p] = final_df.loc[final_df['Permit'] == p]
    # #
    # with pd.ExcelWriter(os.path.join(output_dir, f'filtered_data_summary_stats_longform_example.xlsx')) as writer:
    #     for k, v in final_dict.items():
    #         v.to_excel(writer, sheet_name = f'{k}', index = False)
    # writer.close()


    # example plot
    fig, ax = plt.subplots()
    fitted[17][4].plot_cumulative_density(ax = ax, label = 'Fluoride', ci_show = False)
    ax.set_xlabel('Concentration (mg/L)')
    ax.set_ylabel('Cumulative Probability')
    plt.grid()



    ##GRAVEYARD/RANDOM SNIPPETS#
    # t = {}
    # t2 = {}
    # for p in final_stats['Permit'].unique():
    #     t[p] = final_stats.loc[final_stats['Permit'] == p]
    #     t[p].drop('Permit', axis = 1, inplace=True)
    #     # t[p].set_index('Constituent',  drop = True, inplace = True)
    #     t2[p] = t[p].pivot(index = ['Constituent', 'c_nounits'], columns = 'site',
    #                        values = t[p].columns[3:]).swaplevel(axis = 1).sort_index(axis=1, level=0)
    #     t2[p].columns = t2[p].columns.to_flat_index()
    #
    # m = {}
    # m2 = {}
    # for p in final_df['permit'].unique():
    #     m[p] = final_df.loc[final_df['permit'] == p]
    #     m[p].drop('permit', axis = 1, inplace=True)
    #     m2[p] = m[p].pivot(index = ['Constituent', 'c_nounits'], columns = 'site',
    #                        values = m[p].columns[2:]).swaplevel(axis = 1).sort_index(axis=1, level=0)
    #     m2[p].columns.names = ['Site', 'Stats']
        # m2[p].columns = m2[p].columns.to_flat_index()
        # m2[p] = m2[p].T
        # m2[p].rename(columns={'site': 'Site', 'level_1': 'Stats'}, inplace=True)

    # with pd.ExcelWriter(os.path.join(output_dir, f'filtered_data_summary_stats_v02.xlsx')) as writer:
    #     for k, v in m2.items():
    #         v.to_excel(writer, sheet_name = f'{k}')
    # writer.close()


    # #write out to individual excel files
    # for i in m2.keys():
    #     m2[i].columns.get_level_values(1)
    #     m2[i].to_excel(os.path.join(output_dir, "Permit Results", f'Permit_{i}_TrendTests_Wide.xlsx'))