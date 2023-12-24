from lifelines import KaplanMeierFitter
from lifelines.utils import restricted_mean_survival_time#, median_survival_times
from matplotlib import use
from matplotlib.pyplot import rcParams, subplots#, style 
from os import getcwd
from os.path import dirname, join
from pandas import DataFrame, merge, read_csv
from pymannkendall import original_test

'''
This is Part 2 of the whole script
'''

class Part2:
    # Constructors
    def __init__(self):
        use('Qt5Agg')

        # lib = style.library

        rcParams.update({'font.size': 12})

        ## SET USER INPUT AND OUTPUT DIRS
        self._permit_name = ''
        self._data_dir = join(getcwd(), 'output', f'{self._permit_name}')
        self._input_name = ''
        self._sites = {}
        self._output_name = ''

    # Setters
    def set_permit_name(self, permit_name):
        self._permit_name = permit_name
        self._data_dir = join(getcwd(), 'output', f'{self._permit_name}')

    def set_input_name(self, input_name):
        self._input_name = input_name

    def set_output_name(self, output_name):
        self._output_name = output_name

    # Getters
    def get_data_dir_name(self):
        return self._data_dir.title

    # to_string -> Main method of part 1 indiv
    def __str__(self) -> str:
        data, sites = self.read_data()
        final_stats = self.calculate_stats(sites)
        result, mann_k = self.trend_tests(sites)
        # data and result are there for debugging purposes
        final_df = merge(final_stats, mann_k, on = ['Site', 'Constituent'])
        final_df['Action'] = 'TO REVIEW'
        final_df['Notes'] = 'Pending statistical analysis'

        final_df.to_csv(join(self._data_dir, self._output_name), index = False)
        #

        ## example plot
        # fig, ax = subplots()
        # fitted[27][4].plot_cumulative_density(ax = ax, label = 'NDMA', ci_show = False)
        # ax.set_xlabel('Concentration (ng/L)')
        # ax.set_ylabel('Cumulative Probability')
        # plt.grid()

        return f'Finished Part 2, check {self._output_name} for errors'

    # Additional Methods    
    ## Read in filtered-down data and structure (output of script 02_data_filtering_plotting.py)
    def read_data(self):

        # PLEASE CHANGE THIS EVERYTIME THERE'S SOME NEW OUTPUT

        data = read_csv(join(self._data_dir, self._input_name), header=0) ## change input_data for separate cases

        # structure into dict of sites
        sites = {k: df for k, df in data.groupby('WRD_ID')}

        return data, sites

    def calculate_stats(self, sites):

        """
        3. Summary stats including censoring using the Kaplan-Meier Estimator
        Note that percentiles are reversed for the survival function, which counts from highest to lowest (decreasing increments)
        So traditional (increasing) 25th percentile = 75th percentile in survival function
        """

        print('start stats')

        kmf_dict = sites.copy()
        for k in kmf_dict.keys():
            # A new 'quan' column is created: uses 1s and 0s to flag a sample as 'detected' or 'non-detected'
            kmf_dict[k]['quan'] = 1
            kmf_dict[k].loc[kmf_dict[k]['FLAG'] == '<', 'quan'] = 0

        stats = [] 
        fitted = []
        for k in kmf_dict.keys():
            for c in kmf_dict[k]['C_Units'].unique():
                kmf = KaplanMeierFitter()
                print(f"{k} {c}")
                const = kmf_dict[k].loc[kmf_dict[k]['C_Units'] == c]
                const=const.sort_values(by=['SAMP_DATE','VALUE'], ascending=False)
                if not const['VALUE'].empty: 
                    const['MAX_DATE']=const.loc[const['VALUE'].idxmax(), 'SAMP_DATE']
                    stats.append([k, c, const['CONSTITUENT STANDARDIZED'].iloc[0],
                                const['STORET'].iloc[0],
                                const['UNITS'].unique()[0],
                                len(const),
                                const['VALUE'].min(),
                                const['VALUE'].max(),
                                const['MAX_DATE'].iloc[0],
                                len(const.loc[const['quan'] == 0]),
                                100 - (len(const.loc[const['quan'] == 0]) / len(const.index))*100,
                                const['year'].min(),
                                const['year'].max(),
                                const['METHOD'].unique(),
                                const['DL'].min(),
                                const['DL'].max()])
                                # np.where(const['DL'].min() == const['DL'].max(), const['DL'].max(), str(const['DL'].min()) + '-' + str(const['DL'].max()))])
                    fitted.append([k, c, const['CONSTITUENT STANDARDIZED'].iloc[0], kmf.fit_left_censoring(const['VALUE'], const['quan'])])
            stats_df = DataFrame(stats, columns=['Site', 'Constituent', 'c_nounits', 'STORET', 'Units', 'Count',
                                                    'Min', 'Max', 'DateOfMax', 'Nr. Non-Detects', 'Detection Rate',
                                                    'first year', 'last year', 'Methods', 'DL min', 'DL max']) ##basic info, no K-M needed
            stats_df['Date Range'] = stats_df['first year'].astype(str) + '-' + stats_df['last year'].astype(str)
            # stats_df['DL Range'] = stats_df['DL min'].astype(str) + '-' + stats_df['DL max'].astype(str)
            # fitted_df = pd.DataFrame(fitted, columns = ['permit', 'site', 'constituent', 'c_nounits', 'kmf'])


        censored_stats = []
        for i in range(len(fitted)):
            censored_stats.append([fitted[i][0], fitted[i][1], #fitted[i][2],
                                fitted[i][3].median_survival_time_,
                                restricted_mean_survival_time(fitted[i][3]),
                                fitted[i][3].percentile(0.75), fitted[i][3].percentile(0.25)])   ##note the K-M 75th %ile is counting from high to low. = 25th %ile counting from low to high (same with all %iles)
        km_df = DataFrame(censored_stats, columns=['Site', 'Constituent', 'Median', 'Mean', '25th %ile', '75th %ile'])
        print('Finished Stats')

        final_stats = merge(stats_df, km_df, on = ['Site', 'Constituent'])
        final_stats = final_stats[['Site', 'Constituent', 'c_nounits', 'STORET', 'Units', 'Date Range', 'Count', 'Methods',
                                'Nr. Non-Detects', 'Detection Rate', 'DateOfMax', 'Min', '25th %ile', 'Mean', 'Median',
                                '75th %ile', 'Max', 'DL min', 'DL max']]

        return final_stats

    def trend_tests(self, sites):

        result = []
        trend = []
        s = []
        p = []
        slope = []
        intercept = []

        print('beginning trend test')

        # Starting the trend test using Mann-Kendall-Trends
        for k in sites.keys():
            for c in sites[k]['C_Units'].unique():
                print(c)
                if type(c) == str:
                    const = sites[k].loc[sites[k]['C_Units'] == c]
                    data = const.loc[:,['SAMP_DATE','VALUE']]
                    result.append([k, c, original_test(data['VALUE'])])
                    trend.append(original_test(data['VALUE']).trend)
                    s.append(original_test(data['VALUE']).s)
                    p.append(original_test(data['VALUE']).p)
                    slope.append(original_test(data['VALUE']).slope)
                    intercept.append(original_test(data['VALUE']).intercept)
            mann_k = DataFrame(result)
            mann_k['trend'], mann_k['s'], mann_k['p'], mann_k['slope'], mann_k['intercept'] = trend, s, p, slope, intercept
            mann_k.columns = ['Site', 'Constituent', 'mann-kendall stats', 'trend', 's', 'p', 'slope', 'intercept']

        print('finished trend test')

        return result, mann_k


    ## If anyone is ever interested in digging deeper...autocorrelation test...
    def check_autocorrelation(self, permit, site, constituent):
    ## inputs: permit and constituent are string, site is an integer

        data = self._sites[permit][site].loc[self._sites[permit][site]['C_Units'] == constituent]['VALUE']
        fig, ax = subplots()
        # sm.graphics.tsa.plot_acf(data, ax = ax, lags = 40)

        return None