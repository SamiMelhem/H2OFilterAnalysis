from matplotlib import use 
from matplotlib.pyplot import rcParams, style
from os import getcwd
from os.path import dirname, join
from pandas import concat, DataFrame, read_csv, to_datetime

'''
This is Part 1 of the whole script
'''

class Part1:
    # Constructors
    def __init__(self):
        use('Qt5Agg')

        self._lib = style.library
        rcParams.update({'font.size': 12})


        ## SET USER WORKING DIRECTORY
        self._cwd = getcwd() # the current working dirctory

        ## SET USER INPUT AND OUTPUT DIRS
        self._permit = ''  ## '11. CI-5728 MFSG WDR'  ## 13. CI-10318 ARC WDR
        self._permit_data = '' # 'ARC WDR historical data.csv' ## '221223--MF historical data 2013-2022.csv'  ## ARC WDR historical data.csv
        self._permit_name = ''   #'MFSG'  ## ARC WDR
        self._data_dir = '' # join(dirname(self._cwd), 'WRD_sampling-reduction\\Permits', self._permit, self._permit_data) # Permits -> 13. Cl-10318 ARC WDR -> ARC WDR historical data.csv
        self._all_filters_name = ''
        self._output_dir = '' # join(dirname(self._cwd), 'WRD_sampling-reduction', 'output', self._permit_name)
        self._output_name = ''
        self._excluded_methods = {'EEA Agilent 521.1': ['N-Nitrosodiphenylamine', 'N-Nitrosodiethylamine (NDEA)', 'N-Nitrosodi-N-Propylamine (NDPA)', 'N-Nitrosodimethylamine (NDMA)'],
                    'EPA 200.7': ['Copper', 'Lead', 'Arsenic', 'Antimony', 'Beryllium', 'Cadmium', 'Nickel', 'Silver', 'Thallium'],
                    'EPA 608': ['Lindane (Gamma-BHC)'],
                    'EPA 608.3': ['Chlordane', 'Endrin', 'Heptachlor', 'Heptaclor Epoxide', 'Hexachlorobenzene', 'Lindane (Gamma-BHC)', 'PCB 1016 Aroclor', 
                                  'PCB 1221 Aroclor', 'PCB 1232 Aroclor', 'PCB 1242 Aroclor', 'PCB 1248 Aroclor' , 'PCB 1254 Aroclor', 
                                  'PCB 1260 Aroclor', 'Toxaphene'],
                    'EPA 624': ['Methyl Tert Butyl Ether (MTBE)', 'Carbon Disulfide'],
                    'EPA 624.1': ['Benzene', 'Carbon Tetrachloride', '1,1-Dichloroethane', 'o-Dichlorobenzene (1,2-DCB)', '1,3-Dichlorobenzene', 'p-Dichlorobenzene'],
                    'EPA 625': ['Di(2-ethylhexyl)phthalate (DEHP)', 'N-Nitrosodiphenylamine', 'N-Nitrosodi-N-Propylamine (NDPA)', 'N-Nitrosodimethylamine (NDMA)'],
                    'EPA 625.1 SIM': ['Di(2-ethylhexyl)phthalate (DEHP)', 'N-Nitrosodiphenylamine', 'N-Nitrosodi-N-Propylamine (NDPA)'],
                    'EPA 8260': ['1,2,3-Trichloropropane'],
                    'EPA 8270C (M)Isotope Dil': ['1,4-Dioxane'],
                    'NDMA, NDEA, NDPA': ['N-Nitrosodiethylamine (NDEA)', 'N-Nitrosodi-N-Propylamine (NDPA)', 'N-Nitrosodimethylamine (NDMA)'],
                    'NDMA, NDEA, NDPA, NMOR': ['N-Nitrosodiethylamine (NDEA)', 'N-Nitrosodi-N-Propylamine (NDPA)', 'N-Nitrosodimethylamine (NDMA)'],
                    'PFOS': ['PFOS'],
                    'Ra-226 GA': ['Radium 226'],
                    'RA-228 GA': ['Radium 228'],
                    'SM 7110C': ['Gross Alpha particle activity (excluding radon and uranium)'],
                    'SM7500-Ra B':['Radium 226'],
                    'SM7500-Ra D':['Radium 228']}

    # Setters
    def set_file_name(self, new_file_name):
        self._output_name = new_file_name
    
    def set_permit(self, new_permit):
        self._permit = new_permit
        self.set_data_dir()

    def set_permit_data(self, new_permit_data):
        self._permit_data = new_permit_data
        self.set_data_dir()

    def set_permit_name(self, new_permit_name):
        self._permit_name = new_permit_name
        self._output_dir = join(self._cwd, 'output', self._permit_name)

    def set_all_filters_name(self, new_all_filters_file_name):
        self._all_filters_name = new_all_filters_file_name


    def set_data_dir(self):
        self._data_dir = join(self._cwd, 'Permits', self._permit, self._permit_data)

    # Getters
    def get_data_directory(self):
        return self._data_dir
    
    def get_output_directory(self):
        return self._output_dir

    # to_string -> Main method of part 1 indiv
    def __str__(self) -> str:
        # Execute the whole script
        sites_raw = self.read_data(self._data_dir) # what is data for?, it looks like it is not being used

        sites, first_filter, second_filter, third_filter, fourth_filter = self.filter_data(sites_raw) # Plug in the reading of the data in this method, might just be fine for readability purposes

        df_filt, first_df, second_df, third_df, fourth_df = self.structure_filter_data(sites, first_filter, second_filter, third_filter, fourth_filter)

        # write out filtered dataset for statistical analysis
        df_filt.to_csv(join(self._output_dir, self._output_name), index=False)

        ## concatenate all data filtered out and write out to CSV
        allfilters = concat([first_df, second_df, third_df, fourth_df], join = 'outer')
        allfilters.set_index('Site', inplace = True)
        allfilters.sort_index(inplace=True)

        allfilters.to_csv(join(self._output_dir, self._all_filters_name))

        return f'Finished Part 1, check {self._output_name} for errors'

    # Additional Methods    
    def read_data(self, data_dir):

        data = read_csv(data_dir, header=0) # Reads the data in and the to_string is the table like in excel but prints in Python

        ## 2. pre-process/structure
        data['UNITS'] = data['UNITS'].str.strip()           ## remove hidden spaces (except single spaces between words)
        data['METHOD'] = data['METHOD'].str.strip()
        data['SAMP_DATE'] = to_datetime(data['SAMP_DATE'])
        data['year'] = data.loc[:, 'SAMP_DATE'].dt.year
        ## some data entry inconsistent. When value <= DL, make sure it is set to DL, and flag as < (non-detect)
        ## TO DO: consider changing this to accomodate values == 0 # Looks like it works
        data.loc[data['VALUE'] <= data['DL'], ['VALUE']] = data['DL']   ## When value < DL, make sure it is set to DL
        data.loc[data['VALUE'] <= data['DL'], ['FLAG']] = '<'             ## When value <= DL, flag as <

        ## create a unique ID of constituent and its units (some constituents are listed with multiple units over time)
        data['C_Units'] = data['CONSTITUENT STANDARDIZED'] + " " + "(" + data['UNITS'] + ")"
        data.dropna(subset = 'CONSTITUENT STANDARDIZED', inplace = True) # drops null columns
            
        ## Drop excluded methods and dupliacte entries (for a flipped excluded_methods)
        for k, v in self._excluded_methods.items():
            temp_df = data.loc[data['CONSTITUENT STANDARDIZED'] == k].loc[data['METHOD'].isin(v)] # Changed by Sami due to the flip of excluded_methods
            data = concat([data, temp_df]).drop_duplicates(keep = False)
            ## drop duplicate values remaining AFTER methods accounted for.
            data.drop_duplicates(['WRD_ID', 'C_Units', 'SAMP_DATE', 'SAMP_TIME'], inplace=True)

        ## create flag to select sites -- flag True/False as needed. Can change site/year as needed.
        select = False    
        if select:
            site = 570088
            year = 2020
            data = data.loc[data['WRD_ID'] == site]
            data = data.loc[data['year'] == year]

        # structure into dict of sites
        sites_raw = {k: df for k, df in data.groupby('WRD_ID')}
        for k, df in sites_raw.items():
            sites_raw[k] = df.loc[:,['SAMP_DATE', 'year', 'WRD_ID', 'C_Units', 'CONSTITUENT STANDARDIZED', 'STORET',
                                        'FLAG', 'VALUE', 'UNITS', 'TXT_VALUE', 'METHOD', 'DL']]
        # sites_raw.to_csv(join(self._output_dir, self._output_name), index=False)
        # Possibly create a way to check this method further

        return sites_raw
    
    def filter_data(self, sites_raw):

        """
        Function to filter data by number of measurements, number above/below DL, and by DL ratio
        Takes nested dict of sites as input, returns filtered dataset and lists of each filter
        Each output contains appended information on constituent, WRD identifier, year range,
        and other relevant information
        """

        first_filter = []   # if 3 or less measurements are available ---> KEEP MONITORING
        second_filter = []   # if all remaining measurements are <DL ---> REDUCE MONITORING
        third_filter = []  # if all remaining measurements are 0.0 ---> append to second filter and REDUCE MONITORING
        fourth_filter = []  # if less than four of remaining measurements are quantified (>DL) ---> ASSESS VIA COUNT/MAX VALUE

        sites = sites_raw.copy()

        print('start filtering')

        for k, df in sites.items():
            for c in sites[k]['C_Units'].unique():
                temp = sites[k].loc[sites[k]['C_Units'] == c]
                temp = temp.sort_values(by=['SAMP_DATE','VALUE'], ascending=False) 
                if not temp['VALUE'].empty:
                    temp['DateOfMax'] = temp.loc[temp['VALUE'].idxmax(), 'SAMP_DATE']
                    appending = [f'{k}', f'{c}', temp['CONSTITUENT STANDARDIZED'].iloc[0], temp['STORET'].iloc[0], temp['UNITS'].unique()[0],
                                temp['year'].min(), temp['year'].max(), temp['VALUE'].max(), temp['DateOfMax'].iloc[0], temp['METHOD'].unique(),
                                temp['DL'].min(), temp['DL'].max(), len(temp.loc[temp['FLAG'] == '<']), len(temp)] 
                ## remember that temp['VALUE'].max() sometimes gives DL if the max quantified value is lower than any DL value

                ## 1. Filter site/constituent pairs with 3 or less measurements
                # --> rec sampling frequency stays the same
                if len(temp) <= 3:
                    first_filter.append(appending)
                    sites[k] = sites[k].drop(temp.index)
                
                ## 2. For remaining data, filter site/constituent pairs whose flag values are ALL "<" for a given constituent/compound
                ##  (i.e. all < DL) --> reduce filtering
                elif not any((sites[k]['C_Units'] == c) & (sites[k]['FLAG'] != '<')):
                    second_filter.append(appending)
                    sites[k] = sites[k].drop(sites[k].loc[(sites[k]['C_Units'] == c) & (sites[k]['FLAG'] == '<')].index)

                ## 3. For remaining data, filter sites where ALL detected values are 0
                ## --> reduce filtering (this often ties with step 2)
                elif len(temp) != 0 and ((temp['VALUE']).max() == 0.0):
                    third_filter.append(appending)
                    sites[k] = sites[k].drop(temp.index)

                ## 4. For remaining data, filter sites where a low number of data are above the DL
                # ## --> check max and reduce filtering(?)
                elif (len(temp.loc[temp['FLAG'] == '<']) != 0.0 and temp['FLAG'].isna().sum() <= 4):
                    fourth_filter.append(appending)
                    sites[k] = sites[k].drop(temp.index)

        print('finished filtering data')

        return sites, first_filter, second_filter, third_filter, fourth_filter

    def structure_filter_data(self, sites, first_filter, second_filter, third_filter, fourth_filter) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:  # sites,
        ## for now some filters are empty. May fill as we obtain more data.

        columns = ['Site', 'Constituent', 'C_wo_units', 'STORET', 'Units', 'min year', 'max year',
                'Max', 'DateOfMax', 'Methods', 'DL min', 'DL max', 'Nr. Non-Detects', 'Count']

        ## transform lists into dataframes
        firstfilter_df = DataFrame(first_filter, columns=columns)    ## continue monitoring
        secondfilter_df = DataFrame(second_filter, columns=columns)  ## reduce monitoring
        thirdfilter_df = DataFrame(third_filter, columns=columns)    ## reduce monitoring
        fourthfilter_df = DataFrame(fourth_filter, columns=columns)  ## assess, if max < k can drop

        ## dataset remaining with all filter constituents removed
        # for k,v in sites.items():
        #    print(v)
        df_filt = concat([v for k, v in sites.items()])

        ## manipulation of constituents within individual filters for reporting
        for df in [firstfilter_df, secondfilter_df, thirdfilter_df, fourthfilter_df]:
            df['Date Range'] = df['min year'].astype(str) + '-' + df['max year'].astype(str)
            df.drop(['min year', 'max year'], axis=1, inplace=True)
            df['Detection Rate'] = 100 - (df['Nr. Non-Detects'] / df['Count']) * 100
            # df['Methods'] = df['Methods'].str.replace(["['", "' '","']"], ["", ", ", ""])
            # df['Methods'] = df['Methods'].replace(["['", "' '", "']"], ["", ", ", ""])
            ##set DL column depending if DL min = DL max or not
            # df['DL'] = np.where(df['DL min'] == df['DL max'], df['DL max'], df['DL min'].astype(str) + '-' + df['DL max'].astype(str))

        ## <=3 samples
        firstfilter_df['Action'] = 'Keep same frequency'
        firstfilter_df['Notes'] = 'Three or fewer samples'

        ## all <DL
        secondfilter_df['Action'] = 'Reduce frequency'
        secondfilter_df['Notes'] = 'All samples are ND'

        ## all 0 (empty for now, may fill as we receive more data)
        thirdfilter_df['Action'] = 'Reduce frequency'
        thirdfilter_df['Notes'] = 'All samples are 0'

        ## <=4 above DL
        fourthfilter_df['Action'] = 'TO REVIEW'
        fourthfilter_df['Notes'] = 'Less than 4 values > DL'

        return df_filt, firstfilter_df, secondfilter_df, thirdfilter_df, fourthfilter_df
