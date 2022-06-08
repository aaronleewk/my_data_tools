
import pandas as pd
import numpy as np
import sqlalchemy
from configparser import ConfigParser
import psycopg2
import csv
import ast

class transformation:
    '''
    This class is targeted at dataframe manipulations, specifically multiple mergers.
    Using the 'merge_df' method, you can chain multiple merges together.
    '''

    def __init__(self):
        self.df = np.nan

    def set_df(self, df):
        self.df = df

    def get_df(self):
        return self.df

    # Renames a dataframe column
    def rename(self, old_col, new_col):
        for old_name in old_col:
            for new_name in new_col:
                self.df.rename(columns={old_name: new_name}, inplace=True)

        return self.df
    # This is to enable matching of columns where one is in a different case
    def column_to_lower(self):
        new_col = self.df['col'].str.lower()

    # Merges two dataframes into one
    def merge_df(self, df2, leftCol, rightCol, howMerge, merge_indicator=1, df1=0):

        if pd.isna(self.df) and df1 != 0:
            if merge_indicator:
                self.df = pd.merge(df1, df2, left_on=leftCol,
                                   right_on=rightCol, how=howMerge, indicator=True)
                return self.df
            else:
                self.df = pd.merge(
                    df1, df2, left_on=leftCol, right_on=rightCol, how=howMerge, indicator=False)
                return self.df
        elif pd.notna(self.df) and df1 == 0:
            if merge_indicator:
                self.df = pd.merge(self.df, df2, left_on=leftCol,
                                   right_on=rightCol, how=howMerge, indicator=True)
                return self.df
            else:
                self.df = pd.merge(self.df, df2, left_on=leftCol,
                                   right_on=rightCol, how=howMerge, indicator=False)
                return self.df
        else:
            raise ValueError(
                'Base dataframe is either already defined or is not a valid dataframe. Check if there is existing dataframe using \'get_df\' method.')

    def drop_columns(self, df, column_list):
        return

    def group_data(self, df, criteria):
        # this assumes the grouping criteria is only one dimensional
        return df.groupby([criteria])


    def replace_value(self, col_name, value_org, value_new):
        return
    
    # Only select rows where column 'x' contains specific value(s), where values is in an array []
    def select_rows_with_values(self,values,col_name)
        return self.df[self.df[col_name].isin(values)]
                       

class data_io:

    '''
    Methods for getting and outputting data
    '''

    def read_config(self, file_name):
        parser = ConfigParser()
        parser.optionxform = str  # make option names case sensitive
        found = parser.read(file_name)
        if not found:
            raise ValueError('No config file found!')

        self.__dict__.update(parser.items(parser.sections()[0]))

    def connect_db(self):

        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**self.__dict__)
            return conn.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def update_df(self):

        self.engine = sqlalchemy.create_engine('')

    def drop_db_table(self, table_name):
        sqlQuery = 'DROP TABLE public."%s"' % table_name

        result = self.engine.execute(sqlQuery)

    def add_db_table(self, df, table_name):

        df.to_sql(table_name, self.engine, index=False)


    def from_csv(self, filepath):
        self.name = filepath
        return pd.read_csv(filepath)
    
    def to_csv(self, df, filename):
        df.to_csv(filename, index=False, encoding='utf-8')


class dataset_insights:

    def all_stats(self, df):
        print('Dataset statistics for: ' + self.name)
        cols = list(df.columns)
        total = len(df)
        print('Total number of rows: ' + str(total))

        least = []
        most = []
        for each in cols:
            print('Statistics of column "' + each + '":')
            filled = df[each].notnull().sum()
            not_filled = df[each].isnull().sum()
            print('Filled - ' + str(filled))
            print('Not filled - ' + str(not_filled))
            print('Not filled percentage - ' + str("%.2f" %
                  (not_filled/total * 100)) + '%')
            most.append([each, (filled/total * 100)])

        least.sort(key=lambda x: x[1])
        most.sort(key=lambda x: x[1])

        print('The column that is least filled is "' +
              most[0][0] + '" with only ' + "{:.2f}".format(most[0][1]) + '% filled')
        print('The column that is most filled is "' +
              most[-1][0] + '" with ' + "{:.2f}".format(most[-1][1]) + '% filled')

    def get_columns(self, df):
        cols = list(df.columns)
        print('Length of columns: ' + str(len(cols)))

        for each in cols:
            print(each)

    def add_attributes(self, parameters):
        self.parameters = parameters
        for each in parameters:
            setattr(self.each, each)

    def print_attributes(self):
        for each in self.parameters:
            print(each)

    def is_all_null(self, df, column):
        return df[column].isnull().all()

    def is_all_not_null(self, df, column):
        return df[column].notnull().all()

    def unique_values(self, df, col_name):
        raw_list = df[col_name].to_list()
        return set(raw_list)

    def remove_errors(self, df, col_name, list_of_errors):
        raw_list = df[col_name].to_frame()

        def is_error(df):

            string = df.values[0]
            if string not in list_of_errors:
                return True
            else:
                return False

        error_status = raw_list.apply(is_error, axis=1)

        cleaned_series = error_status[error_status == True]

        return cleaned_series

    # For string-formatted json, e.g. "{'name':'Laura', 'surname':'Smith'}" -> {'name':'Laura', 'surname':'Smith'}
    def parseJSON(string):

        jsonData = ast.literal_eval(string)

        return jsonData

    # For string-formatted lists, e.g. "'Laura','Smith'" -> ['Laura','Smith']
    # 'Split' method does not cover edge cases where substring contains comma
    def parseJSON(string):

        new_list = list(csv.reader([string],delimiter=','))[0]

        return new_list
    
    '''
    I found the Pandas 'duplicated' API quite confusing. 
    Colloquially, I interpret 'duplicated' as meaning 'show me which values are duplicated'.
    I therefore expect to see the values which appear multiple times in the dataset.
    That list of values should be unique. So:
        [1,1,1,2,2,3,3,3,3,3,4] => [1,2,3]
    The equivalent code in Python is 
        set(list of duplicated values only)

    But the 'duplicated' API does the opposite.
    It marks ALL duplicates. So:
        [1,1,1] => [True, True, True]       (using the 'False' argument for the 'keep' parameter)
    The understanding is 'show me all duplicated values'.
    
    This understanding does not fit in with other Pandas APIs such as 'drop_duplicates'
    which discards all values except one from each set of duplicates.
    '''
    
    def only_unique_values(self,col_name):
        return self.df[~self.df.duplicated(subset=[col_name])]
 


'''

# Data analysis idea:
# Once cleaned dataset has been obtained, can figure out why some data was missing
# For example, 'email' was present in dataset 1, but not dataset 2 and dataset 3. Why was it missing?

e.g. find the date those emails appeared in dataset 1. They could have been collected AFTER the process for dataset 2 and 3 were created.


'''
