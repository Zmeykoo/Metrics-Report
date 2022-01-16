import re

import numpy as np
import pandas as pd


class BaseReport:
    """
    Base report class, processing CSV data

    ``data_input1_fp`` -- CSV file path
    ``filters`` -- for example:
                   [{'name': 'var3', 'op': 'eq', 'val': 'Enterprise'},
                    {'name': 'var1', 'op': 'eq', 'val': 'North America'}]

                   There could be multiple items for the same variables.
                   Those filtering criteria are applied on the dataset, used
                   for calculations. Only rows where column with specified ``name``
                   equal to ``val`` will be used.
    """
    DATE_PATTERN = r"\d{1,2}\/\d{4}"

    @classmethod
    def _digitize_digit(cls, value):
        if type(value) == str:
            return value.replace(',', '').replace('$', '').replace('%', '')
        else:
            return value

    def __init__(self, data_input1_fp, filters=None):

        self.started_dataframe = pd.read_csv(data_input1_fp, delimiter=',')

        # extract field names
        self.date_fields = [item for item in self.started_dataframe.columns if re.match(self.DATE_PATTERN, item)]

        # list of years
        self.years = set(int(date.split('/')[1]) for date in self.date_fields)

        # Preliminary clean
        # Nan to 0 transformation
        self.started_dataframe = self.started_dataframe.replace(np.nan, 0)

        # $ and spaces removal from numeric values
        self.started_dataframe = self.started_dataframe.applymap(BaseReport._digitize_digit)

        # then to integers(not work)
        self.started_dataframe[self.date_fields] = self.started_dataframe[self.date_fields].astype(float)

        # Dataframe copy
        self.transformed_dataframe = self.started_dataframe.copy()

        # Filter
        df = self.transformed_dataframe.copy()
        if filters:
            df_columns = df.columns.tolist()
            filter_columns = [i['name'] for i in filters]
            aggregated_filters = {}
            for i in filters:
                col = i['name']
                if col not in df_columns:
                    continue
                value = i['val']
                if col in aggregated_filters:
                    aggregated_filters[col].append(value)
                else:
                    aggregated_filters[col] = [value]

            df_filters = None
            for col, values in aggregated_filters.items():
                if df_filters is None:
                    df_filters = df[col].isin(values)
                else:
                    df_filters = df_filters & df[col].isin(values)
            df = df[df_filters]

        self.transformed_dataframe = df

        # Denominator
        shift = self.transformed_dataframe[self.date_fields[:-12]]
        self.denominator_df = shift.set_axis([k for k in self.date_fields[12:]], axis=1, inplace=False)

        # Gross Retention
        def gross_retention_data(val):
            """Gross Retention =  min(Data Input 1, Denominator)"""
            result = val.copy()
            mainframe = df[val.index].loc[next(myiter)]

            for a in range(len(val)):
                if val[a] > 0:
                    result[a] = min(val[a], mainframe[a])
                else:
                    result[a] = 0

            return result

        # All dateframes
        df = self.transformed_dataframe[self.date_fields]

        myiter = iter(df.index)
        self.gross_retention_df = self.denominator_df.apply(gross_retention_data, axis=1)

        # Net Retention
        def net_retention_data(val):
            """
            val = pd.Series()
            """
            result = val.copy()
            mainframe = df[val.index].loc[next(myiter)]

            for a in range(len(val)):
                if val[a] > 0:
                    result[a] = mainframe[a]
                else:
                    val[a] = 0

            return result

        myiter = iter(df.index)
        self.net_retention_df = self.denominator_df.apply(net_retention_data, axis=1)

    def root(self, data, period):
        """
        Sum of months (columns) in data input.
        Sum of quarters in data input.
        """

        # Month to month sum for columns: '1/2018' etc.
        summed_by_month = {}
        for item in data.head():#self.date_fields:
            summed_by_month.update({item: data[item].sum()})

        # Quarters sum

        summed_by_quarter = {}
        for year in self.years:
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter+1} {year}'
                q_sum = 0
                zero = '0'
                for month_of_quarter in range(1,4,1):
                    if len(str(month_of_quarter + (quarter*3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter*3) )}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in summed_by_month:
                        q_sum += summed_by_month[key]
                    else:
                        q_sum = None
                        break
                if q_sum is not None:
                    summed_by_quarter.update({q_name: q_sum})

        if period == 'month':
            #print(f'Summed_by_month: {summed_by_month}')
            return summed_by_month
        if period == 'quarter':
            #print(f'Quarter sum: {summed_by_quarter}')
            return summed_by_quarter

    def denominator(self):
        """
        Dataframe
        Denominator of Retention
        """
        return self.denominator_df

    def net_retention(self, denominator):
        """
        per quarter
        Numerator of net retention
        """
        #sum
        delta = {}
        for item in self.net_retention_df.head():
            delta.update({item: self.net_retention_df[item].sum()})

        result = {}
        for k in delta:
            if denominator[k] > 0:
                    result[k] = round(delta[k] / denominator[k], 2) * 100
            else:
                result[k] = 0

        final = {}
        years = set(int(date.split('/')[1]) for date in self.net_retention_df.columns)
        for year in years:
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter+1} {year}'
                q_sum = 0
                zero = '0'

                for month_of_quarter in range(1,4,1):
                    if len(str(month_of_quarter + (quarter*3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter*3) )}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in result.keys():
                        q_sum += result[key]
                    else:
                        q_sum = None
                        break
                if q_sum is not None:
                    final.update({q_name: q_sum})
        print(f'Net Retention:\n{final}')
        return final

    def gross_retention(self, denominator):
        """
        per quarter
        Numerator of gross retention
        """
        #sum
        delta1 = {}
        for item in self.gross_retention_df.head():
            delta1.update({item: self.gross_retention_df[item].sum()})

        result = {}
        for k in delta1:
            if denominator[k] > 0:
                result[k] = round(round(delta1[k] / denominator[k], 4) * 100, 2)
            else:
                result[k] = 0

        final = {}
        years = set(int(date.split('/')[1]) for date in self.gross_retention_df.columns)
        for year in years:
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter+1} {year}'
                q_sum = 0
                zero = '0'

                for month_of_quarter in range(1,4,1):
                    if len(str(month_of_quarter + (quarter*3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter*3) )}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in result.keys():
                        q_sum += result[key]
                    else:
                        q_sum = 'empty'
                        break
                if q_sum != 'empty':
                    final.update({q_name: q_sum})

        return final

    def delta_net_run_rate(self, delta1, delta2):
        """
        month or quarter
        in quarter /run_rate * 100
        """
        result = {}
        for k in delta1:
            if k in delta2:
                if delta2[k] > 0:
                    value = round(delta1[k] / delta2[k] * 100, 2)
                else:
                    value = 0
            else:
                value = 0
            result[k] = value

        print(result)
        return result
