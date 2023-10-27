import re
import numpy as np
import pandas as pd

class BaseReport:
    DATE_PATTERN = r"\d{1,2}\/\d{4}"

    def _digitize_digit(self, value):
        if isinstance(value, str):
            return value.replace(',', '').replace('$', '').replace('%', '')
        return value

    def __init__(self, data_input1_fp, filters=None, date_filters=None):
        self.started_dataframe = pd.read_csv(data_input1_fp, delimiter=',')

        # Extract date fields based on the provided pattern
        date_fields = [col for col in self.started_dataframe.columns if re.match(self.DATE_PATTERN, col)]

        if date_filters:
            # Filter date fields based on date_filters
            date_fields = [col for col in date_fields if col in date_filters]

        self.date_fields = date_fields
        self.years = {int(date.split('/')[1]) for date in self.date_fields}

        # Preliminary data cleaning
        self.started_dataframe.fillna(0, inplace=True)
        self.started_dataframe = self.started_dataframe.applymap(self._digitize_digit)
        self.started_dataframe[self.date_fields] = self.started_dataframe[self.date_fields].astype(float)

        # Create a copy of the original dataframe
        self.transformed_dataframe = self.started_dataframe.copy()

        if filters:
            self.apply_filters(filters)

    def apply_filters(self, filters):
        df = self.transformed_dataframe.copy()
        df_columns = df.columns.tolist()
        aggregated_filters = {}

        for filter_item in filters:
            col = filter_item['name']
            if col in df_columns:
                value = filter_item['val']
                aggregated_filters.setdefault(col, []).append(value)

        df_filters = None
        for col, values in aggregated_filters.items():
            if df_filters is None:
                df_filters = df[col].isin(values)
            else:
                df_filters &= df[col].isin(values)

        df = df[df_filters]
        self.transformed_dataframe = df

    def calculate_gross_retention(self):
        # Gross Retention
        result = self.transformed_dataframe[self.date_fields].copy()

        for i in range(1, len(result)):
            mainframe = result.iloc[i - 1]
            for j in range(len(result.columns)):
                result.iat[i, j] = min(result.iat[i, j], mainframe[j]) if result.iat[i, j] > 0 else 0

        return result

    def calculate_net_retention(self):
        # Net Retention
        result = self.transformed_dataframe[self.date_fields].copy()

        for i in range(1, len(result)):
            mainframe = result.iloc[i - 1]
            for j in range(len(result.columns)):
                result.iat[i, j] = mainframe[j] if result.iat[i, j] > 0 else 0

        return result

    def root(self, data, period):
        if period == 'month':
            return data.sum()
        elif period == 'quarter':
            result = {}
            for year in self.years:
                for quarter in range(4):
                    q_name = f'Q{quarter + 1} {year}'
                    quarter_data = data.filter(like=f'Q{quarter + 1} {year}', axis=1)
                    result[q_name] = quarter_data.values.sum()
            return result

    def denominator(self):
        # Denominator
        return self.transformed_dataframe[self.date_fields[:-12]]

    def net_retention(self, denominator):
        # Numerator of net retention per quarter
        delta = self.transformed_dataframe[self.date_fields].sum()
        result = {}
        for k, v in delta.items():
            if denominator[k] > 0:
                result[k] = round(v / denominator[k] * 100, 2)
            else:
                result[k] = 0

        final = {}
        for year in self.years:
            for quarter in range(4):
                q_name = f'Q{quarter + 1} {year}'
                quarter_data = data.filter(like=q_name, axis=1)
                final[q_name] = quarter_data.values.sum()
            return final

    def gross_retention(self, denominator):
        # Numerator of gross retention per quarter
        delta = self.calculate_gross_retention().sum()
        result = {}
        for k, v in delta.items():
            if denominator[k] > 0:
                result[k] = round(v / denominator[k] * 100, 2)
            else:
                result[k] = 0

        final = {}
        for year in self.years:
            for quarter in range(4):
                q_name = f'Q{quarter + 1} {year}'
                quarter_data = data.filter(like=q_name, axis=1)
                final[q_name] = quarter_data.values.sum()
            return final

    def delta_net_run_rate(self, delta1, delta2):
        result = {}
        for k, v1 in delta1.items():
            v2 = delta2.get(k, 0)
            if v2 > 0:
                result[k] = round(v1 / v2 * 100, 2)
            else:
                result[k] = 0

        return result
