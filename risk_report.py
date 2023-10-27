import sys
import os
import json
import pandas as pd
from models import RiskScoreRules
from metrics_data.base_report import BaseReport

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def take_closest(num, keys):
    """Given a list of integers, returns the number closest to 'num'."""
    return min(keys, key=lambda x: abs(x - num))

class RiskReport(BaseReport):
    def __init__(self, filename):
        super().__init__(filename)
    
    def get_score(self, kind, vv, hv):
        """Score lookup method for 2D tables."""
        obj = RiskScoreRules.query.filter_by(kind=kind).one()
        df = pd.read_json(obj.json)
        rows = self._parse_score_table(df)

        closest_row = take_closest(vv, [i[0] for i in rows if i[0] is not None])
        closest_column = take_closest(hv, rows[0][1:])
        col_idx = rows[0].index(closest_column)
        result = None
        for row in rows[1:]:
            if row[0] == closest_row:
                result = row[col_idx]
                break
        return result

    def get_score_simple(self, kind, value):
        """Score lookup function for 2-column tables (without headers)."""
        obj = RiskScoreRules.query.filter_by(kind=kind).one()
        df = pd.read_json(obj.json)
        rows = self._parse_score_table(df)

        closest_one = take_closest(value, [i[0] for i in rows])
        result = None
        for row in rows:
            if row[0] == closest_one:
                result = row[1]
        return result

    def last_value_yoy(self, value, yoy):
        """Value and YoY in the last quarter of the metrics report."""
        l_key = [*value][-1]
        result = [{l_key: value[l_key]}, {l_key: yoy[l_key]}]
        print(f'\nLast quarter: {result}')
        return result

    def top_customers(self, size, parameter='n'):
        """Get top customers - 'p' for percent, 'n' for number."""
        sum_of_customers = self.calculate_total_customers()

        top_customers = self.sort_and_limit_customers(sum_of_customers, size, parameter)

        print(f'Num of top customers: {len(top_customers)}\nTop {size} customers: {top_customers}')
        return top_customers

    def net_retention_removing_largest_upsell(self, net_r):
        """Net Retention % Removing Largest Upsell."""
        max_value = max(net_r.values())

        result = {k: v for k, v in net_r.items() if v < max_value}
        print(f'\nnet_remov: {result}')
        return result

    def _parse_score_table(self, df):
        rows = [[self._digitize_digit(i) for i in df.columns.to_list()]]
        for _, row in df.to_records():
            record = [self._digitize_digit(col) if not pd.isna(col) else 0 for col in row.tolist()[1:]]
            rows.append(record)
        return rows

    def calculate_total_customers(self):
        sum_of_customers = {}
        for i, row in self.started_dataframe.iterrows():
            total = sum(row[self.date_fields])
            sum_of_customers[self.started_dataframe.iloc[i, 0]] = total
        return sum_of_customers

    def sort_and_limit_customers(self, sum_of_customers, size, parameter):
        top_customers = {}
        sorted_keys = sorted(sum_of_customers, key=sum_of_customers.get, reverse=True)

        if parameter == 'p':
            size = int(len(top_customers) * (size / 100))

        i = 0
        for key in sorted_keys:
            if i == size:
                break
            top_customers[key] = sum_of_customers[key]
            i += 1
        return top_customers

if __name__ == '__main__':
    fn = 'input.csv'
    kind = "gross_retention"
    
    risk_report = RiskReport(fn)

    res = risk_report.get_score_simple(kind, 0)
    print(res)
