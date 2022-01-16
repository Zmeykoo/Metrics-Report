import sys
import os
import json

import pandas as pd
from numpy import isnan

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_data.base_report import BaseReport

from models import RiskScoreRules, MetricsReportHistory


def take_closest(num, keys):
    """
    Given a list of integers, returns number which is is the closest to ``num``.
    """
    return min(keys, key=lambda x:abs(x-num))


class RiskReport(BaseReport):

    @classmethod
    def get_score(cls, kind, vv, hv):
        """
        Score lookup method for 2d tables.

        ``kind`` -- Risk score type.
                    For example: "arr_x_growth" or "upsell_x_growth"

        ``vv`` -- 'vertical value', -- a value from last quarter of last year.
                  For example: 2500000

        ``hv`` -- 'horizontal value', for example: percent of year-over-year growth
                  or 'gross $ retenion'.
        """
        obj = RiskScoreRules.query.filter_by(kind=kind).one()
        df = pd.read_json(obj.json)
        row0 = []
        for row in df.columns.to_list():
            if row == 'Unnamed: 0':
                row0.append(None)
            else:
                row0.append(int(cls._digitize_digit(row)))
        rows = [row0]
        for row in df.to_records():
            record = []
            for col in row.tolist()[1:]:
                if type(col) == str:
                    record.append(int(cls._digitize_digit(col)))
                elif isnan(col):
                    record.append(0)
                else:
                    record.append(col)
            rows.append(record)

        result = None
        closest_row = take_closest(vv, [i[0] for i in rows if i[0] is not None])
        closest_column = take_closest(hv, rows[0][1:])
        col_idx = rows[0].index(closest_column)
        result = None
        for row in rows[1:]:
            if int(cls._digitize_digit(row[0])) == closest_row:
                result = row[col_idx]
                break
        return result

    @classmethod
    def get_score_simple(cls, kind, value):
        """
        Score lookup function for 2-column tables (without headers)

        ``kind`` -- Risk score type.
                    For example: "gross_retention" or "net_retention"

        ``value`` -- Value from last quarter of last year.
                     For example: 2500000
        """
        obj = RiskScoreRules.query.filter_by(kind=kind).one()
        df = pd.read_json(obj.json)
        rows = [[int(cls._digitize_digit(i)) for i in df.columns.to_list()]]
        for _,k, v in df.to_records():
            rows.append([int(cls._digitize_digit(k)), int(cls._digitize_digit(v))])

        result = None
        closest_one = take_closest(value, [i[0] for i in rows])
        for row in rows:
            if row[0] == closest_one:
                result = row[1]
        return result

    def last_value_yoy(self,value, yoy):
        """
        Value and yoy in last quarter in metrics report
        """
        #print(f'LAST\n {value}\n{yoy}')
        l_key = [*value][-1]
        result = [{l_key: value[l_key]}, {l_key: yoy[l_key]}]

        print(f'\nLast quarter:{result}')
        return result

    def top(self, size, parametr):
        """
        Top customers
        p - percent of customers
        n - number of customers
        """
        #calculate sum of customer
        sum_of_customers = {}
        for i, row in self.started_dataframe.iterrows():
            total = sum([value for value in row[self.date_fields]])
            sum_of_customers[self.started_dataframe.iloc[i, 0]] = total

        #sorting
        top_customers = {}
        sorted_keys = sorted(sum_of_customers, key=sum_of_customers.get).__reversed__()

        for key in sorted_keys:
            top_customers[key] = sum_of_customers[key]

        #print(f'top customers:{top_customers}')
        if parametr == "p":
            size = int(len(top_customers) * (size / 100))
        #print(f'{size}')

        #top size
        top_size = {}
        i = 0
        for key in top_customers.keys():
            if i == size:
                break
            top_size[key] = top_customers[key]
            i += 1
        print(f'Num of top customers:{len(top_size)}\nTop {size} customers:{top_size}')
        return top_customers

    def net_retention_removing_largest_upsell(self, net_r):
        """Net Retention % Removing Largest Upsell"""
        #the largest upsell
        all_values = net_r.values()
        max_value = max(all_values)

        #remove
        result = {}
        for k in net_r:
            if net_r[k] < max_value:
                result[k] = net_r[k]
        print(f'\nnet_remov: {result}')
        return result


if __name__ == '__main__':
    fn = 'input.csv'
    kind = "gross_retention"
    #import pdb;pdb.set_trace()
    res = RiskReport.get_score_simple(kind, 0)
    print(res)
    res = RiskReport.get_score_simple(kind, 270.44)
    #res = RiskReport.get_score(kind, 3825984, 392)
    #kind = "upsell_x_growth"
    #res = RiskReport.get_score(kind, 3825984, 392)
    print(res)
    sys.exit()

    report = RiskReport(fn)

    denominator_data = report.denominator()
    denominator = report.root(denominator_data, 'month')

    net_retention = report.net_retention(denominator)
    net_retention_data = report.net_retention(denominator)

    net_retention_removing_largest_upsell = report.net_retention_removing_largest_upsell(net_retention)

    print('\nTop customers')
    top = report.top(2)
