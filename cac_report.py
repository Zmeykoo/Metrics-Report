import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from metrics_data.base_report import BaseReport
from metrics_data.metrics_report import MetricsReport

class CAC(MetricsReport):
    """
    Customer ARR Concentration Report.
    """
    def top(self, size, use_percent=True):
        """
        Top customers
        ``use_percent```-- Return first N top customers, where N is percent of ``size``
                           Otherwise return first N customers, where N is ``size``.
        """

        top_all_customers = self.transformed_dataframe[self.date_fields].sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum(axis=0)

        # calc size
        if use_percent:
            size = int(len(top_all_customers) * (size / 100))

        top_customers = top_all_customers.iloc[:size].sum()

        #print(f'Capital: {capital}')
        #print(f'Top {size} customers = {top_customers}')
        #print(f'impact: {impact}%\n')

        return top_customers

    def ccr(self):
        """
        Customer Concentration Risk
        """

        top_all_customers = self.transformed_dataframe[self.date_fields].sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum(axis=0)
        largest_customer = top_all_customers.iloc[0]

        # Creating series
        customers_perc = [k for k in range(1, 101)]
        d = []
        d_num = []

        for k in customers_perc:
            d.append(round(top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum() / capital * 100, 2))
            d_num.append(round(top_all_customers.iloc[:int(len(top_all_customers) / 100 * k)].sum(), 2))

        result_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, d)]
        result_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, d_num)]


        print(f'Num\n{result_num}')
        print(f'Percent\n{result_perc}')


        print(f'Largest customer: {largest_customer}')
        print(self.transformed_dataframe[self.date_fields])
        return result_perc

    def ucr(self):
        """Trailing 12 months Upsell ARR Concentration"""

        top_all_customers = self.upsell_arr_df.sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum(axis=0)
        denominator = self.denominator_df.sum(axis=0).iloc[-1]
        gross_ret = self.gross_retention_df.sum(axis=0).iloc[-1]


        largest_customer = top_all_customers.iloc[0]
        largest_customer_2 = top_all_customers.iloc[1]

        impact_1 = round(largest_customer / denominator * 100, 2)
        impact_2 = round(largest_customer_2 / denominator * 100, 2)

        upsell_1 = round(largest_customer / capital * 100, 2)
        upsell_2 = round(largest_customer_2 / capital * 100, 2)

        # Creating table data
        customers_perc = [k for k in range(1, 101)]

        blue_l = []
        blue_l_num = []

        red_l = []
        red_l_num = []

        for k in customers_perc:
            blue_l.append(round(top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum() / capital * 100, 2))
            blue_l_num.append(round(top_all_customers.iloc[:int(len(top_all_customers) / 100 * k)].sum(), 2))

            red_l.append(round((gross_ret + capital - top_all_customers.iloc[:int(
                len(top_all_customers) * (k / 100))].sum()) / denominator * 100, 2))
            red_l_num.append(round(gross_ret + capital - top_all_customers.iloc[:int(
                len(top_all_customers) * (k / 100))].sum(), 2))


        # X,Y axises
        blue_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, blue_l)]
        blue_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, blue_l_num)]

        red_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, red_l)]
        red_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, red_l_num)]

        print(f'Blue Num\n{blue_num}')
        print(f'Blue Percent\n{blue_perc}')
        print(f'Red Num\n{red_num}')
        print(f'Red Percent\n{red_perc}')

        print(f'1st largest customer: {largest_customer}')
        print(f'2nd largest customer: {largest_customer_2}')
        print(f'Impact_1st_customer: {impact_1}')
        print(f'Impact_2st_customer: {impact_2}')
        print(f'Upsell%_1_st_customer: {upsell_1}')
        print(f'Upsell%_2_nd_customer: {upsell_2}')

        return blue_perc


if __name__ == '__main__':
    fn = 'input.csv'
    #fn = 'orig.csv'
    report = CAC(fn)
    """
    top1p = report.top(1, use_percent=True)
    print('\nTop 1%: {}\n'.format(top1p))
    top92p = report.top(92, use_percent=True)
    print('\nTop 92%: {}\n'.format(top92p))
    """
    #ccr = report.ccr()
    #print('\nCCR:\n{}\n'.format(ccr))

    print('UCR')
    ucr = report.ucr()
