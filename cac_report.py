import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from metrics_data.metrics_report import MetricsReport

class CAC(MetricsReport):
    def __init__(self, filename):
        super().__init__(filename)

    def top(self, size, use_percent=True):
        top_all_customers = self.transformed_dataframe[self.date_fields].sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum()

        if use_percent:
            size = int(len(top_all_customers) * (size / 100))

        top_customers = top_all_customers.iloc[:size].sum()

        return top_customers

    def ccr(self):
        top_all_customers = self.transformed_dataframe[self.date_fields].sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum()
        largest_customer = top_all_customers.iloc[0]

        customers_perc = [k for k in range(1, 101)]
        d = []
        d_num = []

        for k in customers_perc:
            d.append(round(top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum() / capital * 100, 2))
            d_num.append(round(top_all_customers.iloc[:int(len(top_all_customers) / 100 * k)].sum(), 2))

        result_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, d)
        result_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, d_num)]

        return result_perc

    def ucr(self):
        top_all_customers = self.upsell_arr_df.sum(axis=1).sort_values(ascending=False)
        capital = top_all_customers.sum()
        denominator = self.denominator_df.sum().iloc[-1]
        gross_ret = self.gross_retention_df.sum().iloc[-1]

        largest_customer = top_all_customers.iloc[0]
        largest_customer_2 = top_all_customers.iloc[1]

        impact_1 = round(largest_customer / denominator * 100, 2)
        impact_2 = round(largest_customer_2 / denominator * 100, 2)

        upsell_1 = round(largest_customer / capital * 100, 2)
        upsell_2 = round(largest_customer_2 / capital * 100, 2)

        customers_perc = [k for k in range(1, 101)]

        blue_l = []
        blue_l_num = []
        red_l = []
        red_l_num = []

        for k in customers_perc:
            blue_l.append(round(top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum() / capital * 100, 2))
            blue_l_num.append(round(top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum(), 2))

            red_l.append(round((gross_ret + capital - top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum()) / denominator * 100, 2))
            red_l_num.append(round(gross_ret + capital - top_all_customers.iloc[:int(len(top_all_customers) * (k / 100))].sum(), 2))

        blue_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, blue_l)]
        blue_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, blue_l_num)]
        red_perc = [{'x': k, 'y': v} for k, v in zip(customers_perc, red_l)]
        red_num = [{'x': k, 'y': v} for k, v in zip(customers_perc, red_l_num)]

        return blue_perc

if __name__ == '__main':
    fn = 'input.csv'
    report = CAC(fn)

    top1p = report.top(1, use_percent=True)
    print(f'\nTop 1%: {top1p}\n')

    top92p = report.top(92, use_percent=True)
    print(f'Top 92%: {top92p}\n')

    ccr = report.ccr()
    print(f'CCR:\n{ccr}\n')

    print('UCR')
    ucr = report.ucr()
