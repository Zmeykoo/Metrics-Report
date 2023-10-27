import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_data.base_report import BaseReport

class MetricsReport(BaseReport):
    def __init__(self, *args, **kwargs):
        super(MetricsReport, self).__init__(*args, **kwargs)
        self.initialize_data()

    def initialize_data(self):
        self.dateframe = self.transformed_dataframe[self.date_fields]
        self.years = [int(date.split('/')[1]) for date in self.date_fields]
    
    def apply_custom_logic(self, data, custom_func):
        result = data.copy()
        for a in range(1, len(data)):
            result[a] = custom_func(data, a)
        return result
    
        
    def new_logo_arr_data(self, data):
        def custom_logic(data, a):
            return 0 if data[a - 1] != 0 else data[a]
        return self.apply_custom_logic(data, custom_logic)


        self.new_logo_arr_df = df.apply(new_logo_arr_data, axis=1)

    def upsell_arr_data(self, data):
        def custom_logic(data, a):
            difference = data[a] - data[a - 1]
            return difference if data[a - 1] > 0 and difference > 0 else 0
        return self.apply_custom_logic(data, custom_logic)

    def churn_data(self, data):
        def custom_logic(data, a):
            return data[a - 1] if data[a - 1] > 0 and data[a] == 0 else 0
        return self.apply_custom_logic(data, custom_logic)

        # Downsell
    def downsell_data(self, data):
        def custom_logic(data, a):
            return data[a - 1] - data[a] if data[a - 1] > 0 and data[a] > 0 and data[a] - data[a - 1] < 0 else 0
        return self.apply_custom_logic(data, custom_logic)

    def last_quarter_data(self, data_quarter):
        result = {}
        for item in range(3, len(data_quarter), 4):
            value = list(data_quarter.values())[item]
            year = list(int(date.split(' ')[1]) for date in data_quarter.keys())[item]
            key = f'{year}'
            result[key] = value
        return result

    def sum_by_year(self, data_quarter):
        summed_by_quarter = {}
        values = list(data_quarter.values())
        step = 0
        for year in self.years:
            key = f'{year}'
            res = sum(values[step:step + 4])
            summed_by_quarter[key] = res
            step += 4
        return summed_by_quarter

    # Part 1
    def run_rate_arr_data(self):
        last_month_of_quarter = {}
        for item in range(2, len(self.date_fields), 3):
            month, year = self.date_fields[item].split('/')
            quarter = (int(month) - 1) // 3 + 1
            key = f'Q{quarter} {year}'
            last_month_of_quarter[key] = self.dateframe.iloc[:, item].sum()
        return last_month_of_quarter
        
    def yoy_growth(self, data):
        result = {}
        data_keys = list(data.keys())
        for i in range(1, len(data_keys)):
            key = data_keys[i]
            prev_key = data_keys[i - 1]
            if data[prev_key] == 0:
                value = 0
            else:
                value = round(data[key] / data[prev_key] * 100 - 100, 2)
            result[key] = value
        return result

    
    def generate_report(self):
        new_logo_arr = self.new_logo_arr_data(self.dateframe)
        upsell_arr = self.upsell_arr_data(self.dateframe)
        total_new_bussines_arr = {
            k: new_logo_arr[k] + upsell_arr[k] for k in self.date_fields
        }
        churn_arr = self.churn_data(self.dateframe)
        downsell_arr = self.downsell_data(self.dateframe)
        last_quarter_run_rate = self.last_quarter_data(self.run_rate_arr_data())
        yoy_growth_run_rate = self.yoy_growth(self.run_rate_arr_data())
        last_quart_new_logo_arr = self.last_quarter_data(new_logo_arr)
        yoy_new_logo_arr = self.yoy_growth(new_logo_arr)
        last_quart_upsell_arr = self.last_quarter_data(upsell_arr)
        yoy_upsell_arr = self.yoy_growth(upsell_arr)
        last_quart_total_new_bussines_arr = self.last_quarter_data(total_new_bussines_arr)
        yoy_total_new_bussines_arr = self.yoy_growth(total_new_bussines_arr)
        last_quart_churn_arr = self.last_quarter_data(churn_arr)
        last_quart_downsell_arr = self.last_quarter_data(downsell_arr)
        total_churn_downsell = {
            k: churn_arr[k] + downsell_arr[k] for k in self.date_fields
        }
        in_quarter_upsell_total_churn = self.in_quarter_upsell_total_churn(upsell_arr, total_churn_downsell)
        last_quart_net_retention = self.last_quarter_data(self.net_retention())
        last_quart_gross_retention = self.last_quarter_data(self.gross_retention())
        delta_net_run_rate = {
            k: in_quarter_upsell_total_churn[k] - last_quarter_run_rate[k] for k in self.date_fields
        }
        # Print or return the generated reports as needed

if __name__ == '__main__':
    fn = 'input.csv'
    report = MetricsReport(fn, filters=[...], date_filters=[...])
    report.generate_report()
