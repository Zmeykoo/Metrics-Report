import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_data.base_report import BaseReport


class MetricsReport(BaseReport):
    def __init__(self, *args, **kwargs):
        super(MetricsReport, self).__init__(*args, **kwargs)

        # All dateframes
        df = self.transformed_dataframe[self.date_fields]

        # print(f'DF\n{df}')

        # New Logo ARR
        def new_logo_arr_data(val):
            result = val.copy()
            for a in range(1, len(val)):
                if val[a - 1] != 0:
                    result[a] = 0
            return result

        self.new_logo_arr_df = df.apply(new_logo_arr_data, axis=1)

        # Upsell ARR
        def upsell_arr_data(val):
            result = val.copy()
            for a in range(1, len(val)):
                difference = val[a] - val[a - 1]
                if val[a - 1] > 0 and difference > 0:
                    result[a] = difference
                else:
                    result[a] = 0
            return result

        self.upsell_arr_df = df.apply(upsell_arr_data, axis=1)

        # Total New Bussines ARR
        self.total_new_bussines_arr_df = self.new_logo_arr_df + self.upsell_arr_df

        # Churn
        def churn_data(val):
            result = val.copy()
            for a in range(1, len(val)):
                if val[a - 1] > 0 and val[a] == 0:
                    result[a] = val[a - 1]
                else:
                    result[a] = 0
            return result

        self.churn_df = df.apply(churn_data, axis=1)

        # Downsell
        def downsell_data(val):
            result = val.copy()
            for a in range(1, len(val)):
                if val[a - 1] > 0 and val[a] > 0 and val[a] - val[a - 1] < 0:
                    result[a] = val[a - 1] - val[a]
                else:
                    result[a] = 0
            return result

        self.downsell_df = df.apply(downsell_data, axis=1)

    def last_quart(self, data_quarter):
        result = {}
        values = list(data_quarter.values())
        keys = list(data_quarter.keys())
        years = list(int(date.split(' ')[1]) for date in keys)
        for item in range(3, len(data_quarter), 4):
            value= values[item]
            key = f'{years[item]}'
            result.update({key: value})
        print(f'last quarter: {result}')
        return result

    def sum_year(self, data_quarter):
        """
        sum of year
        """
        summed_by_quarter = {}
        values = list(data_quarter.values())
        step = 0
        for year in self.years:
            key = f'{year}'
            res = sum(values[step:step + 4])
            summed_by_quarter.update({key: res})
            step += 4

        print(f'sum of year: {summed_by_quarter}')
        return summed_by_quarter

    # Part 1
    def run_rate_arr(self):
        """
        run_rate_arr

        last month of quarter
        """

        dateframe = self.transformed_dataframe[self.date_fields]
        last_month_of_qurter = {}

        for item in range(2, len(self.date_fields), 3):
            month, year = self.date_fields[item].split('/')
            month = int(month)
            quartet = None
            if month <= 3:
                quarter = 1
            elif month <= 6:
                quarter = 2
            elif month <= 9:
                quarter = 3
            elif month <= 12:
                quarter = 4

            last_month_of_qurter.update({f"Q{quarter} {year}": dateframe.iloc[:, item].sum()})

        print(last_month_of_qurter)

        return last_month_of_qurter

    def yoy_growth_per_year(self,quarter_sum):
        quarter_keys = list(quarter_sum.keys())
        result = {}
        for year in range(1, len(quarter_sum)):
            key = f'{quarter_keys[year]}'
            prev_key = f'{quarter_keys[year - 1]}'
            if quarter_sum[prev_key] == 0:
                value = 0
            else:
                value = round(quarter_sum[key] / quarter_sum[prev_key] * 100 - 100, 2)
                result.update({key: value})

        print(f'yoy_year{result}')
        return result

    def yoy_growth_per_quarter(self, run_rate):
        """
        YoY growh per quarter.
        """
        result = {}
        years = [i for i in run_rate]
        for i in range(1, len(years)):
            if ' ' in years[i]:
                bits = years[i].split(' ')
            elif '/' in years[i]:
                bits = years[i].split('/')
            else:
                continue
            quarter, year = bits
            prev_year_key = "{} {}".format(quarter, (int(year) - 1))
            if prev_year_key not in run_rate:
                continue
            if run_rate[prev_year_key] == 0:
                diff = 0
            else:
                diff = round(run_rate[years[i]] / run_rate[prev_year_key] * 100 - 100, 2)
            result.update({years[i]: diff})

        return result

    def yoy_growth(self, data):
        """
        Takes annual ``data`` in form:
        {'01/2018': 8110576, '02/2018': 0, .. }

        Return percentage of increase at the end of quarter, comparing to beginning of quarter.
        (a/b * 100 - 100)
        """
        result = {}
        years = [i for i in self.years]
        for year_idx in range(1, len(years)):
            year = years[year_idx]
            prev_year = years[year_idx-1]
            for month in self.date_fields:
                bits = month.split('/')
                if int(bits[1]) != year:
                    continue
                month = bits[0]
                data_key = '%s/%s' % (month, year)
                value = 0
                if data_key in data:
                    prev_year_key = '%s/%s' % (month, prev_year)
                    if prev_year_key in data and data[prev_year_key] != 0:
                        value = (round(data[data_key] / data[prev_year_key] * 100 - 100, 2))
                result[data_key] = value

        return result

    def main_row(self):
        """
        Dataframe
        run_rate_arr
        """
        return self.transformed_dataframe[self.date_fields]

    def new_logo_arr(self):
        """
        Dataframe
        New Logo ARR
        """
        return self.new_logo_arr_df

    def upsell_arr(self):
        """
        Dateframe
        Upsell ARR
        """
        return self.upsell_arr_df

    def total_new_business_arr(self):
        """
        per quarter
        New Logo ARR + Upsell ARR
        """
        summed_by_month = {}
        for k in self.total_new_bussines_arr_df:
            summed_by_month[k] = sum(self.total_new_bussines_arr_df[k])

        result_quart = {}

        # Quarters sum
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
                    if key in summed_by_month.keys():
                        q_sum += summed_by_month[key]
                    else:
                        q_sum = 'empty'
                        break
                if q_sum != 'empty':
                    result_quart.update({q_name: q_sum})
        print(result_quart)
        return result_quart

    def churn_arr(self):
        """
        Dataframe
        Churn ARR
        """
        return self.churn_df

    def downsell_arr(self):
        """
        Dataframe
        Downsell ARR
        """
        return self.downsell_df

    def total_churn_downsell(self, churn_data, downsell_data):
        """
        month or quarter
        Total churn + downsell.
        """
        result = {}
        for k in churn_data:
            result[k] = churn_data[k] + downsell_data[k]

        return result

    def in_quarter_upsell_total_churn(self, upsell_arr, total_curn_downsell):
        """
        per quarter
        In Quarter Upsell - Total Churn
        """
        result = {}
        delmonth = self.date_fields[12:]

        us = {a: upsell_arr[a] for a in upsell_arr if a in delmonth}

        for k in us:
            if k in upsell_arr:
                result[k] = upsell_arr[k] - total_curn_downsell[k]
        #print(f'Month{result}')
        years = set(int(date.split('/')[1]) for date in delmonth)
        final = {}
        for year in years:
            #print(year)
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter + 1} {year}'
                q_sum = 0
                zero = '0'

                for month_of_quarter in range(1, 4, 1):
                    if len(str(month_of_quarter + (quarter*3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter*3) )}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in result.keys():
                        q_sum += result[key]

                final.update({q_name: q_sum})
        print('Upsell - Total churn', final)
        return final

    # Part 2
    def logos_calcualte(self, dateframe):
        """Active, New, Churned, Upsell, Downsell"""
        summed_by_month = {}
        for col in dateframe:
            k = 0
            for row in dateframe[col]:
                if row > 0:
                    k += 1
            summed_by_month[col] = k

        # Quarters sum
        result_quart = {}
        for year in self.years:
             for quarter in range(0, 4, 1):
                q_name = f'Q{quarter + 1} {year}'
                q_sum = 0
                zero = '0'

                for month_of_quarter in range(1, 4, 1):
                    if len(str(month_of_quarter + (quarter*3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter*3) )}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in summed_by_month.keys():
                        q_sum += summed_by_month[key]
                    else:
                        q_sum = 'empty'
                        break
                if q_sum != 'empty':
                    result_quart.update({q_name: q_sum})

        return result_quart

    def txns_plus_minus(self,churned_l, upsell_l, downsell_l):
        """+/- Txns"""
        result = {}
        for col in churned_l:
            if upsell_l[col]:
                value = upsell_l[col] - (churned_l[col] + downsell_l[col])
            else:
                value = 0
            result[col] = value
        #print(f'+/- Txns: {result}')
        return result

    def gross_logo_retention(self):
        """
        Gross Logo Retention

        Gross retention / Denominator * 100
        """
        #month
        summed_by_month = {}
        summed_by_month2 = {}
        for item in self.denominator_df.head():  # self.date_fields:
            summed_by_month.update({item: self.denominator_df[item].sum()})
        for item in self.gross_retention_df.head():  # self.date_fields:
            summed_by_month2.update({item: self.gross_retention_df[item].sum()})

        # Quarters sum
        years = set(int(date.split('/')[1]) for date in self.denominator_df.columns)
        denom_quarter = {}
        gross_quarter = {}

        for year in years:
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter + 1} {year}'
                q_sum = 0
                q_sum2= 0
                zero = '0'
                for month_of_quarter in range(1, 4, 1):
                    if len(str(month_of_quarter + (quarter * 3))) == 1:
                        key = f'{zero + str(month_of_quarter + (quarter * 3))}/{year}'
                    else:
                        key = f'{str(month_of_quarter + (quarter * 3))}/{year}'
                    if key in summed_by_month:
                        q_sum += summed_by_month[key]
                        q_sum2 += summed_by_month2[key]
                    else:
                        q_sum, q_sum2 = 'empty', 'empty'
                        break

                if q_sum != 'empty' and q_sum2 != 'empty':
                    denom_quarter.update({q_name: q_sum})
                    gross_quarter.update({q_name: q_sum2})

        result = {}
        for k in gross_quarter:
            if denom_quarter[k] > 0:
                result[k] = round(gross_quarter[k] / denom_quarter[k] * 100, 2)
            else:
                result[k] = 0
        return result

    def percent_upsell_txns(self, delta1, delta2):
        """
        % Upsell Txns

        Upsell Txns(+1 year) / Active Logo * 100 - 100
        """
        result = {}
        for k in delta2:
            if delta1[k] and delta1[k] > 0:
                value = round(delta1[k] / delta2[k] * 100, 2)
                result[k] = value
        #print(f"% Upsell Txns{result}\n")
        return result

    # Part 3
    def avg_new_logo(self, delta1, delta2):
        """
        Avg. New Logo

        New Logo ARR / New Logos
        """
        result = {}
        for k in delta1:
            if delta2[k] > 0:
                value = round(delta1[k] / delta2[k], 2)
            else:
                value = 0
            result[k] = value
        return result

    def avg_upsell(self, delta1, delta2):
        """
        Avg. Upsell

        Upsell ARR / Upsell Txns
        """
        result = {}
        for k in delta1:
            if delta2[k] > 0:
                value = round(delta1[k] / delta2[k], 2)
            else:
                value = 0
            result[k] = value
        return result

    # Part 4
    def magic_num_no_laq(self, total_arr, main_row_quart):
        """
        Magic Number No Lag

        Total ARR / main_row.
        """
        magic_no_lag = {}

        for k in total_arr:
            if k in main_row_quart:
                if main_row_quart[k] > 0:
                    magic_no_lag[k] = round(total_arr[k] / main_row_quart[k], 2)
                else:
                    magic_no_lag[k] = 0
            else:
                magic_no_lag[k] = 0

        return magic_no_lag

    def magic_num(self, magic_num_no_lag, total_arr, qtr):
        """
        Magic num Qtr : 1,2,3,4

        result[item] = total arr[item] / (total arr[item - qtr] / magic num no lag [item - qtr])
        """
        delta1 = [int(r) for r in total_arr.values()]
        delta2 = [float(r) for r in magic_num_no_lag.values()]
        quarter_fields = [r for r in total_arr.keys()]

        result = {}
        for item in range(qtr, len(total_arr)):
            if delta1[item-qtr] > 0 and delta2[item-qtr] > 0:
                result[quarter_fields[item]] = round(delta1[item] / (delta1[item-qtr] / delta2[item-qtr]), 2)
            else:
                result[quarter_fields[item]] = 0
        print(f'magic num: {qtr} Qtr\n{result}')
        return result

    def cac(self, new_logo_arr, magic_num_no_lag, new_logos, qtr):
        """
        CAC no Lag, 1, 2, 3, 4

        (New Logo ARR[item-qr] / Magic Num No Lag[item-qtr]) / New logos[item]
        """
        delta1 = [int(r) for r in new_logo_arr.values()]
        delta2 = [float(r) for r in magic_num_no_lag.values()]
        delta3 = [int(r) for r in new_logos.values()]
        quarter_fields = [r for r in new_logo_arr.keys()]
        result = {}

        for item in range(qtr, len(new_logo_arr)):
            if delta2[item-qtr] > 0 and delta3[item] > 0:
                result[quarter_fields[item]] = round((delta1[item-qtr] / delta2[item-qtr]) / delta3[item], 0)
            else:
                result[quarter_fields[item]] = 0

        print(f'cac num: {qtr} Qtr\n{result}')
        return result

    @classmethod
    def to_json(cls, df):
        return df.to_json()


if __name__ == '__main__':
    fn = 'input.csv'
    report = MetricsReport(fn)
    ###############################Data
    main_row_data = report.main_row()
    new_logo_arr_data = report.new_logo_arr()
    upsell_arr_data = report.upsell_arr()
    churn_arr_data = report.churn_arr()
    downsell_arr_data = report.downsell_arr()

    denominator_data = report.denominator()
    denominator = report.root(denominator_data, 'month')
    net_retention_data = report.net_retention(denominator)
    gross_retention_data = report.gross_retention(denominator)

    ###############################Part1

    print('\n\nFirst row')
    run_rate_arr = report.run_rate_arr()
    yoy_growth_run_rate = report.yoy_growth_per_quarter(run_rate_arr)
    last_quart_run = report.last_quart(run_rate_arr)
    yoy_last_quart_run = report.last_quart(yoy_growth_run_rate)

    print('\nNew_logo_ARR')
    new_logo_arr = report.root(new_logo_arr_data, 'month')
    new_logo_arr_quart = report.root(new_logo_arr_data, 'quarter')
    new_logo_arr_yoy = report.yoy_growth(new_logo_arr)
    sum_year_new_logo = report.sum_year(new_logo_arr_quart)
    yoy_year_new_logo = report.yoy_growth_per_year(sum_year_new_logo)

    print('\nUpsell ARR')
    upsell_arr = report.root(upsell_arr_data, 'month')
    upsell_arr_quart = report.root(upsell_arr_data, 'quarter')
    upsell_arr_yoy = report.yoy_growth(upsell_arr)
    upsell_arr_yoy_quart = report.yoy_growth_per_quarter(upsell_arr_quart)
    sum_year_upsell = report.sum_year(upsell_arr_quart)
    yoy_year_upsell = report.yoy_growth_per_year(sum_year_upsell)

    print('\nTotal new business ARR')
    new_business_arr = report.total_new_business_arr()
    new_business_arr_yoy = report.yoy_growth(new_business_arr)
    new_business_arr_yoy_quarter = report.yoy_growth_per_quarter(new_business_arr)
    sum_year_new_business = report.sum_year(new_business_arr)
    yoy_year_new_business_arr = report.yoy_growth_per_year(sum_year_new_business)

    print('\nChurn ARR')
    churn_arr = report.root(churn_arr_data, 'month')
    churn_arr_quart = report.root(churn_arr_data, 'quarter')

    print('\nDownsell ARR')
    downsell_arr = report.root(downsell_arr_data, 'month')
    downsell_arr_quart = report.root(downsell_arr_data, 'quarter')

    print('\nTotal Churn + Downsell')
    total_churn_downsell = report.total_churn_downsell(churn_arr, downsell_arr)
    total_churn_downsell_quart = report.total_churn_downsell(churn_arr_quart, downsell_arr_quart)

    print('\nIn quarter upsell - total churn')
    in_quarter_upsell_total_churn = report.in_quarter_upsell_total_churn(upsell_arr, total_churn_downsell)

    print('\nNet Retention')
    last_quart_net_retention = report.last_quart(net_retention_data)
    print('\nGross Retention')
    gross_retention = report.gross_retention(denominator)
    last_quart_gross_retention = report.last_quart(gross_retention_data)

    print('\nDelta Net Run Rate')
    delta_net_run_rate = report.delta_net_run_rate(in_quarter_upsell_total_churn, run_rate_arr)

    #####################Part 2
    print('\nLogos: Active, New, Churned, Upsell, Downsell')
    main_logos_calculate = report.logos_calcualte(main_row_data)
    main_logos_yoy = report.yoy_growth_per_quarter(main_logos_calculate)
    last_quart_main_logos = report.last_quart(main_logos_calculate)
    yoy_last_quart_main_logos = report.last_quart(main_logos_yoy)

    new_logos_calculate = report.logos_calcualte(new_logo_arr_data)
    new_logos_yoy = report.yoy_growth(new_logos_calculate)
    sum_year_new_logos = report.sum_year(new_logos_calculate)
    yoy_year_new_logos = report.yoy_growth_per_year(sum_year_new_logos)

    churn_logos_calculate = report.logos_calcualte(churn_arr_data)
    churn_logos_yoy = report.yoy_growth(churn_logos_calculate)
    sum_year_churn_logos = report.sum_year(churn_logos_calculate)

    upsell_logos_calculate = report.logos_calcualte(upsell_arr_data)
    upsell_logos_yoy = report.yoy_growth(upsell_logos_calculate)
    sum_year_upsell_logos = report.sum_year(upsell_logos_calculate)

    downsell_logos_calculate = report.logos_calcualte(downsell_arr_data)
    downsell_logos_yoy = report.yoy_growth(downsell_logos_calculate)
    sum_year_downsell_logos = report.sum_year(downsell_logos_calculate)

    print('\ntxns')
    txns = report.txns_plus_minus(churn_logos_calculate, upsell_logos_calculate, downsell_logos_calculate)

    print('\n% Upsell Txns')
    percent_upsell_txns = report.percent_upsell_txns(upsell_logos_calculate, main_logos_calculate)

    print('Gross Logo Ret')
    gross_logo_retention = report.gross_logo_retention()
    last_quart_gross_logo_retention = report.last_quart(gross_logo_retention)

    ################Part3
    print('\nAvg New Logo')
    avg_new_logo = report.avg_new_logo(new_logo_arr_quart, new_logos_calculate)
    year_new_logo = report.avg_new_logo(sum_year_new_logo, sum_year_new_logos)
    print('\nAvg Upsell')
    avg_upsell = report.avg_upsell(upsell_arr_quart, upsell_logos_calculate)
    year_avg_upsell = report.avg_upsell(sum_year_upsell, sum_year_upsell_logos)


    ################Part 4
    #print('\nMagic Num no Lag')
    #magic_num_no_lag = report.magic_num_no_laq(new_business_arr, run_rate_arr)

    #print('\nMagic Num')
    #magic_num = report.magic_num(magic_num_no_lag, new_business_arr, 4)

    #print('\nCAC')
    #cac = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 4)

    #print(result)
