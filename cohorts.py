from metrics_data.metrics_report import MetricsReport

class Cohorts(MetricsReport):
    """Cohorts"""

    def initial_cohort(self, period):
        """Step 1"""
        p = None
        if period == 'month':
            p = 1
        elif period == 'quarter':
            p = 3
        elif period == 'year':
            p = 12

        customers_names = self.transformed_dataframe[self.date_fields[:p]].sum(axis=1)
        tagged_customers = customers_names[customers_names.values > 0].index.tolist()

        print(f'Сustomer indexes that had greater than 0$ in the first {period}: {tagged_customers}\nnum = {len(tagged_customers)}')
        return tagged_customers

    def initial_cohort_arr(self, ic, period):
        """Step 2"""
        df = self.transformed_dataframe.filter(items=ic, axis=0)
        #print(df)

        # sum by month
        if period == 'month':
            print({k: self.transformed_dataframe[k].sum(axis=0) for k in self.date_fields})
            result_month = {k: df[k].sum(axis=0) for k in self.date_fields}
            return result_month

        # sum by quarter
        if period == 'quarter':
            lmq = ['03', '06', '09', '12']
            result_quarter = {key: df[key].sum(axis=0) for key in self.date_fields if key.split('/')[0] in lmq}
            s = 0

            return result_quarter

        # year -> the last month of the year
        if period == 'year':
            last_month_year = {key: df[key].sum(axis=0) for key in self.date_fields if key.split('/')[0] == '12'}
            return last_month_year

    def cohort_net_retention(self, dictionary):
        """
        Step 5
        ARR / first column * 100 - 100
        """
        result = {k: round(dictionary[k] / [*dictionary.values()][0] * 100 - 100, 2) for k in dictionary}
        print(f'Cohort Net Retention %: {result}')
        return result

if __name__ == '__main__':
    #fn = 'input.csv'
    fn = 'orig.csv'
    report = Cohorts(fn)
    initial_year = report.initial_cohort('year')
    initial_month = report.initial_cohort('month')
    ic_arr_month = report.initial_cohort_arr(initial_year, 'month')
    ic_arr_quarter = report.initial_cohort_arr(initial_year, 'quarter')
    ic_arr_year = report.initial_cohort_arr(initial_year, 'year')
    print(ic_arr_month)
    print(ic_arr_quarter)
    print(ic_arr_year)
    cohort_net = report.cohort_net_retention(ic_arr_month)
    cohort_net_quarter = report.cohort_net_retention(ic_arr_quarter)
    cohort_net_year = report.cohort_net_retention(ic_arr_year)


