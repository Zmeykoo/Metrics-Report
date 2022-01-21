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

    def initial_cohort_arr(self, ic):
        """Step 2"""
        df = self.transformed_dataframe.filter(items=ic, axis=0)
        print(df)

        #sum by month
        result_month = {k: df[k].sum(axis=0) for k in self.date_fields}

        #sum by quarter
        result_quarter = {}
        s = 0

        for year in self.years:
            diaposon = self.date_fields[s:s + 3]
            for quarter in range(0, 4, 1):
                q_name = f'Q{quarter + 1} {year}'
                if len(diaposon) == 3:
                    result_quarter[q_name] = df[diaposon].values.sum()
                s += 3

        print(f'Result Cohort ARR month: {result_month}')
        print(f'Result Cohort ARR quarter: {result_quarter}')
        return result_month

    def cohort_net_retention(self, dictionary):
        """
        Step 5
        ARR / first column * 100 - 100
        """
        result = {k: round(dictionary[k] / [*dictionary.values()][0] * 100 - 100, 2) for k in dictionary}
        print(f'Cohort Net Retention %: {result}')
        return result

if __name__ == '__main__':
    fn = 'input.csv'
    #fn = 'orig.csv'
    report = Cohorts(fn)
    initial_year = report.initial_cohort('year')
    initial_month = report.initial_cohort('month')
    ic_arr = report.initial_cohort_arr(initial_year)
    cohort_net = report.cohort_net_retention(ic_arr)
