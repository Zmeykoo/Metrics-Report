from metrics_data.metrics_report import MetricsReport

class Cohorts(MetricsReport):
    """Cohorts"""

    def initial_cohort(self, dataframe, first_period, period):
        """Step 1"""

        p = None
        if first_period == 'month':
            p = 1
        elif first_period == 'quarter':
            p = 3
        elif first_period == 'year':
            p = 12

        df = None
        if dataframe == "ARR":
            df = self.transformed_dataframe[self.date_fields].copy()

        elif dataframe == "Gross Retained ARR":
            df = self.gross_retention_df.copy()


        customers_names = df[df.columns[:p]].sum(axis=1)
        tagged_customers = customers_names[customers_names.values > 0].index.tolist()

        print(f'Сustomer indexes that had greater than 0$ in the first {period}: {tagged_customers}'
              f'\nnum = {len(tagged_customers)}')
        df = df.filter(items=tagged_customers, axis=0)

        # sum by month
        if period == 'month':
            result_month = {key: df[key].sum(axis=0) for key in df[df.columns]}
            return result_month

        # sum by quarter
        if period == 'quarter':
            lmq = ['03', '06', '09', '12']
            result_quarter = {key: df[key].sum(axis=0) for key in df[df.columns] if key.split('/')[0] in lmq}
            return result_quarter

        # year -> the last month of the year
        if period == 'year':
            last_month_year = {key: df[key].sum(axis=0) for key in df[df.columns] if key.split('/')[0] == '12'}
            return last_month_year

    def cohort(self, dictionary):
        """
        Step 5
        ARR / first column * 100 - 100
        """

        result = {k: round(dictionary[k] / [*dictionary.values()][0] * 100 - 100, 2) for k in dictionary}
        print(f'Cohort Net Retention %: {result}')
        return result

    def graph(self):
        """Direct Customer Cohort Analysis"""
        # Max = round(max(total)
        # Minimal = round(max(total), 2) / 6 or / len(self.years)
        #y_axis = [num for num in range(minimal, maximum, minimal)]
        years = self.years
        x_axis = []
if __name__ == '__main__':
    fn = 'input.csv'
    #fn = 'orig.csv'
    report = Cohorts(fn)

    # ARR & Cohort Net Retention%
    initial_year = report.initial_cohort('ARR', 'year', 'month')
    initial_month = report.initial_cohort('ARR', 'month', 'month')

    #initial_gross_year = report.initial_cohort('Gross Retained ARR', 'year', 'month')
    #initial_gross_month = report.initial_cohort('Gross Retained ARR', 'month', 'month')


    cohort_net_arr_year = report.cohort(initial_year)
    cohort_net_arr_month = report.cohort(initial_month)
    #cohort_net_gross_year = report.cohort(initial_gross_year)

    # Gross Retained ARR & Cohort Gross Retention %
    #initial_gross_month = report.initial_cohort('month')
    # Number of Customers
    # Cohort Gross Logo Retention
