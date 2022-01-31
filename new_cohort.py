import pandas as pd

from metrics_data.metrics_report import MetricsReport

class Cohorts(MetricsReport):
    """Cohorts"""
    def initial_cohort(self, year, cohort):

        if year in self.years:
            df_year = self.transformed_dataframe[[m for m in self.date_fields if m.split('/')[1] == str(year)]]
            #print(df_year)
            months = df_year.columns.tolist()
            k = 0
            result = {}
            initial_indexes = []

            for init_month in months:
                initial_month = [i for i in df_year[df_year[init_month].values > 0].index.tolist() if i not in initial_indexes]
                result_for_month = []
                if cohort == 'arr':
                    for m in months[k:]:
                        total_month = [i for i in df_year[df_year[m].values > 0].index.tolist() if i in initial_month]
                        result_for_month.append(sum(total_month))

                elif cohort == 'number':
                    """
                    print(f'initial month: {initial_month}\n'
                          f'used indexes: {initial_indexes}')
                    """
                    for m in months[k:]:
                        total_month = [i for i in df_year[df_year[m].values > 0].index.tolist() if i in initial_month]
                        #print(f'{init_month} {m}: {total_month}')
                        result_for_month.append(len(total_month))

                initial_indexes += initial_month
                result.update({init_month: result_for_month})
                k += 1

            cohort_df = pd.DataFrame.from_dict(result, orient='index', columns=months)

            return cohort_df

        else:
            print(f'FY {year} was not defined.')

    def cohort_retention(self, ic):
        """month / initial month * 100"""
        def cohort_reteintion_data(ser):
            res = round(ser / ic.iloc[:, 0] * 100, 2)
            return res

        c_retention = ic.apply(cohort_reteintion_data, axis=0)


        print(f'Cohort Number of Customer:\n{ic}')
        print(f'Cohort Retention:\n{c_retention}')
        return c_retention

if __name__ == '__main__':
    #fn = 'input.csv'
    fn = 'cohort.csv'
    #fn = 'orig.csv'
    report = Cohorts(fn)


    initial_cohort_arr = report.initial_cohort(2018, 'arr')
    initial_cohort_number = report.initial_cohort(2018, 'number')

    cohort_retention_arr = report.cohort_retention(initial_cohort_arr)
    cohort_retention_number = report.cohort_retention(initial_cohort_number)