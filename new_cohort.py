import pandas as pd

from metrics_data.metrics_report import MetricsReport

class Cohorts(MetricsReport):
    """Cohorts"""

    def __init__(self, *args, **kwargs):
        super(Cohorts, self).__init__(*args, **kwargs)

    def initial_cohort(self, year, cohort_type='arr'):
        if year not in self.years:
            print(f'FY {year} was not defined.')
            return None

        df_year = self.transformed_dataframe[self.transformed_dataframe['Fiscal Year'] == year]

        result = {}
        initial_indexes = set()

        months = self.date_fields
        k = 0

        for init_month in months:
            initial_month_data = df_year[df_year[init_month] > 0]

            if cohort_type == 'arr':
                result_for_month = []
                for m in months[k:]:
                    total_month_data = initial_month_data[initial_month_data[m] > 0]
                    result_for_month.append(total_month_data['ARR'].sum())
                result.update({init_month: result_for_month})

            elif cohort_type == 'number':
                result_for_month = []
                for m in months[k:]:
                    total_month_data = initial_month_data[initial_month_data[m] > 0]
                    result_for_month.append(len(total_month_data))
                result.update({init_month: result_for_month})

            initial_indexes.update(initial_month_data.index)
            k += 1

        cohort_df = pd.DataFrame.from_dict(result, orient='index', columns=months)
        return cohort_df

    def cohort_retention(self, initial_cohort):
        def cohort_retention_data(ser):
            return round(ser / initial_cohort.iloc[:, 0] * 100, 2)

        c_retention = initial_cohort.apply(cohort_retention_data, axis=0)

        print(f'Cohort Number of Customers:\n{initial_cohort}')
        print(f'Cohort Retention:\n{c_retention}')
        return c_retention


if __name__ == '__main__':
    fn = 'cohort.csv'
    report = Cohorts(fn)

    initial_cohort_arr = report.initial_cohort(2018, 'arr')
    initial_cohort_number = report.initial_cohort(2018, 'number')

    cohort_retention_arr = report.cohort_retention(initial_cohort_arr)
    cohort_retention_number = report.cohort_retention(initial_cohort_number)
