from metrics_data.metrics_report import MetricsReport

def search_last_quarter(dictionary):
    """Searching the last quarter of last year"""
    l_q = [*dictionary.keys()][-1]
    return {l_q: dictionary[l_q]}

fn = 'input.csv'
report = MetricsReport(fn)

main_row_data = report.main_row()
new_logo_arr_data = report.new_logo_arr()
upsell_arr_data = report.upsell_arr()
churn_arr_data = report.churn_arr()
downsell_arr_data = report.downsell_arr()


run_rate_quart = report.run_rate_arr()
new_logo_arr_quart = report.root(new_logo_arr_data, 'quarter')
upsell_arr_quart = report.root(upsell_arr_data, 'quarter')
churn_arr_quart = report.root(churn_arr_data, 'quarter')
downsell_arr_quart = report.root(downsell_arr_data, 'quarter')
total_churn_downsell_quart = report.total_churn_downsell(churn_arr_quart, downsell_arr_quart)

active_logos_calculate = report.logos_calcualte(main_row_data)
new_logos_calculate = report.logos_calcualte(new_logo_arr_data)
churn_logos_calculate = report.logos_calcualte(churn_arr_data)
downsell_logos_calculate = report.logos_calcualte(downsell_arr_data)
upsell_logos_calculate = report.logos_calcualte(upsell_arr_data)

#modify
last_q = [*new_logo_arr_quart.keys()][-1]
ch = 0

new_logo_arr_quart[last_q] += ch
upsell_arr_quart[last_q] += ch
total_churn_downsell_quart[last_q] += ch
new_logos_calculate[last_q] += ch
churn_logos_calculate[last_q] += ch
total_new_business_quart = {k: v1 + v2 for k, v1, v2 in zip(new_logo_arr_quart, new_logo_arr_quart.values(), upsell_arr_quart.values())}
run_rate_quart[last_q] = total_new_business_quart[last_q] - total_churn_downsell_quart[last_q] + run_rate_quart[[*run_rate_quart.keys()][-2]]


txns = report.txns_plus_minus(churn_logos_calculate, upsell_logos_calculate, downsell_logos_calculate)
avg_new_logo = report.avg_new_logo(new_logo_arr_quart, new_logos_calculate)
avg_upsell = report.avg_upsell(upsell_arr_quart, upsell_logos_calculate)

#YoY
run_rate_yoy = report.yoy_growth_per_quarter((run_rate_quart))
new_logo_yoy = report.yoy_growth_per_quarter(new_logo_arr_quart)
upsell_yoy = report.yoy_growth_per_quarter(upsell_arr_quart)
total_new_business_yoy = report.yoy_growth_per_quarter(total_new_business_quart)
active_logos_yoy = report.yoy_growth_per_quarter(active_logos_calculate)
new_logos_yoy = report.yoy_growth_per_quarter(new_logos_calculate)

#Last quarter
last_run_rate = search_last_quarter(run_rate_quart)
last_new_logo = search_last_quarter(new_logo_arr_quart)
last_upsell = search_last_quarter(upsell_arr_quart)
last_total_new_business = {last_q: last_new_logo[last_q] + last_upsell[last_q]}
last_total_churn_downsell = search_last_quarter(total_churn_downsell_quart)

last_acive_logos = search_last_quarter(active_logos_calculate)
last_new_logos = search_last_quarter(new_logos_calculate)
last_churn_logos = search_last_quarter(churn_logos_calculate)
last_downsell_logos = search_last_quarter(downsell_logos_calculate)
last_upsell_logos = search_last_quarter(upsell_logos_calculate)

last_avg_new_logo = search_last_quarter(avg_new_logo)
last_avg_upsell = search_last_quarter(avg_upsell)

#Last quarter yoy
last_run_rate_yoy = search_last_quarter(run_rate_yoy)
last_new_logo_yoy = search_last_quarter(new_logo_yoy)
last_upsell_yoy = search_last_quarter(upsell_yoy)
last_total_new_business_yoy = search_last_quarter(total_new_business_yoy)
last_acive_logos_yoy = search_last_quarter(active_logos_yoy)
last_new_logos_yoy = search_last_quarter(new_logos_yoy)

print(f'\nChanged\nRun Rate and yoy\n{run_rate_quart}\n{run_rate_yoy}\n'
      f'New and yoy\n{new_logo_arr_quart}\n{new_logo_yoy}\n'
      f'Upsell and yoy\n{upsell_arr_quart}\n{upsell_yoy}\n'
      f'Total New Business and yoy\n{total_new_business_quart}\n{total_new_business_yoy}\n'
      f'Total_Churn\n{total_churn_downsell_quart}\n')
print(f'\nLogos\n{new_logos_calculate}\n{upsell_logos_calculate}\n{churn_logos_calculate}\n{downsell_logos_calculate}')
print(f'\nAvg:\n{avg_new_logo}\n{avg_upsell}')

#Output last quarter
print(f'\nLast quarter of last year:')
print(f'Run Rate and yoy\n{last_run_rate}\n{last_run_rate_yoy}\n'
      f'New Logo and yoy\n{last_new_logo}\n{last_new_logo_yoy}\n'
      f'Upsell and yoy\n{last_upsell}\n{last_upsell_yoy}\n'
      f'Total New Business and yoy\n{last_total_new_business}\n{last_total_new_business_yoy}\n'
      f'Total Churn Downsell\n{last_total_churn_downsell}')
print(f'\nActive Logos and yoy\n{last_acive_logos}\n{last_acive_logos_yoy}\n'
      f'New Logos and yoy\n{last_new_logos}\n{last_new_logos_yoy}\n'
      f'Churn Logos, Downsell Logos, Upsell Logos\n{last_churn_logos}\n{last_downsell_logos}\n{last_upsell_logos}')
print(f'\nAvg New Logo and Avg Upsell\n{last_avg_new_logo}\n{last_avg_upsell}')

