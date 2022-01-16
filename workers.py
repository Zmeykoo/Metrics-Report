import sys
import os
import json
import logging
from tempfile import mktemp

from rq.decorators import job
from sqlalchemy.orm.exc import NoResultFound

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics_data.app import redis_client, db
from metrics_data.metrics_report import MetricsReport
from metrics_data.risk_report import RiskReport
from metrics_data.utils import get_readable_traceback
from models import MetricsReportHistory

logger = logging.getLogger(__name__)


def _row(title, date_fields, data, sign='$'):
    record = [title]
    for date in date_fields:
        if date not in data:
            value = ''
        else:
            value = data[date]
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = ''
        else:
            if value == 0:
                value = ''
        if sign == '$' and value != '':
            value = '${:0,.0f}'.format(value)
        elif sign == '%' and value != '':
            if abs(round(value, 1)) > 0:
                if abs(float(value) - int(value)) > 0:
                    value = '{:0,.1f}%'.format(value)
                else:
                    value = '{:0,.0f}%'.format(value)
            else:
                value = ''
        elif sign is None:
            if value != '':
                value = '{:0,.1f}%'.format(value)
        record.append(value)
    return record


def _last_quarter(dct):
    last_year = None
    for key in dct:
        try:
            year = int(key.split(' ')[1])
        except (TypeError, ValueError, IndexError):
            continue
        if last_year is not None and year <= last_year:
            continue
        last_year = year

    q4 = 'Q4 {}'.format(last_year)
    q3 = 'Q3 {}'.format(last_year)
    q2 = 'Q2 {}'.format(last_year)
    q1 = 'Q1 {}'.format(last_year)
    if q4 in dct:
        return q4
    if q3 in dct:
        return q3
    if q2 in dct:
        return q2
    if q1 in dct:
        return q1


def generate_metrics_report_data(csv_path, filters=None):
    """
    Generate Metrics Report.
    """
    report = MetricsReport(csv_path, filters=filters)
    main_row_data = report.main_row()
    new_logo_arr_data = report.new_logo_arr()
    upsell_arr_data = report.upsell_arr()
    churn_arr_data = report.churn_arr()
    downsell_arr_data = report.downsell_arr()

    denominator_data = report.denominator()
    denominator = report.root(denominator_data, 'month')
    net_retention_data = report.net_retention(denominator)
    gross_retention_data = report.gross_retention(denominator)


    run_rate_arr = report.run_rate_arr()
    yoy_growth_run_rate = report.yoy_growth_per_quarter(run_rate_arr)
    #last_quart_run = report.last_quart(run_rate_arr)
    yoy_last_quart_run = report.last_quart(yoy_growth_run_rate)

    #new_logo_arr = report.root(new_logo_arr_data, 'month')
    new_logo_arr_quart = report.root(new_logo_arr_data, 'quarter')
    new_logo_arr_yoy =  report.yoy_growth_per_quarter(new_logo_arr_quart)
    sum_year_new_logo = report.sum_year(new_logo_arr_quart)
    yoy_year_new_logo = report.yoy_growth_per_year(sum_year_new_logo)

    upsell_arr_quart = report.root(upsell_arr_data, 'quarter')
    upsell_arr_yoy = report.yoy_growth_per_quarter(upsell_arr_quart)
    sum_year_upsell = report.sum_year(upsell_arr_quart)
    yoy_year_upsell = report.yoy_growth_per_year(sum_year_upsell)

    new_business_arr = report.total_new_business_arr()
    new_business_arr_yoy = report.yoy_growth_per_quarter(new_business_arr)
    sum_year_new_business = report.sum_year(new_business_arr)
    yoy_year_new_business_arr = report.yoy_growth_per_year(sum_year_new_business)

    churn_arr = report.root(churn_arr_data, 'month')
    churn_arr_quart = report.root(churn_arr_data, 'quarter')

    downsell_arr = report.root(downsell_arr_data, 'month')
    downsell_arr_quart = report.root(downsell_arr_data, 'quarter')

    total_churn_downsell = report.total_churn_downsell(churn_arr, downsell_arr)
    total_churn_downsell_quart = report.total_churn_downsell(churn_arr_quart, downsell_arr_quart)

    upsell_arr = report.root(upsell_arr_data, 'month')
    in_quarter_upsell_total_churn = report.in_quarter_upsell_total_churn(upsell_arr, total_churn_downsell)

    last_quart_net_retention = report.last_quart(net_retention_data)
    last_quart_gross_retention = report.last_quart(gross_retention_data)

    delta_net_run_rate = report.delta_net_run_rate(in_quarter_upsell_total_churn, run_rate_arr)

    main_logos_calculated = report.logos_calcualte(main_row_data)
    main_logos_yoy = report.yoy_growth_per_quarter(main_logos_calculated)
    last_quart_main_logos = report.last_quart(main_logos_calculated)
    yoy_last_quart_main_logos = report.last_quart(main_logos_yoy)

    new_logos_calculated = report.logos_calcualte(new_logo_arr_data)
    new_logos_yoy = report.yoy_growth_per_quarter(new_logos_calculated)
    sum_year_new_logos = report.sum_year(new_logos_calculated)
    yoy_year_new_logos = report.yoy_growth_per_year(sum_year_new_logos)

    churn_logos_calculated = report.logos_calcualte(churn_arr_data)
    churn_logos_yoy = report.yoy_growth_per_quarter(churn_logos_calculated)
    sum_year_churn_logos = report.sum_year(churn_logos_calculated)

    upsell_logos_calculated = report.logos_calcualte(upsell_arr_data)
    #upsell_logos_yoy = report.yoy_growth_per_quarter(upsell_logos_calculated)
    sum_year_upsell_logos = report.sum_year(upsell_logos_calculated)

    downsell_logos_calculated = report.logos_calcualte(downsell_arr_data)
    #downsell_logos_yoy = report.yoy_growth_per_quarter(downsell_logos_calculated)
    sum_year_downsell_logos = report.sum_year(downsell_logos_calculated)

    txns = report.txns_plus_minus(churn_logos_calculated, upsell_logos_calculated, downsell_logos_calculated)

    percent_upsell_txns = report.percent_upsell_txns(upsell_logos_calculated, main_logos_calculated)

    gross_logo_retention = report.gross_logo_retention()
    last_quart_gross_logo_retention = report.last_quart(gross_logo_retention)

    avg_new_logo = report.avg_new_logo(new_logo_arr_quart, new_logos_calculated)

    avg_upsell = report.avg_upsell(upsell_arr_quart, upsell_logos_calculated)

    headers = list(run_rate_arr.keys())
    run_rate_arr_q4s = [k.split(' ')[1] for k in run_rate_arr.keys() if k.startswith('Q4')]
    run_rate_arr.update({' ': '',})
    headers.append(' ')
    headers.extend(run_rate_arr_q4s)
    table = [[' '] + list(headers)]

    # Add summary columns now
    run_rate_arr.update(yoy_last_quart_run)
    new_logo_arr_quart.update(sum_year_new_logo)
    new_logo_arr_yoy.update(yoy_year_new_logo)
    upsell_arr_quart.update(sum_year_upsell)
    upsell_arr_yoy.update(yoy_year_upsell)
    new_business_arr.update(sum_year_new_business)
    new_business_arr_yoy.update(yoy_year_new_business_arr)

    #for year in run_rate_arr_q4s:
    #    key = 'Q4 {}'.format(year)
    #    value = run_rate_arr.get(key)
    #    run_rate_arr.update({year: value})

    net_retention_data.update(last_quart_net_retention)
    gross_retention_data.update(last_quart_gross_retention)

    main_logos_calculated.update(last_quart_main_logos)
    main_logos_yoy.update(yoy_last_quart_main_logos)
    churn_logos_calculated.update(sum_year_churn_logos)
    gross_logo_retention.update(last_quart_gross_logo_retention)
    new_logos_calculated.update(yoy_year_new_logos)

    downsell_logos_calculated.update(sum_year_downsell_logos)
    avg_upsell.update(sum_year_upsell_logos)

    table.append(_row("Run Rate ARR", headers, run_rate_arr))
    table.append(_row("YoY Growth", headers, yoy_growth_run_rate, sign='%'))

    table.append(_row("Total New Business ARR", headers, new_business_arr))
    table.append(_row("YoY Growth", headers, new_business_arr_yoy, sign='%'))

    table.append(_row("New Logo ARR", headers, new_logo_arr_quart))
    table.append(_row("YoY Growth", headers, new_logo_arr_yoy, sign='%'))

    table.append(_row("Upsell ARR", headers, upsell_arr_quart))
    table.append(_row("YoY Growth", headers, upsell_arr_yoy, sign='%'))

    table.append(_row("Total Churn + Downsell", headers, total_churn_downsell_quart))
    table.append(_row("Churn ARR", headers, churn_arr_quart))
    table.append(_row("Downsell ARR", headers, downsell_arr_quart))

    table.append(_row("Net $ Retention (ttm)", headers, net_retention_data, sign='%'))
    table.append(_row("Gross $ Retention (ttm)", headers, gross_retention_data, sign='%'))
    table.append(_row("In Quarter Upsell - Total Churn", headers, in_quarter_upsell_total_churn))
    table.append(_row("Delta Net $/Run Rate", headers, delta_net_run_rate, sign='%'))

    table.append(_row("Active Logos", headers, main_logos_calculated, sign=None))
    table.append(_row("YoY Growth", headers, main_logos_yoy, sign='%'))
    table.append(_row("New Logos", headers, new_logos_calculated, sign=None))
    table.append(_row("YoY Growth", headers, new_logos_yoy, sign='%'))

    table.append(_row("Churned Logos", headers, churn_logos_calculated, sign=None))
    table.append(_row("Upsell Txns", headers, churn_logos_yoy, sign=None))
    table.append(_row("Downsell Txns", headers, downsell_logos_calculated, sign=None))
    table.append(_row("+/- Txns", headers, txns, sign=None))
    table.append(_row("Gross Logo Retention", headers, gross_logo_retention, sign='%'))
    table.append(_row("% Upsell Txns", headers, percent_upsell_txns, sign='%'))

    table.append(_row("Avg. New Logo", headers, avg_new_logo))
    table.append(_row("Avg. Upsell", headers, avg_upsell))

    # Calculate risk score
    last_key = _last_quarter(run_rate_arr)
    last_quarter_arr = 0
    if last_key:
        if last_key in yoy_growth_run_rate:
            yoy_value = yoy_growth_run_rate[last_key]
        else:
            yoy_value = 0
        run_rate_score = RiskReport.get_score("arr_x_growth",
                                              run_rate_arr[last_key], yoy_value)
        last_quarter_arr = run_rate_arr[last_key]
    else:
        run_rate_score = 1000

    last_key = _last_quarter(new_logo_arr_quart)
    if last_key:
        if last_key in new_logo_arr_yoy:
            yoy_value = new_logo_arr_yoy[last_key]
        else:
            yoy_value = 0
        new_logo_score = RiskReport.get_score("new_logo_x_growth",
                                              new_logo_arr_quart[last_key],
                                              yoy_value)
    else:
        new_logo_score = 1000

    if last_key:
        new_logo_orig = new_logo_arr_quart[last_key]
        new_logo_yoy_orig = new_logo_arr_yoy[last_key]
    else:
        new_logo_orig = 0
        new_logo_yoy_orig = 0

    last_key = _last_quarter(upsell_arr_quart)
    if last_key:
        if last_key in upsell_arr_yoy:
            yoy_value = upsell_arr_yoy[last_key]
        else:
            yoy_value = 0
        upsell_arr_score = RiskReport.get_score("upsell_x_growth",
                                                upsell_arr_quart[last_key],
                                                yoy_value)
    else:
        upsell_arr_score = 1000

    if last_key:
        upsell_arr_orig_value = upsell_arr_quart[last_key]
        upsell_arr_orig_yoy_value = yoy_value
    else:
        upsell_arr_orig_value = 0
        upsell_arr_orig_yoy_value = 0

    last_key = _last_quarter(net_retention_data)
    if last_key:
        if last_key in gross_retention_data:
            gross_value = gross_retention_data[last_key]
        else:
            gross_value = 0
        gorss_net_retention_score = RiskReport.get_score("gross_retention_x_net_retention",
                                                         net_retention_data[last_key],
                                                         gross_value)
        gorss_net_retention_orig_value = net_retention_data[last_key]
    else:
        gorss_net_retention_score = 1000
        gorss_net_retention_orig_value = 0

    last_key = _last_quarter(net_retention_data)
    if last_key:
        net_retention_score = RiskReport.get_score_simple("net_retention",
                                                          net_retention_data[last_key])
    else:
        net_retention_score = 1000

    last_key = _last_quarter(gross_retention_data)
    if last_key:
        gross_retention_score = RiskReport.get_score_simple("gross_retention",
                                                            gross_retention_data[last_key])
    else:
        gross_retention_score = 1000

    last_key = _last_quarter(gross_logo_retention)
    if last_key:
        gross_logo_retention_score = RiskReport.get_score_simple("gross_retention", 
                                                                 gross_logo_retention[last_key])
        gross_logo_retention_orig = gross_logo_retention[last_key]
    else:
        gross_logo_retention_score = 1000
        gross_logo_retention_orig = 0

    last_key = _last_quarter(delta_net_run_rate)
    if last_key:
        delta_net_run_rate_score = RiskReport.get_score_simple("delta_net_run_rate",
                                                               delta_net_run_rate[last_key])
    else:
        delta_net_run_rate_score = 0

    last_key = _last_quarter(main_logos_calculated)
    if last_key:
        if last_key in main_logos_yoy:
            yoy_value = main_logos_yoy[last_key]
        else:
            yoy_value = 0
        main_logos_calculated_score = RiskReport.get_score("active_logos_x_growth",
                                                           main_logos_calculated[last_key],
                                                           yoy_value)
    else:
        main_logos_calculated_score = 1000

    last_key = _last_quarter(main_logos_calculated)
    if last_key:
        active_logos_score = RiskReport.get_score_simple("active_logos",
                                                         main_logos_calculated[last_key])
        active_logos_orig = main_logos_calculated[last_key]
    else:
        active_logos_score = 1000
        active_logos_orig = 0

    last_key = _last_quarter(main_logos_yoy)
    if last_key:
        active_logos_yoy_score = RiskReport.get_score_simple("active_logo_growth",
                                                             main_logos_yoy[last_key])
        active_logos_yoy_orig = main_logos_yoy[last_key]
    else:
        active_logos_yoy_score = 1000
        active_logos_yoy_orig = 0

    last_key = _last_quarter(new_logos_calculated)
    if last_key:
        if last_key in new_logos_yoy:
            yoy_value = new_logos_yoy[last_key]
        else:
            yoy_value = 0
        new_logos_score = RiskReport.get_score("new_logos_x_growth",
                                               new_logos_calculated[last_key],
                                               yoy_value)
        new_logos_orig = new_logos_calculated[last_key]
        new_logos_yoy_orig = new_logos_yoy[last_key]
    else:
        new_logos_score = 1000
        new_logos_orig = 0
        new_logos_yoy_orig = 0

    last_key = _last_quarter(txns)
    if last_key:
        txns_score = RiskReport.get_score_simple("txns", txns[last_key])
    else:
        txns_score = 1000

    # Get average score
    group = [run_rate_score, new_logo_score, upsell_arr_score,
             gorss_net_retention_score, net_retention_score,
             gross_retention_score, gross_logo_retention_score,
             delta_net_run_rate_score, main_logos_calculated_score,
             active_logos_score, active_logos_yoy_score, new_logos_score,
             txns_score]
    avg1 = sum(group) / len(group)

    gross_net_impact = avg1 - gorss_net_retention_score
    upsell_growth_impact = avg1 - upsell_arr_score
    gross_logo_retention_impact = avg1 - gross_logo_retention_score

    new_logos_impact = avg1 - new_logos_score
    active_logos_yoy_impact = avg1 - active_logos_yoy_score
    active_logos_impact = avg1 - active_logos_score
    new_logo_impact = avg1 - new_logo_score

    score_table = [
        ['Gross $ Retention', gorss_net_retention_score, gross_net_impact, '{}%'.format(gorss_net_retention_orig_value), None],
        ['Upsell x Growth', upsell_arr_score, upsell_growth_impact, upsell_arr_orig_value, '{}%'.format(upsell_arr_orig_yoy_value)],
        ['Gross Logo Retention', gross_logo_retention_score, gross_logo_retention_impact, gross_logo_retention_orig, None],
        ['New Logos x YoY Growth', new_logos_score, new_logos_impact, new_logos_orig, '{}%'.format(new_logos_yoy_orig)],
        ['Active Logos x YoY Growth', active_logos_yoy_score, active_logos_yoy_impact, active_logos_orig, '{}%'.format(active_logos_yoy_orig)],
        ['Active Logos', active_logos_score, active_logos_impact, active_logos_orig, None],
        ['New Logo Business x Growth', new_logo_score, new_logo_impact, new_logo_orig, new_logo_yoy_orig]
    ]
    return {
        'table': table,
        'score': avg1,
        'score_table': score_table,
        'last_quarter_arr': last_quarter_arr
    }


@job('low', connection=redis_client, timeout=60)
def generate_metrics_report(data, report_id, filters=None):
    """
    Generate Metrics Report with exception handling.
    """
    if isinstance(data, str):
        mode = 'w'
    else:
        mode = 'wb'
    abs_path_filename = mktemp(suffix='.csv')
    with open(abs_path_filename, mode, encoding='utf-8') as fd:
        fd.write(data)

    try:
        obj = MetricsReportHistory.query.get(report_id)
    except NoResultFound:
        logger.warning("No metrics report found in DB for project {}".format(report_id))
        # remove temporary file
        os.unlink(abs_path_filename)
        return None
    finally:
        # remove temporary file
        os.unlink(abs_path_filename)

    try:
        result = generate_metrics_report_data(abs_path_filename, filters=filters)
    except Exception:
        exc_info = sys.exc_info()
        trace = get_readable_traceback(exc_info)
        logger.error("generate_metrics_report: {}".format(trace))

        # Delete object as something's went horribly wrong
        # and we don't need it hanging empty forever in DB
        db.delete(obj)
        # remove temporary file
        os.unlink(abs_path_filename)
        return None
    finally:
        # remove temporary file
        os.unlink(abs_path_filename)

    obj.json = json.dumps(result['table'])
    # Current Market Implied Valuation
    obj.score = result['score']
    obj.score_data = json.dumps(result['score_table'])
    # Current ARR Multiple
    obj.last_quarter_arr = result['last_quarter_arr']
    db.add(obj)
    db.commit()
    db.flush()
    return obj


def generate_magic_numbers_data(csv_path, prev_table):
    """
    Generate magic numbers for Metrics Report from Data Input 2.

    Returns table rows, ready to be appended to Metrics Report table.
    """
    report = MetricsReport(csv_path)

    run_rate_arr = report.run_rate_arr()
    headers = list(run_rate_arr.keys())
    run_rate_arr_q4s = [k.split(' ')[1] for k in run_rate_arr.keys() if k.startswith('Q4')]
    run_rate_arr.update({' ': '',})
    headers.append(' ')
    headers.extend(run_rate_arr_q4s)
    #table = json.loads(metrics_report_obj.json) # [[' '] + list(headers)]

    new_logo_arr_data = report.new_logo_arr()

    upsell_arr_data = report.upsell_arr()

    new_business_arr = report.total_new_business_arr()
    new_logo_arr_quart = report.root(new_logo_arr_data, 'quarter')

    new_logos_calculate = report.logos_calcualte(new_logo_arr_data)
    upsell_arr_quart = report.root(upsell_arr_data, 'quarter')
    upsell_logos_calculated = report.logos_calcualte(upsell_arr_data)

    avg_new_logo = report.avg_new_logo(new_logo_arr_quart, new_logos_calculate)
    avg_upsell = report.avg_upsell(upsell_arr_quart, upsell_logos_calculated)

    new_logos_calculate = report.logos_calcualte(new_logo_arr_data)

    magic_num_no_lag = report.magic_num_no_laq(new_business_arr, run_rate_arr)
    magic_num_1 = report.magic_num(magic_num_no_lag, new_business_arr, 1)
    magic_num_2 = report.magic_num(magic_num_no_lag, new_business_arr, 2)
    magic_num_3 = report.magic_num(magic_num_no_lag, new_business_arr, 3)
    magic_num_4 = report.magic_num(magic_num_no_lag, new_business_arr, 4)

    cac = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 0)
    cac_1 = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 1)
    cac_2 = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 2)
    cac_3 = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 3)
    cac_4 = report.cac(new_logo_arr_quart, magic_num_no_lag, new_logos_calculate, 4)

    avg_new_logo_found = avg_upsell_found = magic_num_no_lag_found = \
        magic_num_1_found = magic_num_2_found = magic_num_3_found = \
        magic_num_4_found = cac_found = cac_1_found = cac_2_found = \
        cac_3_found = cac_4_found = False
    for i in range(len(prev_table)):
        row = prev_table[i]
        if "Avg. New Logo" == row[0]:
            avg_new_logo_found = True
            prev_table[i] = _row("Avg. New Logo", headers, avg_new_logo, sign='$')
        elif "Avg. Upsell" == row[0]:
            avg_upsell_found = True
            prev_table[i] = _row("Avg. Upsell", headers, avg_upsell, sign='%')
        elif "Magic Number No Lag" == row[0]:
            magic_num_no_lag_found = True
            prev_table[i] = _row("Magic Number No Lag", headers, magic_num_no_lag, sign=None)
        elif "Magic Number 1 Qtr" == row[0]:
            magic_num_1_found = True
            prev_table[i] = _row("Magic Number 1 Qtr", headers, magic_num_1, sign=None)
        elif "Magic Number 2 Qtr" == row[0]:
            magic_num_2_found = True
            prev_table[i] = _row("Magic Number 2 Qtr", headers, magic_num_2, sign=None)
        elif "Magic Number 3 Qtr" == row[0]:
            magic_num_3_found = True
            prev_table[i] = _row("Magic Number 3 Qtr", headers, magic_num_3, sign=None)
        elif "Magic Number 4 Qtr" == row[0]:
            magic_num_4_found = True
            prev_table[i] = _row("Magic Number 4 Qtr", headers, magic_num_4, sign=None)
        elif "CAC No Lag" == row[0]:
            cac_found = True
            prev_table[i] = _row("CAC No Lag", headers, cac)
        elif "CAC 1 Qtr" == row[0]:
            cac_1_found = True
            prev_table[i] = _row("CAC 1 Qtr", headers, cac_1)
        elif "CAC 2 Qtr" == row[0]:
            cac_2_found = True
            prev_table[i] = _row("CAC 2 Qtr", headers, cac_2)
        elif "CAC 3 Qtr" == row[0]:
            cac_3_found = True
            prev_table[i] = _row("CAC 3 Qtr", headers, cac_3)
        elif "CAC 4 Qtr" == row[0]:
            cac_4_found = True
            prev_table[i] = _row("CAC 4 Qtr", headers, cac_4)

    if not avg_new_logo_found:
        prev_table.append(_row("Avg. New Logo", headers, avg_new_logo, sign='$'))
    if not avg_upsell_found:
        prev_table.append(_row("Avg. Upsell", headers, avg_upsell, sign='$'))
    if not magic_num_no_lag_found:
        prev_table.append(_row("Magic Number No Lag", headers, magic_num_no_lag, sign=None))
    if not magic_num_1_found:
        prev_table.append(_row("Magic Number 1 Qtr", headers, magic_num_1, sign=None))
    if not magic_num_2_found:
        prev_table.append(_row("Magic Number 2 Qtr", headers, magic_num_2, sign=None))
    if not magic_num_3_found:
        prev_table.append(_row("Magic Number 3 Qtr", headers, magic_num_3, sign=None))
    if not magic_num_4_found:
        prev_table.append(_row("Magic Number 4 Qtr", headers, magic_num_4, sign=None))
    if not cac_found:
        prev_table.append(_row("CAC No Lag", headers, cac))
    if not cac_1_found:
        prev_table.append(_row("CAC 1 Qtr", headers, cac_1))
    if not cac_2_found:
        prev_table.append(_row("CAC 2 Qtr", headers, cac_2))
    if not cac_3_found:
        prev_table.append(_row("CAC 3 Qtr", headers, cac_3))
    if not cac_4_found:
        prev_table.append(_row("CAC 4 Qtr", headers, cac_4))
    return prev_table


@job('low', connection=redis_client, timeout=60)
def generate_magic_numbers(data, report_id):
    """
    Generate magic numbers for Metrics Report from Data Input 2.

    Returns MetricsReportHistory object instance.
    """
    try:
        obj = MetricsReportHistory.query.get(report_id)
    except NoResultFound:
        logger.warning("No metrics report found in DB for project {}".format(report_id))
        return None

    if isinstance(data, str):
        mode = 'w'
    else:
        mode = 'wb'
    abs_path_filename = mktemp(suffix='.csv')
    with open(abs_path_filename, mode, encoding='utf-8') as fd:
        fd.write(data)

    prev_table = json.loads(obj.json)
    try:
        table = generate_magic_numbers_data(abs_path_filename, prev_table)
    except Exception:
        exc_info = sys.exc_info()
        trace = get_readable_traceback(exc_info)
        logger.error("generate_magic_numbers: {}".format(trace))
        # remove temporary file
        os.unlink(abs_path_filename)
        return None
    finally:
        # remove temporary file
        os.unlink(abs_path_filename)

    obj.json = json.dumps(table)
    db.add(obj)
    db.commit()
    db.flush()
    return obj


if __name__ == '__main__':
    fn = "/tmp/tmp5t1bdfdkcsv"
    with open(fn, encoding='utf-8') as fd:
        csv_data = fd.read()
        generate_metrics_report(csv_data, '1')

        generate_magic_numbers(csv_data, '1')
