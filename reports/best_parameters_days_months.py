from multiprocessing import Pool
import datetime as dt
import tables as tb

import numpy as np
import openpyxl.utils.units
import pandas as pd

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

symbol = 'AUDNZD'
volume = 56363  # TODO depends on symbol e.g. EUR AUD relationship

start_year, start_month, start_day = 2011, 1, 1
stop_year, stop_month, stop_day = 2020, 12, 31
# ticks_to_stop = 1000

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'


def trades_sums(name):
    entry_hour = name[0]
    ticks_to_target = name[1]
    ticks_to_stop = name[2]
    print(entry_hour, ticks_to_target, ticks_to_stop)
    time_a = dt.datetime(start_year, start_month, start_day)
    time_b = dt.datetime(stop_year, stop_month, stop_day)
    time_start = '{:%Y-%m-%d}'.format(time_a)
    time_stop = '{:%Y-%m-%d}'.format(time_b)
    # h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5', 'r')
    query = '(Entry_hour == ' + str(entry_hour) + ') & (Ticks_to_target == ' + str(ticks_to_target) + \
            ') & (Ticks_to_stop == ' + str(ticks_to_stop) + ') & ' + '(Entry_Date_Time >= "' + time_start + \
            '") & (Exit_Date_Time <= "' + time_stop + '")'

    trades_s = pd.read_hdf(path + 'trades-AUDNZD-20110101120000-20210610094559_range_15_2000_50.h5', 'trades_key',
                           where=query)

    sum_p_l_100t = trades_s['P_L_100t'].sum()
    trades_s['Entry_Date_Time'] = pd.to_datetime(trades_s['Entry_Date_Time'], errors='coerce')
    sum_p_l_100t_weekdays = trades_s['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.day_name()).sum()
    sum_p_l_100t_months = trades_s['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.month_name()).sum()
    sum_p_l_100t_monthdays = trades_s['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.day).sum()
    sum_p_l_100t_years = trades_s['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.year).sum()
    sum_p_l_100t_weekdays.rename('sum_p_l', inplace=True)
    sum_p_l_100t_months.rename('sum_p_l', inplace=True)
    sum_p_l_100t_monthdays.rename('sum_p_l', inplace=True)
    sum_p_l_100t_years.rename('sum_p_l', inplace=True)

    sum_p_l_plus_100t = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].sum()

    sum_p_l_plus_100t_weekdays = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.day_name()).sum()
    sum_p_l_plus_100t_months = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.month_name()).sum()
    sum_p_l_plus_100t_monthdays = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.day).sum()
    sum_p_l_plus_100t_years = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.year).sum()
    sum_p_l_plus_100t_weekdays.rename('P_L_plus_100t', inplace=True)
    sum_p_l_plus_100t_months.rename('P_L_plus_100t', inplace=True)
    sum_p_l_plus_100t_monthdays.rename('P_L_plus_100t', inplace=True)
    sum_p_l_plus_100t_years.rename('P_L_plus_100t', inplace=True)

    sum_p_l_minus_100t = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].sum()
    sum_p_l_minus_100t_weekdays = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].groupby(
        trades_s['Entry_Date_Time'].dt.day_name()).sum()
    sum_p_l_minus_100t_months = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].groupby(
        trades_s['Entry_Date_Time'].dt.month_name()).sum()
    sum_p_l_minus_100t_monthdays = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.day).sum()
    sum_p_l_minus_100t_years = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].groupby(trades_s['Entry_Date_Time'].dt.year).sum()
    sum_p_l_minus_100t_weekdays.rename('P_L_minus_100t', inplace=True)
    sum_p_l_minus_100t_months.rename('P_L_minus_100t', inplace=True)
    sum_p_l_minus_100t_monthdays.rename('P_L_minus_100t', inplace=True)
    sum_p_l_minus_100t_years.rename('P_L_minus_100t', inplace=True)

    sum_p_l_tax_weekdays = count_with_tax(sum_p_l_minus_100t_weekdays, sum_p_l_plus_100t_weekdays)
    sum_p_l_tax_weekdays.rename('sum_p_l_tax', inplace=True)
    sum_p_l_tax_months = count_with_tax(sum_p_l_minus_100t_months, sum_p_l_plus_100t_months)
    sum_p_l_tax_months.rename('sum_p_l_tax', inplace=True)
    sum_p_l_tax_monthdays = count_with_tax(sum_p_l_minus_100t_monthdays, sum_p_l_plus_100t_monthdays)
    sum_p_l_tax_monthdays.rename('sum_p_l_tax', inplace=True)
    sum_p_l_tax_years = count_with_tax(sum_p_l_minus_100t_years, sum_p_l_plus_100t_years)
    sum_p_l_tax_years.rename('sum_p_l_tax', inplace=True)

    sum_p_l_weekdays = round((sum_p_l_100t_weekdays * volume) / 100000, 2)
    sum_p_l_months = round((sum_p_l_100t_months * volume) / 100000, 2)
    sum_p_l_monthdays = round((sum_p_l_100t_monthdays * volume) / 100000, 2)
    sum_p_l_years = round((sum_p_l_100t_years * volume) / 100000, 2)

    sum_p_l = round((sum_p_l_100t * volume) / 100000, 2)
    sum_p_l_tax = count_with_tax(sum_p_l_minus_100t, sum_p_l_plus_100t)
    entry_date = trades_s.iloc[0]['Entry_Date_Time']
    exit_date = trades_s.iloc[-1]['Entry_Date_Time']

    sum_p_l_plus = round((sum_p_l_plus_100t * volume) / 100000, 2)
    sum_p_l_plus_weekdays = round((sum_p_l_plus_100t_weekdays * volume) / 100000, 2)
    sum_p_l_plus_months = round((sum_p_l_plus_100t_months * volume) / 100000, 2)
    sum_p_l_plus_monthdays = round((sum_p_l_plus_100t_monthdays * volume) / 100000, 2)
    sum_p_l_plus_years = round((sum_p_l_plus_100t_years * volume) / 100000, 2)

    dty = np.dtype([('Sum_p_l', '<f8'), ('Sum_p_l_plus', '<f8'), ('Sum_p_l_tax', '<f8'),
                   ('Entry_Date', 'S10'), ('Exit_Date', 'S10'), ('Entry_hour', '<f8'),
                   ('Ticks_to_target', '<f8'), ('Ticks_to_stop', '<f8')])
    trades_arr = np.array([(sum_p_l, sum_p_l_plus, sum_p_l_tax, entry_date, exit_date, entry_hour, ticks_to_target,
                            ticks_to_stop)],
                          dtype=dty)

    sums_weekdays = pd.DataFrame()
    sums_weekdays['Sum_p_l'] = sum_p_l_weekdays
    sums_weekdays['Sum_p_l_plus'] = sum_p_l_plus_weekdays
    sums_weekdays['Sum_p_l_tax'] = sum_p_l_tax_weekdays
    sums_weekdays['Entry_Date'] = entry_date
    sums_weekdays['Exit_Date'] = exit_date
    sums_weekdays['Entry_hour'] = entry_hour
    sums_weekdays['Ticks_to_target'] = ticks_to_target
    sums_weekdays['Ticks_to_stop'] = ticks_to_stop

    sums_months = pd.DataFrame()
    sums_months['Sum_p_l'] = sum_p_l_months
    sums_months['Sum_p_l_plus'] = sum_p_l_plus_months
    sums_months['Sum_p_l_tax'] = sum_p_l_tax_months
    sums_months['Entry_Date'] = entry_date
    sums_months['Exit_Date'] = exit_date
    sums_months['Entry_hour'] = entry_hour
    sums_months['Ticks_to_target'] = ticks_to_target
    sums_months['Ticks_to_stop'] = ticks_to_stop

    sums_monthdays = pd.DataFrame()
    sums_monthdays['Sum_p_l'] = sum_p_l_monthdays
    sums_monthdays['Sum_p_l_plus'] = sum_p_l_plus_monthdays
    sums_monthdays['Sum_p_l_tax'] = sum_p_l_tax_monthdays
    sums_monthdays['Entry_Date'] = entry_date
    sums_monthdays['Exit_Date'] = exit_date
    sums_monthdays['Entry_hour'] = entry_hour
    sums_monthdays['Ticks_to_target'] = ticks_to_target
    sums_monthdays['Ticks_to_stop'] = ticks_to_stop

    sums_years = pd.DataFrame()
    sums_years['Sum_p_l'] = sum_p_l_years
    sums_years['Sum_p_l_plus'] = sum_p_l_plus_years
    sums_years['Sum_p_l_tax'] = sum_p_l_tax_years
    sums_years['Entry_Date'] = entry_date
    sums_years['Exit_Date'] = exit_date
    sums_years['Entry_hour'] = entry_hour
    sums_years['Ticks_to_target'] = ticks_to_target
    sums_years['Ticks_to_stop'] = ticks_to_stop
    return_list = [sums_weekdays, sums_monthdays, sums_months, sums_years, trades_arr]
    return return_list


def count_with_tax(sum_p_l_minus_100t, sum_p_l_plus_100t):
    sum_p_l_plus = (sum_p_l_plus_100t * volume) / 100000
    sum_p_l_minus = (sum_p_l_minus_100t * volume) / 100000
    sum_p_l_tax = round((sum_p_l_plus * 0.75) + sum_p_l_minus, 2)
    return sum_p_l_tax


# a, b, c = 15, 400, 5
# target_range = range(a, b, c)
target_range = [15, 65, 115, 165, 215, 265, 315, 365, 415]
stop_range = [15, 65, 115, 165, 215, 265, 315, 365, 415, 725, 1000, 1500, 2000]
d, e, f = 0, 24, 1
hour_range = range(d, e, f)
range_triple_list = list()
for entry_hour_in in hour_range:
    for ticks_to_target_in in target_range:
        for ticks_to_stop_in in stop_range:
            range_triple_list.append([entry_hour_in, ticks_to_target_in, ticks_to_stop_in])
# print(range_paar_list)
if __name__ == '__main__':
    pool = Pool(10)
    result_list = pool.map(trades_sums, range_triple_list)
    weekdays = list()
    monthdays = list()
    months = list()
    years = list()
    trades_sums = list()
#    for list_element in range(0, len(result_list), 1):
    for list_element in result_list:
        weekdays.append(list_element[0])
        monthdays.append(list_element[1])
        months.append(list_element[2])
        years.append(list_element[3])
        trades_sums.append(list_element[4])

    weekdays_all = pd.concat(weekdays, sort=False)
    monthdays_all = pd.concat(monthdays, sort=False)
    months_all = pd.concat(months, sort=False)
    years_all = pd.concat(years, sort=False)
    trades_all = np.concatenate(trades_sums)
    print(trades_all)
    print('-----------------------------------------------------------------')
    print(trades_all[0:20])

    print(weekdays_all)
    print(monthdays_all)
    print(months_all)
    print(trades_all)

    print('-----------------------------------------------------------------')
    tr_sum_p_l = np.sort(trades_all, order='Sum_p_l')
    print(tr_sum_p_l[0:20])
    reverse_tr_sum_p_l = tr_sum_p_l[::-1]
    print('-----------------------------------------------------------------')
    print(reverse_tr_sum_p_l[0:20])

    h5_trades = tb.open_file(path + 'trades_sums_2011_2020_AUDNZD.h5', 'w')
    rows = 10000
    filters = tb.Filters(complevel=0)
    tab = h5_trades.create_table('/', 'trades_sums', reverse_tr_sum_p_l, title='AUDNZD summary Trades ',
                                 expectedrows=rows, filters=filters)
    df_trades = pd.DataFrame(reverse_tr_sum_p_l)
    df_trades.to_excel(path + 'trades_sums_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    tr_sum_p_l_tax = np.sort(trades_all, order='Sum_p_l_tax')
    print(tr_sum_p_l_tax[0:20])
    reverse_tr_sum_p_l_tax = tr_sum_p_l_tax[::-1]
    print('-----------------------------------------------------------------')
    print(reverse_tr_sum_p_l_tax[0:20])

    h5_trades_tax = tb.open_file(path + 'trades_sums_tax_2011_2020_AUDNZD.h5', 'w')
    rows = 10000
    filters = tb.Filters(complevel=0)
    tab = h5_trades_tax.create_table('/', 'trades_sums', reverse_tr_sum_p_l_tax, title='AUDNZD summary Trades ',
                                     expectedrows=rows, filters=filters)
    df_trades_tax = pd.DataFrame(reverse_tr_sum_p_l_tax)
    df_trades_tax.to_excel(path + 'trades_sums_tax_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    tr_sum_p_l_plus = np.sort(trades_all, order='Sum_p_l_plus')
    print(tr_sum_p_l_plus[0:20])
    reverse_tr_sum_p_l_plus = tr_sum_p_l_plus[::-1]
    print('-----------------------------------------------------------------')
    print(reverse_tr_sum_p_l_plus[0:20])

    h5_trades_plus = tb.open_file(path + 'trades_sums_plus_2011_2020_AUDNZD.h5', 'w')
    rows = 10000
    filters = tb.Filters(complevel=0)
    tab = h5_trades_plus.create_table('/', 'trades_sums', reverse_tr_sum_p_l_plus, title='AUDNZD summary Trades ',
                                      expectedrows=rows, filters=filters)
    df_trades_plus = pd.DataFrame(reverse_tr_sum_p_l_plus)
    df_trades_plus.to_excel(path + 'trades_sums_plus_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    weekdays_asc = weekdays_all.sort_values(by=['Sum_p_l_tax'])
    print(weekdays_asc.head(20))
    weekdays_desc = weekdays_all.sort_values(by=['Sum_p_l_tax'], ascending=False)
    print('-----------------------------------------------------------------')
    print(weekdays_desc.head(20))
    weekdays_desc.to_excel(path + 'weekdays_tax_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    monthdays_asc = monthdays_all.sort_values(by=['Sum_p_l_tax'])
    print(monthdays_asc.head(20))
    monthdays_desc = monthdays_all.sort_values(by=['Sum_p_l_tax'], ascending=False)
    print('-----------------------------------------------------------------')
    print(monthdays_desc.head(20))
    monthdays_desc.to_excel(path + 'monthdays_tax_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    months_asc = months_all.sort_values(by=['Sum_p_l_tax'])
    print(months_asc.head(20))
    months_desc = months_all.sort_values(by=['Sum_p_l_tax'], ascending=False)
    print('-----------------------------------------------------------------')
    print(months_desc.head(20))
    months_desc.to_excel(path + 'months_tax_2011_2020_AUDNZD.xlsx')

    print('-----------------------------------------------------------------')
    years_asc = years_all.sort_values(by=['Sum_p_l_tax'])
    print(years_asc.head(20))
    years_desc = years_all.sort_values(by=['Sum_p_l_tax'], ascending=False)
    print('-----------------------------------------------------------------')
    print(years_desc.head(20))
    years_desc.to_excel(path + 'years_tax_2011_2020_AUDNZD.xlsx')


    h5_trades.close()
