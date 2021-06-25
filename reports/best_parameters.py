from multiprocessing import Pool
import datetime as dt
import tables as tb

import numpy as np
import openpyxl.utils.units
import pandas as pd

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship

start_year_i, start_month_i, start_day_i = 2021, 1, 3
stop_year_i, stop_month_i, stop_day_i = 2021, 4, 20
ticks_to_stop_i = 1000

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'


def strategy(symbol_in, volume_in, ticks_to_target_c, ticks_to_stop_in,
             entry_hour_c,
             start_year_in, start_month_in, start_day_in, stop_year_in, stop_month_in, stop_day_in):
    symbol = symbol_in
    volume = volume_in  # TODO depends on symbol e.g. EUR AUD relationship
    ticks_to_target = ticks_to_target_c
    ticks_to_stop = ticks_to_stop_in
    entry_hour = entry_hour_c
    start_year, start_month, start_day = start_year_in, start_month_in, start_day_in
    stop_year, stop_month, stop_day = stop_year_in, stop_month_in, stop_day_in
    time_a = dt.datetime(start_year, start_month, start_day)
    time_b = dt.datetime(stop_year, stop_month, stop_day)
    time_start = '{:%Y-%m-%d}'.format(time_a)
    time_stop = '{:%Y-%m-%d}'.format(time_b)
    # h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5', 'r')
    query = '(Entry_hour == ' + str(a) + ') & (Ticks_to_target == ' + str(
        b) + ') & ' + '(Entry_Date_Time >= "' + time_start + '") & (Exit_Date_Time <= "' + time_stop + '")'
    # print(query)
    trades_s = pd.read_hdf(path + 'trades_AUDNZD.h5', 'trades_key', where=query)
    print(trades_s)
    sum_p_l_100t = trades_s['P_L_100t'].sum()
    print(sum_p_l_100t)
    sum_p_l_plus_100t = (trades_s['P_L_100t'] > 0).sum()
    print(sum_p_l_plus_100t)
    sum_p_l_minus_100t = (trades_s['P_L_100t'] < 0).sum()
    sum_p_l = (sum_p_l_100t * volume) / 100000
    sum_p_l_plus = (sum_p_l_plus_100t * volume) / 100000
    sum_p_l_minus = (sum_p_l_minus_100t * volume) / 100000
    sum_p_l_tax = round((sum_p_l_plus * 0.75) + sum_p_l_minus, 2)
    entry_date = trades_s.iloc[0]['Entry_Date_Time']
    exit_date = trades_s.iloc[-1]['Entry_Date_Time']
    dty = np.dtype([('Sum_p_l', '<f8'), ('Sum_p_l_tax', '<f8'),
                   ('Entry_Date', 'S10'), ('Exit_Date', 'S10'), ('Entry_hour', '<f8'),
                   ('Ticks_to_target', '<f8'), ('Ticks_to_stop', '<f8')])
    trades_arr = ((sum_p_l, sum_p_l_tax, entry_date, exit_date, entry_hour, ticks_to_target, ticks_to_stop), dty)
    return trades_arr


def trades_sums(name):
    entry_hour_i = name[0]
    ticks_to_target_i = name[1]
    print(entry_hour_i)
    print(ticks_to_target_i)
    trades_sum = strategy(symbol_i, volume_i, ticks_to_target_i, ticks_to_stop_i,
                          entry_hour_i,
                          start_year_i, start_month_i, start_day_i,
                          stop_year_i, stop_month_i, stop_day_i)
    return trades_sum


a, b, c = 15, 395, 5
target_range = range(a, b, c)
d, e, f = 0, 24, 1
hour_range = range(d, e, f)
range_paar_list = list()
for entry_hour_in in hour_range:
    for ticks_to_target_in in target_range:
        range_paar_list.append([entry_hour_in, ticks_to_target_in])
print(range_paar_list)
if __name__ == '__main__':
    pool = Pool(1)
    trades_sum_list = pool.map(trades_sums, range_paar_list)
    trades_all = np.concatenate(trades_sum_list)
    h5_trades = tb.open_file(path + 'trades_sums_AUDNZD.h5', 'w')
    rows = 10000
    filters = tb.Filters(complevel=0)
    tab = h5_trades.create_table('/', 'trades_sums', trades_all, title='AUDNZD summary Trades ',
                                 expectedrows=rows, filters=filters)
    h5_trades.close()
