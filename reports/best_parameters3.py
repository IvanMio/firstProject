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

start_year, start_month, start_day = 2020, 1, 1
stop_year, stop_month, stop_day = 2020, 12, 31
# ticks_to_stop = 1000

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'


def trades_sums(name):
    entry_hour = name[0]
    ticks_to_target = name[1]
    ticks_to_stop = name[2]
    print(entry_hour)
    print(ticks_to_target)
    time_a = dt.datetime(start_year, start_month, start_day)
    time_b = dt.datetime(stop_year, stop_month, stop_day)
    time_start = '{:%Y-%m-%d}'.format(time_a)
    time_stop = '{:%Y-%m-%d}'.format(time_b)
    # h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5', 'r')
    query = '(Entry_hour == ' + str(entry_hour) + ') & (Ticks_to_target == ' + str(ticks_to_target) + \
            ') & (Ticks_to_stop == ' + str(ticks_to_stop) + ') & ' + '(Entry_Date_Time >= "' + time_start + \
            '") & (Exit_Date_Time <= "' + time_stop + '")'
    # print(query)
    # trades_s = pd.read_hdf(path + 'trades_AUDNZD.h5', 'trades_key', where=query)
    # trades_s = pd.read_hdf(path + 'trades-AUDNZD-20200101120000-20210610112059_1000.h5', 'trades_key', where=query)
    # trades_s = pd.read_hdf(path + 'trades-AUDNZD-20110101120000-20210610094559_1000.h5', 'trades_key', where=query)
    # trades_s = pd.read_hdf(path + 'trades-AUDNZD-20200101120000-20210610112059_range_15_2000_50.h5', 'trades_key', where=query)
    trades_s = pd.read_hdf(path + 'trades-AUDNZD-20110101120000-20210610094559_range_15_2000_50.h5', 'trades_key',
                           where=query)
    # print(trades_s)
    sum_p_l_100t = trades_s['P_L_100t'].sum()
    # print(sum_p_l_100t)
    sum_p_l_plus_100t = trades_s[trades_s['P_L_100t'] > 0]['P_L_100t'].sum()
    # sum_p_l_plus_100t = trades_s['P_L_100t'] > 0
    # sum_p_l_plus_100t = sum_p_l_plus_100t['P_L_100t'].sum()
    # print("----------------", sum_p_l_plus_100t)
    # print(sum_p_l_plus_100t)
    sum_p_l_minus_100t = trades_s[trades_s['P_L_100t'] < 0]['P_L_100t'].sum()
    # sum_p_l_minus_100t = trades_s['P_L_100t'] < 0
    # sum_p_l_minus_100t = sum_p_l_minus_100t['P_L_100t'].sum()
    sum_p_l = (sum_p_l_100t * volume) / 100000
    sum_p_l_plus = (sum_p_l_plus_100t * volume) / 100000
    sum_p_l_minus = (sum_p_l_minus_100t * volume) / 100000
    sum_p_l_tax = round((sum_p_l_plus * 0.75) + sum_p_l_minus, 2)
    entry_date = trades_s.iloc[0]['Entry_Date_Time']
    exit_date = trades_s.iloc[-1]['Entry_Date_Time']
    dty = np.dtype([('Sum_p_l', '<f8'), ('Sum_p_l_tax', '<f8'),
                   ('Entry_Date', 'S10'), ('Exit_Date', 'S10'), ('Entry_hour', '<f8'),
                   ('Ticks_to_target', '<f8'), ('Ticks_to_stop', '<f8')])
    trades_arr = np.array([(sum_p_l, sum_p_l_tax, entry_date, exit_date, entry_hour, ticks_to_target, ticks_to_stop)],
                          dtype=dty)
    return trades_arr


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
    trades_sum_list = pool.map(trades_sums, range_triple_list)
    trades_all = np.concatenate(trades_sum_list)
    print(trades_all)
    print('-----------------------------------------------------------------')
    print(trades_all[0:20])

    print('-----------------------------------------------------------------')
    tr_out = np.sort(trades_all, order='Sum_p_l')
    print(tr_out[0:20])
    reverse_tr_out = tr_out[::-1]
    print('-----------------------------------------------------------------')
    print(reverse_tr_out[0:20])

    print('-----------------------------------------------------------------')
    tr_out = np.sort(trades_all, order='Sum_p_l_tax')
    print(tr_out[0:20])
    reverse_tr_out = tr_out[::-1]
    print('-----------------------------------------------------------------')
    print(reverse_tr_out[0:20])
    h5_trades = tb.open_file(path + 'trades_sums_AUDNZD.h5', 'w')
    rows = 10000
    filters = tb.Filters(complevel=0)
    tab = h5_trades.create_table('/', 'trades_sums', trades_all, title='AUDNZD summary Trades ',
                                 expectedrows=rows, filters=filters)
    h5_trades.close()
