# This is a main class for one execution.
import os

import numpy as np
import pandas as pd
import tables as tb

from daily2 import strategy, open_hdf5_files, check_file_date_range
from multiprocessing import Pool

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
# entry_hour_i = 7
entry_min_i, entry_sec_i = 10, 0
# exit_hour_i = 6
exit_min_i, exit_sec_i = 50, 0
exit_hour_alt_i = 22
start_year_i, start_month_i, start_day_i = 2021, 1, 3
stop_year_i, stop_month_i, stop_day_i = 2021, 4, 20
# ticks_to_target_i = 65
ticks_to_stop_i = 1000
save_i = True
take_all_dates_from_file = True
path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
# h5_trades = tb.open_file(path + 'trades_AUDNZD.h5', 'a')


# print(os.cpu_count())

# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001


def daily_trades(name):
    entry_hour_i = name[0]
    ticks_to_target_i = name[1]
    if entry_hour_i == 0:
        exit_hour_i = 23
    else:
        exit_hour_i = entry_hour_i - 1
    start_year_i_c, start_month_i_c, start_day_i_c, stop_year_i_c, stop_month_i_c, stop_day_i_c = \
        check_file_date_range(entry_hour_i, take_all_dates_from_file, start_year_i, start_month_i, start_day_i,
                              stop_year_i, stop_month_i, stop_day_i)
    trades = strategy(symbol_i, volume_i, ticks_to_target_i, ticks_to_stop_i,
                      entry_hour_i, entry_min_i, entry_sec_i, exit_hour_i, exit_min_i, exit_sec_i,
                      exit_hour_alt_i,
                      start_year_i_c, start_month_i_c, start_day_i_c,
                      stop_year_i_c, stop_month_i_c, stop_day_i_c)
    check_file_date_range(entry_hour_i, take_all_dates_from_file, start_year_i, start_month_i, start_day_i,
                          stop_year_i, stop_month_i, stop_day_i)
    return trades


open_hdf5_files()
a, b, c = 15, 400, 5
target_range = range(a, b, c)
d, e, f = 0, 24, 1
hour_range = range(d, e, f)
range_paar_list = list()
for entry_hour in hour_range:
    for ticks_to_target in target_range:
        range_paar_list.append([entry_hour, ticks_to_target])
if __name__ == '__main__':
    pool = Pool(10)
    trades_list = pool.map(daily_trades, range_paar_list)
    print(len(trades_list))
    trades_all = np.concatenate(trades_list)
    h5_trades = tb.open_file(path + 'trades_AUDNZD.h5', 'w')
    # h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5', 'a')
    # trades_all = pd.concat(trades_list, ignore_index=True, sort=False)
    # trades_all = trades_list[0]
    # for i in range(1, len(trades_list), 1):
    #     print(i)
    #     # trades_all.append(trades_list[i], ignore_index=True, sort=False)
    #     pd.concat((trades_all, trades_list[i]), ignore_index=False, sort=False)
    #     print(trades_all)
    #     print(trades_list[i])
    #     # h5_trades.append('trades_key', trades_list[i], data_columns=True)
    #     # print(h5_trades.keys())
    #     # print(h5_trades.info())
    # h5_trades.append('trades_key', trades_all, data_columns=True)
    rows = 1000000
    filters = tb.Filters(complevel=0)
    tab = h5_trades.create_table('/', 'trades', trades_all, title='AUDNZD all Trades ',
                                 expectedrows=rows, filters=filters)
    h5_trades.close()

