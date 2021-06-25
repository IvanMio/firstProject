# This is a main class for one execution.
import os

import numpy as np
import pandas as pd

from currency.strategy5 import strategy, open_hdf5_file, check_file_date_range
from multiprocessing import Pool

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_hour_i, entry_min_i, entry_sec_i = 7, 10, 0
exit_hour_i, exit_min_i, exit_sec_i = 6, 50, 0
exit_hour_alt_i = 22
start_year_i, start_month_i, start_day_i = 2021, 1, 3
stop_year_i, stop_month_i, stop_day_i = 2021, 4, 20
# ticks_to_target_i = 65
ticks_to_stop_i = 1000
save_i = True
take_all_dates_from_file = True

# print(os.cpu_count())

# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001


open_hdf5_file()
start_year_i_c, start_month_i_c, start_day_i_c, stop_year_i_c, stop_month_i_c, stop_day_i_c = \
    check_file_date_range(entry_hour_i, take_all_dates_from_file, start_year_i, start_month_i, start_day_i,
                          stop_year_i, stop_month_i, stop_day_i)


def daily_trades(name):
    errors_return, trades_return = strategy(symbol_i, volume_i, name, ticks_to_stop_i, save_i,
                                            entry_hour_i, entry_min_i, entry_sec_i, exit_hour_i, exit_min_i, exit_sec_i,
                                            exit_hour_alt_i,
                                            start_year_i_c, start_month_i_c, start_day_i_c,
                                            stop_year_i_c, stop_month_i_c, stop_day_i_c)
    return errors_return, trades_return


a, b, c = 15, 400, 5
target_range = range(a, b, c)
if __name__ == '__main__':
    analyse_errors = pd.DataFrame()
    analyse_trades = pd.DataFrame()
    pool = Pool(10)
    trades = pool.map(daily_trades, target_range)
    for i in target_range:
        j = int(i / c - a / c)
        print(j, len(trades))
        analyse_trades = analyse_trades.append(trades[j][1], ignore_index=True)
        analyse_errors = analyse_errors.append(trades[j][0], ignore_index=True)

    print(analyse_errors)
    print(analyse_trades)
    path = 'C:/trading_data/'
    analyse_trades.to_excel(path + 'trades_summary.xlsx')
    analyse_errors.to_excel(path + 'errors_summary.xlsx')

check_file_date_range(entry_hour_i, take_all_dates_from_file, start_year_i, start_month_i, start_day_i,
                      stop_year_i, stop_month_i, stop_day_i)
