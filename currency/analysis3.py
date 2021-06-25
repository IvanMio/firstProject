# This is a main class for one execution.
import os

import numpy as np
import pandas as pd

from currency import Strategy3
from currency.Strategy3 import strategy, read_file
from multiprocessing import Pool, Queue

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_time_i = '10:10:00'
exit_time_i = '09:50:00'
# ticks_to_target_i = 65
ticks_to_stop_i = 1000
save_i = True

# print(os.cpu_count())

# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001


read_file(entry_time_i)


def daily_trades(name):
    errors_return, trades_return = strategy(symbol_i, volume_i, entry_time_i, exit_time_i, name,
                                            ticks_to_stop_i, save_i)
    return errors_return, trades_return


a, b, c = 15, 400, 5
target_range = range(a, b, c)
if __name__ == '__main__':
    analyse_errors = pd.DataFrame()
    analyse_trades = pd.DataFrame()
    pool = Pool(6)
    trades = pool.map(daily_trades, target_range)
    for i in target_range:
        j = int(i/c - a/c)
        print(j, len(trades))
        analyse_trades = analyse_trades.append(trades[j][1], ignore_index=True)
        analyse_errors = analyse_errors.append(trades[j][0], ignore_index=True)

    print(analyse_errors)
    print(analyse_trades)
    path = 'C:/trading_data/'
    analyse_trades.to_excel(path + 'trades_summary.xlsx')
    analyse_errors.to_excel(path + 'errors_summary.xlsx')