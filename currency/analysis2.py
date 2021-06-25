# This is a main class for one execution.
import os

import numpy as np
import pandas as pd

from currency import Strategy3
from currency.Strategy3 import strategy, read_file
from multiprocessing import Pool, Queue

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_time_i = '04:10:00'
exit_time_i = '03:50:00'
# ticks_to_target_i = 65
ticks_to_stop_i = 1000
save_i = True

# print(os.cpu_count())

# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001


read_file(entry_time_i)


def daily_trades(name, trades_results):
    errors_return, trades_return = strategy(symbol_i, volume_i, entry_time_i, exit_time_i, name,
                                            ticks_to_stop_i, save_i)
    # errors_results.put(errors_return)
    trades_results.put(trades_return)


# for ticks_to_target in range(15, 400, 5):
#     daily_trades(ticks_to_target)
target_range = range(15, 400, 5)
if __name__ == '__main__':
    analyse_errors = pd.DataFrame()
    analyse_trades = pd.DataFrame()
    pool = Pool(7)
    # myErrors= Queue()
    myTrades = Queue()
    pool.map(daily_trades, target_range,  myTrades)
    for i in target_range:
        # errors_return = myErrors.get()
        trades_return = myTrades.get()
        # analyse_errors = analyse_errors.append(errors_return.iloc[0], ignore_index=True)
        # analyse_errors = analyse_errors.append(errors_return.iloc[1], ignore_index=True)
        # if len(errors_return.index) > 2:
        #     analyse_errors = analyse_errors.append(errors_return.iloc[2], ignore_index=True)
        analyse_trades = analyse_trades.append(trades_return.iloc[0], ignore_index=True)

#     print('_____________________________________________________________________________________________________')
# with Pool(6) as p:
#     print('main line')
#     p.map(process_a_day, date_list)

#     # print(errors_return)
#     # print(trades_return)
#     # print(errors_return.iloc[0])
#     # print(trades_return.iloc[0])
#     analyse_errors = analyse_errors.append(errors_return.iloc[0], ignore_index=True)
#     analyse_errors = analyse_errors.append(errors_return.iloc[1], ignore_index=True)
#     if len(errors_return.index) > 2:
#         analyse_errors = analyse_errors.append(errors_return.iloc[2], ignore_index=True)
#     analyse_trades = analyse_trades.append(trades_return.iloc[0], ignore_index=True)
    pool.close()
    pool.join()

    print(analyse_errors)
    print(analyse_trades)
    path = 'C:/trading_data/'
    analyse_trades.to_excel(path + 'trades_summary.xlsx')
    analyse_errors.to_excel(path + 'errors_summary.xlsx')