# This is a main class for one execution.
import os

import numpy as np
import pandas as pd

from currency import Strategy3
from currency.Strategy3 import strategy, read_file

symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_time_i = '03:10:00'
exit_time_i = '02:50:00'
ticks_to_target_i = 65
ticks_to_stop_i = 1000
save_i = True

# print(os.cpu_count())

# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001

analyse_errors = pd.DataFrame()
analyse_trades = pd.DataFrame()
errors_return = pd.DataFrame()
trades_return = pd.DataFrame()
read_file(entry_time_i)
for ticks_to_target_i in range(15, 400, 5):
    errors_return, trades_return = strategy(symbol_i, volume_i, entry_time_i, exit_time_i, ticks_to_target_i, ticks_to_stop_i, save_i)
    # print(errors_return)
    # print(trades_return)
    # print(errors_return.iloc[0])
    # print(trades_return.iloc[0])
    analyse_errors = analyse_errors.append(errors_return.iloc[0], ignore_index=True)
    analyse_errors = analyse_errors.append(errors_return.iloc[1], ignore_index=True)
    if len(errors_return.index) > 2:
        analyse_errors = analyse_errors.append(errors_return.iloc[2], ignore_index=True)
    analyse_trades = analyse_trades.append(trades_return.iloc[0], ignore_index=True)
print(analyse_errors)
print(analyse_trades)
path = 'C:/trading_data/'
analyse_trades.to_excel(path + 'trades_summary.xlsx')
analyse_errors.to_excel(path + 'errors_summary.xlsx')