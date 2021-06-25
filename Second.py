# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import numpy as np
import pandas as pd
import datetime
import matplotlib as plt
from matplotlib import pyplot as mpl

cumulative_profit = 0.0
tick = 0.00001
ticks_to_target = 75
ticks_to_stop = 1000
volume = 96000
entry_time = '23:10:00'
entry_price = 0.0  # placeholder: to be filled after entry.
exit_time_start_range = '22:50:00'
exit_time_end_range = '22:59:00'


def string_to_time(from_string):

    from_string = datetime.datetime.strptime(from_string, '%H:%M:%S')
    from_string = datetime.time(from_string.hour, from_string.minute, from_string.second)
    return from_string


entry_time = string_to_time(entry_time)
exit_time_start_range = string_to_time(exit_time_start_range)
exit_time_end_range = string_to_time(exit_time_end_range)


def target_price(entry_price, ticks_to_target, tick):
    return entry_price + (ticks_to_target * tick)


def stop_price(entry_price, ticks_to_stop, tick):
    return entry_price - (ticks_to_stop * tick)


def timestamp_index_to_time(timestamp):
    timestamp = datetime.datetime.strptime(timestamp, '%H:%M:%S')
    timestamp = datetime.time(timestamp.hour, timestamp.minute, timestamp.second)
    return timestamp


# targetPrice = targetPrice(entryPrice, ticksToTarget, tick)
# plt.style.use('seaborn')
# mpl.rcParams['font.family'] = 'serif'
data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
data = pd.DataFrame(data['Last'], dtype=float)
data.dropna(inplace=True)
# data.info()
# print(data.tail())
# print(data.head())
data['Time'] = data.index.time
# data['Position'] = np.where(data['Time'] > entry_time, 1, 0)
data['Position'] = np.where((data.index.time > entry_time) | (data.index.time < exit_time_start_range), 1, 0)
data['Buy'] = np.where((data['Position'] == 1) & (data['Position'].shift(1) == 0), 1, 0)
# global i
i = 1
buy_date = []
for row in data[data['Buy'] == 1].itertuples():
    # row['Buy'] = i
    print(row)
    # row[4] = i
    data.loc[row[0], 'Buy'] = i
    buy_date.append(row[0])
    print()
    print(row[4])
    if i > 1:
        # entry_price = row[1]
        dd = data[(data.index > buy_date[i-2]) & (data.index < buy_date[i-1]) & (data['Position'] == 1)].copy()
        # ddd = dd.loc[:'Last', 'Position', 'Buy']
        dd['Position'] = i-1
        print(dd.tail())
    i = i + 1
    print(i)
    # dd = data[(data.index > buy_date[i-2]) & (data.index < buy_date[i-1]) & (data['Position'] == 1)].copy()
    # dd['Position'] = i-1
    # print(dd.tail())

print(data[data['Buy'] > 0])
print(data.tail())

# !!data['BS Counter'] = np.where((data['Position'] == 1) & (data['Position'].shift(1) == 0), 1, 0)
# data['Buy signal'] = np.where((data.index.time > entry_time) & (data.index.shift(1).time <= entry_time), 1, 0)
# data['Position'] = np.where(data['Timestamp'] > entryTime, 1, 0)
# data['rets'] = np.log(data / data.shift(1))
# data['vola'] = data['rets']
# data['vola'] = data['rets'].rolling(252).std() * np.sqrt(252)
# data[['Last', 'vola']].plot(subplots=True, figsize=(10, 6));
# mpl.show()
# data.info()
print(data.head(180))



