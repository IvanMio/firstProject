# This is a main class for daily strategy analysis
import os
from datetime import datetime

import numpy as np
import pandas as pd


data = pd.DataFrame()
tdb = np.timedelta64

i = 1
j = 1
k = 1
m = 1
plus_trade_cum = 0
minus_trade_cum = 0
plus_trade_cum_proc = 0
minus_trade_cum_proc = 0
plus_trade_cum_amount = 0.0
minus_trade_cum_amount = 0.0
biggest_cum_amount = 0.0
dropdown_sequence_amount = 0.0
dropdown_biggest_amount = 0.0
d_d_max_delta = np.timedelta64(0, 'D')
d_d_sequence_delta = np.timedelta64
biggest = False

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)


# TODO should I use numpy types here and/or make it simpler?
def string_to_time(from_string):
    from_string = datetime.datetime.strptime(from_string, '%H:%M:%S')
    from_string = datetime.time(from_string.hour, from_string.minute, from_string.second)
    return from_string


def time_deltas(entry_time):
    global tdb
    # timedelta (TODO should be changed to work with time zone)
    entry_hour = int(entry_time[0:2])
    if entry_hour == 23:
        print(entry_hour)
        hours_delta = 1
    else:
        print(entry_hour)
        hours_delta = - entry_hour
    tda = datetime.timedelta(hours=hours_delta)
    tdb = pd.to_timedelta(hours_delta, unit='h')
    return entry_hour, tda, tdb


def read_file(entry_time_in):
    global tdb, data
    data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
    # self.data = pd.read_csv('C:/ticks-AUDNZD-Jahr.csv', index_col=0, parse_dates=True, decimal=',')
    data = pd.DataFrame(data['Last'], dtype=float)
    data.dropna(inplace=True)
    # data.info()
    entry_hour, tda, tdb = time_deltas(entry_time_in)
    data.index += tdb

direction = 'long'
entry_reason_time = 'ZEIT'
tick = 0.00001

symbol = 'AUDNZD'
volume = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_time = '02:10:00'
exit_time = '01:50:00'
ticks_to_target = 65
ticks_to_stop = 1000
save = True

read_file(entry_time)
for ticks_to_target_i in range(15, 400, 5):
    print(tick, ticks_to_target, ticks_to_stop, volume)
    cumulative_profit = 0.0
    entry_price = np.float64  # placeholder: to be filled after entry.  TODO do I need this definition?

    entry_hour, tda, tdb = time_deltas(entry_time)
    # Result for one variant, depending on entry_time, exit time, ticks to target, exit time and ticks to stop
    trades = pd.DataFrame(columns=['Symbol', 'Direction', 'Entry_Date&Time', 'Entry_Reason', 'Entry_Price', 'Volume',
                                   'Exit_Date&Time', 'Exit_Reason', 'Exit_Price', 'P&L', 'Cum P&L', '+T', '-T',
                                   '%Proc+T', '%Proc-T', 'Cum +P&L', 'Cum -P&L', 'Cum P&L-Tax', 'Max D-D Amount',
                                   'Max D-D Time', '%Proc Max D-D'])
    errors = pd.DataFrame(columns=['Date', 'ErrNr', 'ErrType'])
    entry_time = string_to_time(entry_time)
    exit_time = string_to_time(exit_time)
    entry_time = (datetime.datetime.combine(datetime.date.today(), entry_time) + tda).time()
    exit_time = (datetime.datetime.combine(datetime.date.today(), exit_time) + tda).time()
    print(entry_time)
    print(exit_time)
    # TODO If I could integrate those 2 lines I don't need an additional column in dataframe
    # data['Date'] = DatetimeIndex(data.index).date
    data['Date'] = data.index.date
    date_list = data['Date'].unique().tolist()
    print(date_list)
    # time_t = DatetimeIndex(data.index).time
    # data['Position'] = np.where((time_t > entry_time) & (time_t < exit_time), 1, 0)
    data['Position'] = np.where((data.index.time > entry_time) & (data.index.time < exit_time), 1, 0)
    data['Buy'] = np.where((data['Position'] == 1) & (data['Position'].shift(1) == 0), 1, 0)
    data['Sell'] = np.where((data['Position'] == 0) & (data['Position'].shift(1) == 1), 'VRIME', '')
    data.info()

    d_d_start_date = date_list[0]
    for dl_date in date_list:
