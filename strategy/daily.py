# This is a main class for one execution.
import numpy as np
import pandas as pd
import datetime
import tstables as tstab
import tables as tb
import datetime as dt

from pandas.io.pytables import Table
from tables.hdf5extension import File
from tstables import TsTable

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)


class TsDesc(tb.IsDescription):
    timestamp = tb.Int64Col(pos=0)
    Last = tb.Float64Col(pos=1)


# class PyDesc(tb.IsDescription):
#     Entry_Date_Time = tb.StringCol(26, pos=1),
#     Entry_Price = tb.Float64Col(pos=2),
#     Exit_Date_Time = tb.StringCol(26, pos=3),
#     Exit_Reason = tb.Int8Col(pos=4),
#     Exit_Price = tb.Float64Col(pos=5),
#     P_L_100t = tb.Float64Col(pos=6),
#     Entry_hour = tb.Int8Col(pos=7),
#     Ticks_to_target = tb.Int8Col(pos=8),
#     Ticks_to_stop = tb.Int8Col(pos=9)


direction = 'long'
entry_reason_time = 'ZEIT'
tick = 0.00001

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
h5_input = File()
ts = TsTable(h5_input, '/', TsDesc)


# h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5')
# py = Table(h5_trades, '/', PyDesc)


def check_file_date_range(entry_hour_i, take_all_dates_from_file,
                          start_year_i, start_month_i, start_day_i, stop_year_i, stop_month_i, stop_day_i):
    a = ts.min_dt()
    b = ts.max_dt()
    # print('The  data in File are from ', a, '  to ', b)
    # print('You want to analyse the data from ', start_year_i, '-', start_month_i, '-', start_day_i, '  to ',
    #      stop_year_i, '-', stop_month_i, '-', stop_day_i)
    if take_all_dates_from_file:
        start_year_i, start_month_i, start_day_i = a.year, a.month, a.day
        stop_year_i, stop_month_i, stop_day_i = b.year, b.month, b.day
        # print('Data range ignored. All data from file taken')
    if dt.datetime(start_year_i, start_month_i, start_day_i).weekday() == 5:
        start_day_i += 1
        # print('Start day changed from saturday to sunday')
    if (dt.datetime(start_year_i, start_month_i, start_day_i).weekday() == 6) & (entry_hour_i != 23):
        start_day_i += 1
        # print('Start day changed from sunday to monday')
    return start_year_i, start_month_i, start_day_i, stop_year_i, stop_month_i, stop_day_i


def open_hdf5_files():
    global h5_input, ts
    h5_input = tb.open_file(path + 'ticks.h5', 'r')
    # h5_input = tb.open_file(path + 'ticks-AUDNZD-2020.h5', 'r')
    ts = h5_input.root.ts._f_get_timeseries()
    # h5_trades = tb.open_file(path + 'trades_AUDNZD.h5')
    # data.info()


def strategy(symbol_in, volume_in, ticks_to_target_in, ticks_to_stop_in,
             entry_hour_in, entry_min_in, entry_sec_in, exit_hour_in, exit_min_in, exit_sec_in, exit_hour_alt_in,
             start_year_in, start_month_in, start_day_in, stop_year_in, stop_month_in, stop_day_in):
    symbol = symbol_in
    volume = volume_in  # TODO depends on symbol e.g. EUR AUD relationship
    ticks_to_target = ticks_to_target_in
    ticks_to_stop = ticks_to_stop_in
    entry_hour, entry_min, entry_sec = entry_hour_in, entry_min_in, entry_sec_in
    exit_hour, exit_min, exit_sec, exit_hour_alt = exit_hour_in, exit_min_in, exit_sec_in, exit_hour_alt_in
    start_year, start_month, start_day = start_year_in, start_month_in, start_day_in
    stop_year, stop_month, stop_day = stop_year_in, stop_month_in, stop_day_in
    print(tick, ticks_to_target, ticks_to_stop, volume, entry_hour)
    # trades = pd.DataFrame(columns=['Symbol', 'Direction', 'Entry_Date_Time', 'Entry_Reason', 'Entry_Price', 'Volume',
    #                                'Exit_Date_Time', 'Exit_Reason', 'Exit_Price', 'P_L_100t'])
    trades = pd.DataFrame(columns=['Entry_Date_Time', 'Entry_Price',
                                   'Exit_Date_Time', 'Exit_Reason', 'Exit_Price', 'P_L_100t'])
    # i = 1

    s_date = dt.date(start_year, start_month, start_day)  # start date
    e_date = dt.date(stop_year, stop_month, stop_day)  # end date
    delta = e_date - s_date  # as timedelta
    # date_list = list()
    date_list = [s_date + dt.timedelta(days=i) for i in range(delta.days + 1)]
    # for i in range(delta.days + 1):
    #     day = s_date + dt.timedelta(days=i)
    #     date_list.append(day)
    d_d_start_date = date_list[0]
    new_rows = list()
    for dl_date in date_list:
        exit_hour_t = exit_hour
        dl_date_next = dl_date + dt.timedelta(days=1)
        if dl_date.weekday() == 5:
            continue
        if (dl_date.weekday() == 6) & (entry_hour != 23):
            continue
        if (dl_date.weekday() == 4) & (entry_hour == 23):
            continue
        if dl_date.weekday() == 4:
            exit_hour_t = exit_hour_alt
            dl_date_next = dl_date
        if dl_date_next > date_list[-1]:
            continue
        read_start_dt = dt.datetime(dl_date.year, dl_date.month, dl_date.day, entry_hour, entry_min, entry_sec)
        read_end_dt = dt.datetime(dl_date_next.year, dl_date_next.month, dl_date_next.day,
                                  exit_hour_t, exit_min, exit_sec)
        # print('____________________________________________________________________________________________________')
        # print(read_start_dt)
        # print(read_end_dt)
        # print(entry_hour)
        # print(ticks_to_target)
        # print('____________________________________________________________________________________________________')
        try:
            date_slice = ts.read_range(read_start_dt, read_end_dt)
        except:
            print(
                '____________________________________________________________________________________________________')
            print('except_except')
            print(
                '____________________________________________________________________________________________________')
            print(read_start_dt)
            print(read_end_dt)
            print(
                '____________________________________________________________________________________________________')
            continue
#        finally:
            # print(
            #     '____________________________________________________________________________________________________')
            # print(
            #     '____________________________________________________________________________________________________')
            # print(read_start_dt)
            # print(read_end_dt)
            # print(entry_hour)
            # print(ticks_to_target)
            # print(
            #     '____________________________________________________________________________________________________')
            # print(
            #     '____________________________________________________________________________________________________')
        # date_slice.info()
        entry_price = date_slice.iloc[0]['Last']
        entry_d_und_t = date_slice.index[0]
        target_price = entry_price + ticks_to_target * tick
        stop_price = entry_price - ticks_to_stop * tick
        date_slice['Sell_target'] = np.where((date_slice['Last'] >= target_price), 'TARGET', '')
        date_slice['Sell_stop'] = np.where((date_slice['Last'] <= stop_price), 'STOP', '')
        row_target = date_slice.loc[date_slice['Sell_target'] == 'TARGET'].head(1)
        row_stop = date_slice.loc[date_slice['Sell_stop'] == 'STOP'].head(1)
        row_s = date_slice.iloc[-2:-1].head(1)
        if not row_stop.empty:
            row_s = row_s.append(row_stop)
        if not row_target.empty:
            row_s = row_s.append(row_target)
        row_s.sort_index(inplace=True)
        exit_d_und_t = row_s.index[0]
        # print(exit_d_und_t)
        exit_reason = row_s.iloc[0]['Sell_target'] + row_s.iloc[0]['Sell_stop']
        if exit_reason == 'STOP':
            exit_reason_int = 2
        if exit_reason == 'TARGET':
            exit_reason_int = 0
        if exit_reason == '' or exit_reason is None:
            exit_reason = 'TIME'
            exit_reason_int = 1
        # print(exit_reason)
        exit_price = row_s.iloc[0]['Last']
        volume_100t = 100000
        p_und_l_100t = round(volume_100t * (exit_price - entry_price), 2)

        # print(date_slice)
        new_row = {'Entry_Date_Time': str(entry_d_und_t),
                   'Entry_Price': entry_price,
                   'Exit_Date_Time': str(exit_d_und_t), 'Exit_Reason': exit_reason, 'Exit_Price': exit_price,
                   'P_L_100t': p_und_l_100t, 'Entry_hour': entry_hour,
                   'Ticks_to_target': ticks_to_target,
                   'Ticks_to_stop': ticks_to_stop}

        # trades_row['Entry_Date_Time'] = entry_d_und_t
        # trades_row['Entry_Price'] = entry_price
        # trades_row['Exit_Date_Time'] = exit_d_und_t
        # trades_row['Exit_Reason'] = exit_reason_int
        # trades_row['Exit_Price'] = exit_price
        # trades_row['P_L_100t'] = p_und_l_100t
        # trades_row['Entry_hour'] = entry_hour
        # trades_row['Ticks_to_target'] = ticks_to_target
        # trades_row['Ticks_to_stop'] = ticks_to_stop
        # print(trades_row['Entry_hour'], trades_row['Ticks_to_target'], )
        # trades_row.append()
        # print(trades_row['Entry_hour'], trades_row['Ticks_to_target'], )

        new_rows.append(new_row)
        # trades = trades.append(new_row, ignore_index=True)
    trades = trades.append(new_rows, ignore_index=True)
    return trades
    # i += 1
    # py.flush()
    # print(trades)
    # path = 'C:/trading_data/' + '{:%Y-%m-%d}'.format(date_list[0]) + '_' + str(entry_hour) + '-' + \
    #        str(entry_min) + '-' + str(entry_sec) + '_' + str(ticks_to_target) + '_' + str(ticks_to_stop) + '_'
    # trades.to_excel(path + 'trades.xlsx')
