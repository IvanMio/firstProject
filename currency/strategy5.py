# This is a main class for one execution.
import numpy as np
import pandas as pd
import datetime
import tstables as tstab
import tables as tb
import datetime as dt

from tables.hdf5extension import File
from tstables import TsTable

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)


class TsDesc(tb.IsDescription):
    timestamp = tb.Int64Col(pos=0)
    Last = tb.Float64Col(pos=1)


direction = 'long'
entry_reason_time = 'ZEIT'
tick = 0.00001

h5 = File()
ts = TsTable(h5, '/', TsDesc)


def check_file_date_range(entry_hour_i, take_all_dates_from_file,
                          start_year_i, start_month_i, start_day_i, stop_year_i, stop_month_i,  stop_day_i):
    a = ts.min_dt()
    b = ts.max_dt()
    print('The  data in File are from ', a, '  to ', b)
    print('You want to analyse the data from ', start_year_i, '-', start_month_i, '-', start_day_i, '  to ',
          stop_year_i, '-', stop_month_i, '-', stop_day_i)
    if take_all_dates_from_file:
        start_year_i, start_month_i, start_day_i = a.year, a.month, a.day
        stop_year_i, stop_month_i, stop_day_i = b.year, b.month, b.day
        print('Data range ignored. All data from file taken')
    if dt.datetime(start_year_i, start_month_i, start_day_i).weekday() == 5:
        start_day_i += 1
        print('Start day changed from saturday to sunday')
    if (dt.datetime(start_year_i, start_month_i, start_day_i).weekday() == 6) & (entry_hour_i != 23):
        start_day_i += 1
        print('Start day changed from sunday to monday')
    return start_year_i, start_month_i, start_day_i, stop_year_i, stop_month_i, stop_day_i


def open_hdf5_file():
    global h5, ts
    # h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks.h5', 'r')
    h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks-AUDNZD-2020.h5', 'r')
    ts = h5.root.ts._f_get_timeseries()
    # data.info()


def strategy(symbol_in, volume_in, ticks_to_target_in, ticks_to_stop_in, save_in,
             entry_hour_in, entry_min_in, entry_sec_in, exit_hour_in, exit_min_in, exit_sec_in, exit_hour_alt_in,
             start_year_in, start_month_in, start_day_in, stop_year_in, stop_month_in, stop_day_in):
    symbol = symbol_in
    volume = volume_in  # TODO depends on symbol e.g. EUR AUD relationship
    ticks_to_target = ticks_to_target_in
    ticks_to_stop = ticks_to_stop_in
    save = save_in
    entry_hour, entry_min, entry_sec = entry_hour_in, entry_min_in, entry_sec_in
    exit_hour, exit_min, exit_sec, exit_hour_alt = exit_hour_in, exit_min_in, exit_sec_in, exit_hour_alt_in
    start_year, start_month, start_day = start_year_in, start_month_in, start_day_in
    stop_year, stop_month, stop_day = stop_year_in, stop_month_in, stop_day_in
    print(tick, ticks_to_target, ticks_to_stop, volume)
    trades = pd.DataFrame(columns=['Symbol', 'Direction', 'Entry_Date&Time', 'Entry_Reason', 'Entry_Price', 'Volume',
                                   'Exit_Date&Time', 'Exit_Reason', 'Exit_Price', 'P&L', 'Cum P&L', '+T', '-T',
                                   '%Proc+T', '%Proc-T', 'Cum +P&L', 'Cum -P&L', 'Cum P&L-Tax', 'Max D-D Amount',
                                   'Max D-D Time', '%Proc Max D-D'])
    errors = pd.DataFrame(columns=['Date', 'ErrNr', 'ErrType'])
    i = 1
    cumulative_profit = 0.0
    plus_trade_cum = 0
    minus_trade_cum = 0
    plus_trade_cum_amount = 0.0
    minus_trade_cum_amount = 0.0
    biggest_cum_amount = 0.0
    dropdown_sequence_amount = 0.0
    dropdown_biggest_amount = 0.0
    d_d_max_delta = np.timedelta64(0, 'D')
    biggest = False

    s_date = dt.date(start_year, start_month, start_day)  # start date
    e_date = dt.date(stop_year, stop_month, stop_day)  # end date
    delta = e_date - s_date  # as timedelta
    date_list = list()
    for i in range(delta.days + 1):
        day = s_date + dt.timedelta(days=i)
        date_list.append(day)
    d_d_start_date = date_list[0]
    for dl_date in date_list:
        if dl_date.weekday() == 5:
            continue
        if (dl_date.weekday() == 6) & (entry_hour != 23):
            continue
        if dl_date.weekday() == 4:
            exit_hour = 22
        dl_date_next = dl_date + dt.timedelta(days=1)
        if dl_date_next > date_list[-1]:
            continue
        read_start_dt = dt.datetime(dl_date.year, dl_date.month, dl_date.day, entry_hour, entry_min, entry_sec)
        read_end_dt = dt.datetime(dl_date_next.year, dl_date_next.month, dl_date_next.day,
                                  exit_hour, exit_min, exit_sec)

        date_slice = ts.read_range(read_start_dt, read_end_dt)
        date_slice.info()
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
        print(exit_d_und_t)
        exit_reason = row_s.iloc[0]['Sell_target'] + row_s.iloc[0]['Sell_stop']
        if exit_reason == '' or exit_reason is None:
            exit_reason = 'TIME'
        print(exit_reason)
        exit_price = row_s.iloc[0]['Last']
        p_und_l = round(volume * (exit_price - entry_price), 2)
        cumulative_profit += p_und_l
        if (dropdown_sequence_amount < 0.0) & (dropdown_sequence_amount == dropdown_biggest_amount) & \
                (dropdown_biggest_amount != 0.0):
            d_d_end_date = dl_date
            d_d_sequence_delta = np.datetime64(d_d_end_date) - np.datetime64(d_d_start_date)
            # print('d_d_sequence_delta', d_d_sequence_delta)
            if d_d_sequence_delta > d_d_max_delta:
                d_d_max_delta = d_d_sequence_delta
                # print('d_d_max_delta', d_d_max_delta)
        if p_und_l >= 0:
            plus_trade_cum += 1
            plus_trade_cum_amount += p_und_l
            if cumulative_profit > biggest_cum_amount:
                biggest_cum_amount = cumulative_profit
                biggest = True
                dropdown_sequence_amount = 0.0
        else:
            minus_trade_cum += 1
            minus_trade_cum_amount += p_und_l
            if biggest:
                d_d_start_date = dl_date
            biggest = False
            if (cumulative_profit - biggest_cum_amount) < dropdown_sequence_amount:
                dropdown_sequence_amount = round(cumulative_profit - biggest_cum_amount, 2)
                # print('dropdown_sequence_amount', dropdown_sequence_amount)
                if dropdown_sequence_amount < dropdown_biggest_amount:
                    dropdown_biggest_amount = dropdown_sequence_amount
                    # print('dropdown_biggest_amount', dropdown_biggest_amount)
        plus_trade_cum_proc = round((plus_trade_cum / (plus_trade_cum + minus_trade_cum)) * 100, 1)
        minus_trade_cum_proc = round((minus_trade_cum / (plus_trade_cum + minus_trade_cum)) * 100, 1)
        p_und_l_cum_after_tax = round((plus_trade_cum_amount * 0.75) + minus_trade_cum_amount, 2)
        p_und_l_cum_after_tax_ref = round(cumulative_profit * 0.75, 2)
        if cumulative_profit < abs(dropdown_biggest_amount):
            dropdown_biggest_proc = 100
        else:
            dropdown_biggest_proc = round((abs(dropdown_biggest_amount) / cumulative_profit) * 100, 1)
        # print(date_slice)
        new_row = {'Symbol': symbol, 'Direction': direction, 'Entry_Date&Time': entry_d_und_t,
                   'Entry_Reason': entry_reason_time, 'Entry_Price': entry_price, 'Volume': volume,
                   'Exit_Date&Time': exit_d_und_t, 'Exit_Reason': exit_reason, 'Exit_Price': exit_price,
                   'P&L': p_und_l, 'Cum P&L': cumulative_profit, '+T': plus_trade_cum, '-T': minus_trade_cum,
                   '%Proc+T': plus_trade_cum_proc, '%Proc-T': minus_trade_cum_proc,
                   'Cum +P&L': plus_trade_cum_amount, 'Cum -P&L': minus_trade_cum_amount,
                   'Cum P&L-Tax': p_und_l_cum_after_tax, 'Cum P&L-Tax-Ref': p_und_l_cum_after_tax_ref,
                   'Max D-D Amount': dropdown_biggest_amount, 'Max D-D Time': d_d_max_delta,
                   '%Proc Max D-D': dropdown_biggest_proc}

        trades = trades.append(new_row, ignore_index=True)
        i += 1
    print(trades)
    print(errors)
    if save:
        path = 'C:/trading_data/' + '{:%Y-%m-%d}'.format(date_list[0]) + '_' + str(entry_hour) + '-' + \
               str(entry_min) + '-' + str(entry_sec) + '_' + str(ticks_to_target) + '_' + str(ticks_to_stop) + '_'
        trades.to_excel(path + 'trades.xlsx')
        errors.to_excel(path + 'errors.xlsx')
    errors_return = pd.DataFrame()
    # errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == j-1) &
    #                                                 (errors['ErrType'] == 'BUY IMPULSE ERROR')], ignore_index=True)
    #
    # errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == m-1) & (errors['ErrType'] == 'TIME SELL '
    #                                                                                                  'IMPULSE '
    #                                                                                                  'ERROR')],
    #                                      ignore_index=True)
    # errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == k-1) & (errors['ErrType'] == 'ANY SELL '
    #                                                                                                  'IMPULSE ERROR: '
    #                                                                                                  'TRADE NOT '
    #                                                                                                  'ADDED')],
    #                                     ignore_index=True)
    # errors_return['Entry_hour'] = entry_hour
    # errors_return['Ticks_to_target'] = ticks_to_target
    trades_return = pd.DataFrame()
    trades_return = trades_return.append(trades.iloc[-1], ignore_index=True)
    trades_return['Entry_hour'] = entry_hour
    trades_return['Ticks_to_target'] = ticks_to_target
    trades_return['NrTr'] = i-1
    return errors_return, trades_return
