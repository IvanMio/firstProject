# This is a main class for one execution.
import numpy as np
import pandas as pd
import datetime

tdb = np.timedelta64

direction = 'long'
entry_reason_time = 'ZEIT'
tick = 0.00001

symbol = 'AUDNZD'
volume = 56363  # TODO depends on symbol e.g. EUR AUD relationship
ticks_to_target = 65
ticks_to_stop = 1000

# biggest = False


# TODO should I use numpy types here and/or make it simpler?
def string_to_time(from_string):
    from_string = datetime.datetime.strptime(from_string, '%H:%M:%S')
    from_string = datetime.time(from_string.hour, from_string.minute, from_string.second)
    return from_string


def strategy(symbol_in, volume_in, entry_time_in, exit_time_in, ticks_to_target_in, ticks_to_stop_in, save_in):
    global symbol, volume, ticks_to_target, ticks_to_stop, direction, entry_reason_time, tick, tdb

    # global errors, i, j, k, m, trades, biggest, biggest_cum_amount, cumulative_profit, d_d_max_delta
    # global dropdown_biggest_amount, dropdown_sequence_amount,  minus_trade_cum
    # global minus_trade_cum_amount, plus_trade_cum,  plus_trade_cum_amount
    # global d_d_start_date

    symbol = symbol_in
    volume = volume_in  # TODO depends on symbol e.g. EUR AUD relationship
    entry_time = entry_time_in
    exit_time = exit_time_in
    ticks_to_target = ticks_to_target_in
    ticks_to_stop = ticks_to_stop_in
    save = save_in

    exit_time_friday = '22:50:00'

    # direction = 'long'
    # entry_reason_time = 'ZEIT'
    # tick = 0.00001

    pd.set_option('display.max_columns', 25)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 1000)

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

    print(entry_time)
    print(exit_time)
    print(exit_time_friday)
    entry_time = string_to_time(entry_time)
    exit_time = string_to_time(exit_time)
    exit_time_friday = string_to_time(exit_time_friday)
    entry_time = (datetime.datetime.combine(datetime.date.today(), entry_time) + tda).time()
    exit_time = (datetime.datetime.combine(datetime.date.today(), exit_time) + tda).time()
    exit_time_friday = (datetime.datetime.combine(datetime.date.today(), exit_time_friday) + tda).time()
    print(entry_time)
    print(exit_time)
    print(exit_time_friday)

    # data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
    # data = pd.DataFrame(data['Last'], dtype=float)
    # data.dropna(inplace=True)
    # data.index += tdb
    # TODO If I could integrate those 2 lines I don't need an additional column in dataframe
    # data['Date'] = DatetimeIndex(data.index).date
    data['Date'] = data.index.date
    date_list = data['Date'].unique().tolist()
    print(date_list)
    # time_t = DatetimeIndex(data.index).time
    # data['Position'] = np.where((time_t > entry_time) & (time_t < exit_time), 1, 0)
    data['Position'] = np.where((data.index.time > entry_time) & (data.index.time < exit_time), 1, 0)
    print('if ' + str(exit_time) + str(exit_time_friday))
    data['Buy'] = np.where((data['Position'] == 1) & (data['Position'].shift(1) == 0), 1, 0)
    data['Sell'] = np.where((data['Position'] == 0) & (data['Position'].shift(1) == 1), 'VRIME', '')
    data.info()

    # def calculate_for_a_day(biggest, biggest_cum_amount, cumulative_profit, d_d_max_delta, d_d_start_date, direction, dl_date,
    #                         dropdown_biggest_amount, dropdown_sequence_amount, entry_reason_time, errors, i, j, k, m,
    #                         minus_trade_cum, minus_trade_cum_amount, plus_trade_cum, plus_trade_cum_amount, symbol, tdb, tick,
    #                         ticks_to_stop, ticks_to_target, trades, volume):
    def calculate_for_a_day(dl_date):
        global symbol, volume, ticks_to_target, ticks_to_stop, direction, entry_reason_time, tick, tdb
        nonlocal errors, i, j, k, m, trades, biggest, biggest_cum_amount, cumulative_profit, d_d_max_delta
        nonlocal dropdown_biggest_amount, dropdown_sequence_amount, minus_trade_cum
        nonlocal minus_trade_cum_amount, plus_trade_cum, plus_trade_cum_amount
        nonlocal d_d_start_date, d_d_sequence_delta, plus_trade_cum_proc, minus_trade_cum_proc
        nonlocal exit_time
        # print(i, dl_date)
        dl_date_text = '{:%Y-%m-%d}'.format(dl_date)
        date_slice = data.loc[dl_date_text].copy()  # TODO better solution?
        buy_row = date_slice.query('Buy == 1')
        if buy_row.empty:
            # print(dl_date_text, j, 'BUY IMPULSE ERROR: NO TRADING')
            new_error = {'Date': dl_date_text, 'ErrNr': j, 'ErrType': 'BUY IMPULSE ERROR'}
            errors = errors.append(new_error, ignore_index=True)
            j += 1
        sell_row = date_slice.query('Sell == "VRIME"')
        if sell_row.empty:
            # print(dl_date_text, m, 'TIME SELL IMPULSE ERROR')
            new_error = {'Date': dl_date_text, 'ErrNr': m, 'ErrType': 'TIME SELL IMPULSE ERROR'}
            errors = errors.append(new_error, ignore_index=True)
            m += 1
        # TODO maybe also to add not sell_row.empty or similar
        if not buy_row.empty:
            # print(buy_row)
            entry_price = buy_row.iloc[0]['Last']
            entry_d_und_t = buy_row.index[0]
            entry_d_und_t -= tdb
            target_price = buy_row.iloc[0]['Last'] + ticks_to_target * tick
            stop_price = buy_row.iloc[0]['Last'] - ticks_to_stop * tick

            date_slice['Sell_target'] = np.where((date_slice['Last'] >= target_price) & (date_slice['Position'] == 1),
                                                 'TARGET', '')
            date_slice['Sell_stop'] = np.where((date_slice['Last'] <= stop_price) & (date_slice['Position'] == 1),
                                               'STOP',
                                               '')
            row_target = date_slice.loc[date_slice['Sell_target'] == 'TARGET'].head(1)
            row_stop = date_slice.loc[date_slice['Sell_stop'] == 'STOP'].head(1)
            row_s = date_slice.loc[date_slice['Sell'] == 'VRIME'].head(1)
            print('row_s normal ', row_s)

            # row_s = pd.concat([row_target, row_stop, row_sell]) TODO consider again
            if not row_stop.empty:
                row_s = row_s.append(row_stop)
            if not row_target.empty:
                row_s = row_s.append(row_target)
            row_s.sort_index(inplace=True)
            # print(row_s.head())
            if row_s.empty:
                row_s = date_slice.iloc[-10:-7]
                print('row_s ersatz ', row_s)
                row_s = date_slice.iloc[-10:-7].head(1)
                print('row_s ersatz 1', row_s)
                print('ersatz end')

            if not row_s.empty:
                exit_d_und_t = row_s.index[0]
                print(exit_d_und_t)
                exit_d_und_t -= tdb
                exit_reason = row_s.iloc[0]['Sell_target'] + row_s.iloc[0]['Sell'] + row_s.iloc[0]['Sell_stop']
                # print(exit_reason)
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
                        # if (dropdown_sequence_amount == dropdown_biggest_amount) & (dropdown_biggest_amount != 0.0):
                        #     d_d_end_date = dl_date
                        #     d_d_sequence_delta = np.datetime64(d_d_end_date) - np.datetime64(d_d_start_date)
                        #     print('d_d_sequence_delta', d_d_sequence_delta)
                        #     if d_d_sequence_delta > d_d_max_delta:
                        #         d_d_max_delta = d_d_sequence_delta
                        #         print('d_d_max_delta', d_d_max_delta)
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
            else:
                # print(dl_date_text, k, 'ANY SELL IMPULSE ERROR: TRADE NOT ADDED')
                new_error = {'Date': dl_date_text, 'ErrNr': k, 'ErrType': 'ANY SELL IMPULSE ERROR: TRADE NOT ADDED'}
                errors = errors.append(new_error, ignore_index=True)
                k += 1
        # i += 1
        return errors, i, j, k, m, trades

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
    d_d_start_date = date_list[0]
    biggest = False
    for dl_date in date_list:
        errors, i, j, k, m, trades = calculate_for_a_day(dl_date)
    print(trades)
    print(errors)
    # print(i-1, new_row.values())
    # print(j-1, m-1, k-1, new_error.values())
    if save:
        # path = 'C:/trading_data/' + '{:%Y-%m-%d}'.format(date_list[0]) + '_' + '{:%H-%M-%S}'.format(entry_time) + '_' \
        #        + str(ticks_to_target) + '_' + str(ticks_to_stop) + '_'
        path = 'C:/trading_data/' + '{:%Y-%m-%d}'.format(date_list[0]) + '_' + str(entry_hour) + '_' + \
               '{:%M-%S}'.format(entry_time) + '_' + str(ticks_to_target) + '_' + str(ticks_to_stop) + '_'
        trades.to_excel(path + 'trades.xlsx')
        errors.to_excel(path + 'errors.xlsx')
    errors_return = pd.DataFrame()
    errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == j-1) & (errors['ErrType'] == 'BUY IMPULSE ERROR')], ignore_index=True)
    errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == m-1) & (errors['ErrType'] == 'TIME SELL IMPULSE ERROR')], ignore_index=True)
    errors_return = errors_return.append(errors.loc[(errors['ErrNr'] == k-1) & (errors['ErrType'] == 'ANY SELL IMPULSE ERROR: TRADE NOT ADDED')], ignore_index=True)
    errors_return['Entry_hour'] = entry_hour
    errors_return['Ticks_to_target'] = ticks_to_target
    trades_return = pd.DataFrame()
    trades_return = trades_return.append(trades.iloc[i-2], ignore_index=True)
    trades_return['Entry_hour'] = entry_hour
    trades_return['Ticks_to_target'] = ticks_to_target
    trades_return['NrTr'] = i-1
    return errors_return, trades_return
    # return i-1, new_row, j-1, m-1, k-1, new_error


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
    # data = pd.read_csv('C:/ticks-AUDNZD-Jahr.csv', index_col=0, parse_dates=True, decimal=',')
    data = pd.DataFrame(data['Last'], dtype=float)
    data.dropna(inplace=True)
    # data.info()
    entry_hour, tda, tdb = time_deltas(entry_time_in)
    data.index += tdb


symbol_i = 'AUDNZD'
volume_i = 56363  # TODO depends on symbol e.g. EUR AUD relationship
entry_time_i = '23:10:00'
exit_time_i = '22:50:00'
ticks_to_target_i = 200
ticks_to_stop_i = 300
save_i = True

data = pd.DataFrame()


# strategy(symbol_i, volume_i, entry_time_i, exit_time_i, ticks_to_target_i, ticks_to_stop_i, save_i)
