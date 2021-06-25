# This is a main class for one execution.
import datetime

import numpy as np
import pandas as pd


# TODO should I use numpy types here and/or make it simpler?
def string_to_time(from_string):
    from_string = datetime.datetime.strptime(from_string, '%H:%M:%S')
    from_string = datetime.time(from_string.hour, from_string.minute, from_string.second)
    return from_string


class Strategy(object):
    def __init__(self, symbol, volume, entry_time, exit_time, ticks_to_target, ticks_to_stop, save):
        self.symbol = symbol
        self.volume = volume  # TODO depends on symbol e.g. EUR AUD relationship
        self.entry_time = entry_time
        self.exit_time = exit_time
        self.ticks_to_target = ticks_to_target
        self.ticks_to_stop = ticks_to_stop
        self.save = save
        self.direction = 'long'
        self.entry_reason_time = 'ZEIT'
        self.tick = 0.00001
        pd.set_option('display.max_columns', 25)
        pd.set_option('display.max_rows', 100)
        pd.set_option('display.width', 1000)
        self.cumulative_profit = 0.0
        self.entry_price = np.float64  # placeholder: to be filled after entry.  TODO do I need this definition?
        print(self.tick, ticks_to_target, ticks_to_stop, volume)
        # timedelta (TODO es ist dependent on entry_time .. should be changed)
        entry_hour = int(entry_time[0:2])
        if entry_hour == 23:
            print(entry_hour)
            hours_delta = 1
        else:
            print(entry_hour)
            hours_delta = - entry_hour
        tda = datetime.timedelta(hours=hours_delta)
        self.tdb = pd.to_timedelta(hours_delta, unit='h')
        # Result for one variant, depending on entry_time, exit time, ticks to target, exit time and ticks to stop
        self.trades = pd.DataFrame(columns=['Symbol', 'Direction', 'Entry_Date&Time', 'Entry_Reason', 'Entry_Price', 'Volume',
                                       'Exit_Date&Time', 'Exit_Reason', 'Exit_Price', 'P&L', 'Cum P&L', '+T', '-T',
                                       '%Proc+T',
                                       '%Proc-T', 'Cum +P&L', 'Cum -P&L', 'Cum P&L-Tax', 'Max D-D Amount', 'Max D-D Time'])
        self.errors = pd.DataFrame(columns=['Date', 'ErrNr', 'ErrType'])
        self.entry_time = string_to_time(self.entry_time)
        self.exit_time = string_to_time(self.exit_time)
        self.entry_time = (datetime.datetime.combine(datetime.date.today(), self.entry_time) + tda).time()
        self.exit_time = (datetime.datetime.combine(datetime.date.today(), self.exit_time) + tda).time()
        print(entry_time)
        print(exit_time)
        self.data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
        self.data = pd.DataFrame(self.data['Last'], dtype=float)
        self.data.dropna(inplace=True)
        self.data.index += self.tdb
        self.data[
            'Date'] = self.data.index.date  # TODO If I could integrate those 2 lines I don't need an additional column in dataframe
        self.date_list = self.data[
            'Date'].unique().tolist()  # TODO If I could integrate those 2 lines I don't need an additional column
        print(self.date_list)
        self.data['Position'] = np.where((self.data.index.time > self.entry_time) & (self.data.index.time < self.exit_time), 1, 0)
        self.data['Buy'] = np.where((self.data['Position'] == 1) & (self.data['Position'].shift(1) == 0), 1, 0)
        self.data['Sell'] = np.where((self.data['Position'] == 0) & (self.data['Position'].shift(1) == 1), 'VRIME', '')
        self.data.info()

    # def preparation_trades_and_errors(self):
    #     global cumulative_profit, entry_price, tdb, trades, errors, entry_time, data, date_list
    #     volume = self.volume
    #     ticks_to_target = self.ticks_to_target
    #     ticks_to_stop = self.ticks_to_stop
    #     tick = self.tick
    #     exit_time = self.exit_time
    #     pd.set_option('display.max_columns', 25)
    #     pd.set_option('display.max_rows', 100)
    #     pd.set_option('display.width', 1000)
    #     cumulative_profit = 0.0
    #     entry_price = np.float64  # placeholder: to be filled after entry.  TODO do I need this definition?
    #     print(tick, ticks_to_target, ticks_to_stop, volume)
    #     # timedelta (TODO es ist dependent on entry_time .. should be changed)
    #     entry_hour = int(entry_time[0:2])
    #     if entry_hour == 23:
    #         print(entry_hour)
    #         hours_delta = 1
    #     else:
    #         print(entry_hour)
    #         hours_delta = - entry_hour
    #     tda = datetime.timedelta(hours=hours_delta)
    #     tdb = pd.to_timedelta(hours_delta, unit='h')
    #     # Result for one variant, depending on entry_time, exit time, ticks to target, exit time and ticks to stop
    #     trades = pd.DataFrame(columns=['Symbol', 'Direction', 'Entry_Date&Time', 'Entry_Reason', 'Entry_Price', 'Volume',
    #                                    'Exit_Date&Time', 'Exit_Reason', 'Exit_Price', 'P&L', 'Cum P&L', '+T', '-T',
    #                                    '%Proc+T',
    #                                    '%Proc-T', 'Cum +P&L', 'Cum -P&L', 'Cum P&L-Tax', 'Max D-D Amount', 'Max D-D Time'])
    #     errors = pd.DataFrame(columns=['Date', 'ErrNr', 'ErrType'])
    #     entry_time = string_to_time(entry_time)
    #     exit_time = string_to_time(exit_time)
    #     entry_time = (datetime.datetime.combine(datetime.date.today(), entry_time) + tda).time()
    #     exit_time = (datetime.datetime.combine(datetime.date.today(), exit_time) + tda).time()
    #     print(entry_time)
    #     print(exit_time)
    #     data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
    #     data = pd.DataFrame(data['Last'], dtype=float)
    #     data.dropna(inplace=True)
    #     data.index += tdb
    #     data[
    #         'Date'] = data.index.date  # TODO If I could integrate those 2 lines I don't need an additional column in dataframe
    #     date_list = data[
    #         'Date'].unique().tolist()  # TODO If I could integrate those 2 lines I don't need an additional column
    #     print(date_list)
    #     data['Position'] = np.where((data.index.time > entry_time) & (data.index.time < exit_time), 1, 0)
    #     data['Buy'] = np.where((data['Position'] == 1) & (data['Position'].shift(1) == 0), 1, 0)
    #     data['Sell'] = np.where((data['Position'] == 0) & (data['Position'].shift(1) == 1), 'VRIME', '')
    #     data.info()

    def execution_trades_and_errors(self):
        errors = self.errors
        entry_time = self.entry_time
        cumulative_profit = self.cumulative_profit
        date_list = self.date_list
        trades = self.errors
        volume = self.volume
        symbol = self.symbol
        direction = self.direction
        entry_reason_time = self.entry_reason_time
        ticks_to_target = self.ticks_to_target
        ticks_to_stop = self.ticks_to_stop
        tick = self.tick
        save = self.save
        tdb = self.tdb
        data = self.data
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
        for dl_date in date_list:
            print(i, dl_date)
            dl_date_text = '{:%Y-%m-%d}'.format(dl_date)
            date_slice = data.loc[dl_date_text].copy()  # TODO better solution?
            buy_row = date_slice.query('Buy == 1')
            if buy_row.empty:
                print(dl_date_text, j, 'BUY IMPULSE ERROR: NO TRADING')
                new_error = {'Date': dl_date_text, 'ErrNr': j, 'ErrType': 'BUY IMPULSE ERROR'}
                errors = errors.append(new_error, ignore_index=True)
                j += 1
            sell_row = date_slice.query('Sell == "VRIME"')
            if sell_row.empty:
                print(dl_date_text, m, 'TIME SELL IMPULSE ERROR')
                new_error = {'Date': dl_date_text, 'ErrNr': m, 'ErrType': 'TIME SELL IMPULSE ERROR'}
                errors = errors.append(new_error, ignore_index=True)
                m += 1
            # TODO maybe also to add not sell_row.empty or similar
            if not buy_row.empty:
                print(buy_row)
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
                # row_s = pd.concat([row_target, row_stop, row_sell]) TODO consider again
                if not row_stop.empty:
                    row_s = row_s.append(row_stop)
                if not row_target.empty:
                    row_s = row_s.append(row_target)
                row_s.sort_index(inplace=True)
                print(row_s.head())

                if not row_s.empty:
                    exit_d_und_t = row_s.index[0]
                    exit_d_und_t -= tdb
                    exit_reason = row_s.iloc[0]['Sell_target'] + row_s.iloc[0]['Sell'] + row_s.iloc[0]['Sell_stop']
                    print(exit_reason)
                    exit_price = row_s.iloc[0]['Last']
                    p_und_l = volume * (exit_price - entry_price)
                    cumulative_profit += p_und_l
                    if (dropdown_sequence_amount < 0.0) & (dropdown_sequence_amount == dropdown_biggest_amount) & \
                            (dropdown_biggest_amount != 0.0):
                        d_d_end_date = dl_date
                        d_d_sequence_delta = np.datetime64(d_d_end_date) - np.datetime64(d_d_start_date)
                        print('d_d_sequence_delta', d_d_sequence_delta)
                        if d_d_sequence_delta > d_d_max_delta:
                            d_d_max_delta = d_d_sequence_delta
                            print('d_d_max_delta', d_d_max_delta)
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
                            dropdown_sequence_amount = cumulative_profit - biggest_cum_amount
                            print('dropdown_sequence_amount', dropdown_sequence_amount)
                            if dropdown_sequence_amount < dropdown_biggest_amount:
                                dropdown_biggest_amount = dropdown_sequence_amount
                                print('dropdown_biggest_amount', dropdown_biggest_amount)
                    plus_trade_cum_proc = (plus_trade_cum / (plus_trade_cum + minus_trade_cum)) * 100
                    minus_trade_cum_proc = (minus_trade_cum / (plus_trade_cum + minus_trade_cum)) * 100
                    p_und_l_cum_after_tax = (plus_trade_cum_amount * 0.75) + minus_trade_cum_amount
                    p_und_l_cum_after_tax_ref = cumulative_profit * 0.75
                    # print(date_slice)
                    new_row = {'Symbol': symbol, 'Direction': direction, 'Entry_Date&Time': entry_d_und_t,
                               'Entry_Reason': entry_reason_time, 'Entry_Price': entry_price, 'Volume': volume,
                               'Exit_Date&Time': exit_d_und_t, 'Exit_Reason': exit_reason, 'Exit_Price': exit_price,
                               'P&L': p_und_l, 'Cum P&L': cumulative_profit, '+T': plus_trade_cum, '-T': minus_trade_cum,
                               '%Proc+T': plus_trade_cum_proc, '%Proc-T': minus_trade_cum_proc,
                               'Cum +P&L': plus_trade_cum_amount, 'Cum -P&L': minus_trade_cum_amount,
                               'Cum P&L-Tax': p_und_l_cum_after_tax, 'Cum P&L-Tax-Ref': p_und_l_cum_after_tax_ref,
                               'Max D-D Amount': dropdown_biggest_amount, 'Max D-D Time': d_d_max_delta}

                    trades = trades.append(new_row, ignore_index=True)
                else:
                    print(dl_date_text, k, 'ANY SELL IMPULSE ERROR: TRADE NOT ADDED')
                    new_error = {'Date': dl_date_text, 'ErrNr': k, 'ErrType': 'ANY SELL IMPULSE ERROR: TRADE NOT ADDED'}
                    errors = errors.append(new_error, ignore_index=True)
                    k += 1
            i += 1
        print(trades)
        print(errors)
        if save:
            path = 'C:/trading_data/' + '{:%Y-%m-%d}'.format(date_list[0]) + '_' + '{:%H-%M-%S}'.format(entry_time) + '_' \
                   + str(ticks_to_target) + '_' + str(ticks_to_stop) + '_'
            trades.to_csv(path + 'trades.csv')
            errors.to_csv(path + 'errors.csv')


one_strategy = Strategy('AUDNZD', 56363, '23:10:00', '22:50:00', 65, 1000, True)
# symbol = 'AUDNZD'
# volume = 56363  # TODO depends on symbol e.g. EUR AUD relationship
# entry_time = '23:10:00'
# exit_time = '22:50:00'
# ticks_to_target = 65
# ticks_to_stop = 1000
# save = True
#
# direction = 'long'
# entry_reason_time = 'ZEIT'
# tick = 0.00001

# one_strategy.preparation_trades_and_errors()
one_strategy.execution_trades_and_errors()
