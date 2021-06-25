import pandas as pd
import datetime as dt

pd.set_option('display.max_columns', 25)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

start_year_i, start_month_i, start_day_i = 2021, 1, 3
stop_year_i, stop_month_i, stop_day_i = 2021, 3, 20
time_a = dt.datetime(start_year_i, start_month_i, start_day_i)
time_b = dt.datetime(stop_year_i, stop_month_i, stop_day_i)

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
# h5_trades = pd.HDFStore(path + 'trades_AUDNZD.h5', 'r')
h5_trades = pd.HDFStore(path + 'ticks-AUDNZD-20200101120000-20210610112059.h5', 'r')
print(h5_trades.keys())
print(h5_trades.info())
# print(h5_trades.get_storer('trades_key').table)
print(h5_trades.walk())
a = 21
b = 90
print('{:%Y-%m-%d}'.format(time_a))
print('{:%Y-%m-%d}'.format(time_b))
aa = '{:%Y-%m-%d}'.format(time_a)
bb = '{:%Y-%m-%d}'.format(time_b)
query = '(Entry_hour == ' + str(a) + ') & (Ticks_to_target == ' + str(b) + ') & ' + '(Entry_Date_Time >= "' + aa + '") & (Exit_Date_Time <= "' + bb + '")'
print(query)
abc = pd.read_hdf(path + 'trades_AUDNZD.h5', 'trades_key',  where=query)
 #                        '(Entry_Date_Time >= ' + '{:%Y-%m-%d}'.format(time_a) + ') & '
 #                        '(Exit_Date_Time <= ' + '{:%Y-%m-%d}'.format(time_b) + ')')
print(abc.head(10))
h5_trades.close()


# store = pd.HDFStore(path + 'example.h5')
# print(store.keys())
# store.close()
