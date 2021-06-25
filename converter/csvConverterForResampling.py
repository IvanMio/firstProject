import tstables as tstab
import pandas as pd
import tables as tb
import datetime as dt
import numpy as np
import matplotlib as plt
from matplotlib import pyplot as mpl

plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'


class TsDesc(tb.IsDescription):
    timestamp = tb.Int64Col(pos=0)
    Last = tb.Float64Col(pos=1)


path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
# data = pd.read_csv(path + 'ticks.csv', index_col=0, parse_dates=True, decimal=',')
# data = pd.read_csv(path + 'ticks-AUDNZD-20200101120000-20210610112059.csv', index_col=0, parse_dates=True, decimal=',')
data = pd.read_csv(path + 'ticks-AUDNZD-20110101120000-20210610094559.csv', index_col=0, parse_dates=True, decimal=',')
data.info()
data = pd.DataFrame(data['Last'], dtype=float)
print(data.head(20))

res = data.resample('1T', label='right').last()
res['log'] = np.log(res / res.shift(1)) * 10000
res['pct'] = res['Last'].pct_change() * 10000
print(res.head(20))

res2 = data.resample('5T', label='right').last()
res2['log'] = np.log(res2 / res2.shift(1)) * 10000
res2['pct'] = res2['Last'].pct_change() * 10000
print(res2.head(20))


res3 = data.resample('20T', label='right').last()
res3['log'] = np.log(res3 / res3.shift(1)) * 10000
res3['pct'] = res3['Last'].pct_change() * 10000
print(res3.head(20))

res4 = data.resample('4H', label='right', base=23).last()
res4['log'] = np.log(res4 / res4.shift(1)) * 10000
res4['pct'] = res4['Last'].pct_change() * 10000
print(res4.head(20))

years = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]


res_grouped = res['log'].groupby([res.index.hour, res.index.minute]).mean()
res_grouped.index.set_names(['Hour', 'Min'], inplace=True)
df_res_grouped = pd.DataFrame(res_grouped)
df_res_grouped.reset_index(inplace=True)
df_res_grouped['Sum'] = df_res_grouped['log'].cumsum()
print(res_grouped.head(60))
df_res_grouped['Timedelta'] = pd.to_timedelta(df_res_grouped['Hour'], 'hours') \
                               + pd.to_timedelta(df_res_grouped['Min'], 'min')
df_res_grouped['Datetime'] = dt.datetime(2021, 6, 21) + df_res_grouped['Timedelta']
df_res_grouped.set_index('Datetime', inplace=True)
print(df_res_grouped.head(60))
print(df_res_grouped.tail(60))
df_res_grouped['Sum'].plot(subplots=True, figsize=(10, 6))
mpl.show()


# days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
days = [6, 0, 1, 2, 3, 4, 5]
index_list1 = list()
for day in days:
    for hour in range(0, 24, 1):
        for minutes in range(0, 55, 5):
            index_list1.append((day, hour, minutes))
# res2_grouped = res2['log'].groupby([res2.index.day_name(), res2.index.hour, res2.index.minute]).mean().reindex(index_list1)
res2_grouped = res2['log'].groupby([res2.index.dayofweek, res2.index.hour, res2.index.minute]).mean().reindex(index_list1)
res2_grouped.index.set_names(['Weekday', 'Hour', 'Min'], inplace=True)
df_res2_grouped = pd.DataFrame(res2_grouped)
df_res2_grouped.reset_index(inplace=True)
df_res2_grouped['Sum'] = df_res2_grouped['log'].cumsum()
df_res2_grouped['Weekday'].replace({6: -1}, inplace=True)
print(res2_grouped.head(60))
df_res2_grouped['Timedelta'] = pd.to_timedelta(df_res2_grouped['Weekday'], 'days') \
                               + pd.to_timedelta(df_res2_grouped['Hour'], 'hours') \
                               + pd.to_timedelta(df_res2_grouped['Min'], 'min')
df_res2_grouped['Datetime'] = dt.datetime(2021, 6, 21) + df_res2_grouped['Timedelta']
df_res2_grouped.set_index('Datetime', inplace=True)
# df_res2_grouped['Timedelta'] = dt.timedelta(days=df_res2_grouped['Weekday'], hours=df_res2_grouped['Hour'], minutes=df_res2_grouped['Min'])
# df_res2_grouped['Timedelta'] = dt.timedelta(days=df_res2_grouped['Weekday'], hours=df_res2_grouped['Hour'])
print(df_res2_grouped.head(60))
print(df_res2_grouped.tail(60))
df_res2_grouped['Sum'].plot(subplots=True, figsize=(15, 9))
mpl.show()


res3_grouped = res3['log'].groupby([res3.index.day, res3.index.hour, res3.index.minute]).mean()
res3_grouped.index.set_names(['Monthday', 'Hour', 'Min'], inplace=True)
df_res3_grouped = pd.DataFrame(res3_grouped)
df_res3_grouped.reset_index(inplace=True)
df_res3_grouped['Sum'] = df_res3_grouped['log'].cumsum()
print(res3_grouped.head(60))
df_res3_grouped['Timedelta'] = pd.to_timedelta(df_res3_grouped['Monthday'], 'days') \
                               + pd.to_timedelta(df_res3_grouped['Hour'], 'hours') \
                               + pd.to_timedelta(df_res3_grouped['Min'], 'min')
df_res3_grouped['Datetime'] = dt.datetime(2021, 6, 30) + df_res3_grouped['Timedelta']
df_res3_grouped.set_index('Datetime', inplace=True)
print(df_res3_grouped.head(60))
print(df_res3_grouped.tail(60))
df_res3_grouped['Sum'].plot(subplots=True, figsize=(10, 6))
mpl.show()



months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
index_list2 = list()
for month in months:
    for hour in range(1, 32, 1):
        for minutes in range(0, 24, 1):
            index_list2.append((month, hour, minutes))
# res4_grouped = res4['log'].groupby([res4.index.month_name(), res4.index.day, res4.index.hour]).mean().reindex(index_list2)
res4_grouped = res4['log'].groupby([res4.index.month, res4.index.day, res4.index.hour]).mean()
res4_grouped.index.set_names(['Month', 'Day', 'Hour'], inplace=True)
df_res4_grouped = pd.DataFrame(res4_grouped)
df_res4_grouped.reset_index(inplace=True)
df_res4_grouped['Sum'] = df_res4_grouped['log'].cumsum()
df_res4_grouped['Year'] = 2020
print(res4_grouped.head(360))
# df_res4_grouped['Timedelta'] = pd.to_timedelta(df_res4_grouped['Day'], 'days') \
#                                + pd.to_timedelta(df_res3_grouped['Hour'], 'hours') \
#                                + 30 * pd.to_timedelta(df_res3_grouped['Month'], 'days')
df_res4_grouped['Datetime'] = pd.to_datetime(df_res4_grouped[['Year', 'Month', 'Day', 'Hour']])
df_res4_grouped.set_index('Datetime', inplace=True)
# df_res3_grouped['Datetime'] = pd.to_datetime(df_res4_grouped)
print(df_res4_grouped.head(60))
print(df_res4_grouped.tail(60))
df_res4_grouped['Sum'].plot(subplots=True, figsize=(10, 6))
mpl.show()

# h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks.h5', 'w')
# h5 = tb.open_file(path + 'ticks-AUDNZD-20200101120000-20210610112059.h5', 'w')
# h5 = tb.open_file(path + 'ticks-AUDNZD-20110101120000-20210610094559.h5', 'w')
# ts = h5.create_ts('/', 'ts', TsDesc)
# ts.append(data)
#
# print(type(ts))
print('end')
# h5.close()

