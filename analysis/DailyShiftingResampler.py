import numpy as np
import pandas as pd
import matplotlib as plt
from matplotlib import pyplot as mpl

plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'

tick = 0.00001
path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
path_a = 'C:/Users/ivanm/Documents/Currency/AUDNZD/analysis/'
# data = pd.read_csv(path + 'ticks.csv', index_col=0, parse_dates=True, decimal=',')
# data = pd.read_csv(path + 'ticks-AUDNZD-20200101120000-20210610112059.csv', index_col=0, parse_dates=True, decimal=',')
data = pd.read_csv(path + 'ticks-AUDNZD-20110101120000-20210610094559.csv', index_col=0, parse_dates=True, decimal=',')
data.info()
data = pd.DataFrame(data['Last'], dtype=float)
# print(data.head(20))

for i in range(0, 24, 1):
    offset = str(i) + 'H'
    # resampler = data.resample('24H', label='right', offset=offset)
    # res = pd.DataFrame()
    res = data.resample('24H', label='right', offset=offset).last()
    # res.rename('Close', inplace=True)
    res['Open'] = data.resample('24H', label='right', offset=offset).first()
    res['High'] = data.resample('24H', label='right', offset=offset).max()
    res['Low'] = data.resample('24H', label='right', offset=offset).min()
    res['High-Open'] = (res['High'] - res['Open']) / tick
    res['Low-Open'] = (res['Low'] - res['Open']) / tick
    file_name = 'daily_candle_base_' + str(i) + 'h.xlsx'
    res.to_excel(path_a + file_name)
    ticks_stat_pos = res['High-Open'].value_counts()
    ticks_stat_neg = res['Low-Open'].value_counts()
    bins = [0, 15, 30, 45, 60, 75, 90, 120, 150, 180, 210, 260, 310, 360, 410, 510, 610, 710, 810, 910, 1010, 1510, 2010, 3010, 1010]
    bins1 = [0, -15, -30, -45, -60, -75, -90, -120, -150, -180, -210, -260, -310, -360, -410, -510, -610, -710, -810, -910, -1010, -1510,
            -2010, -3010, -1010]
    ticks_stat_pos1 = res['High-Open'].value_counts(bins=bins, sort=False)
    ticks_stat_neg1 = res['Low-Open'].value_counts(bins=bins1, sort=False)
    file_name1 = 'ticks_stat_base_' + str(i) + 'h.xlsx'
    # ticks_stat.to_excel(path_a + file_name1)
    ticks_stat_pos1.plot(kind='bar', figsize=(10, 6))
    mpl.show()
    ticks_stat_neg1.plot(kind='bar', figsize=(10, 6))
    mpl.show()
    print(res.head(20))
    print(res.tail(20))
