import tstables as tstab
import pandas as pd
import tables as tb
import datetime as dt


class TsDesc(tb.IsDescription):
    timestamp = tb.Int64Col(pos=0)
    Last = tb.Float64Col(pos=1)


path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
# data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
# data = pd.read_csv(path + 'ticks-AUDNZD-20200101120000-20210610112059.csv', index_col=0, parse_dates=True, decimal=',')
data = pd.read_csv(path + 'ticks-AUDNZD-20110101120000-20210610094559.csv', index_col=0, parse_dates=True, decimal=',')

data = pd.DataFrame(data['Last'], dtype=float)
data.info()

# h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks.h5', 'w')
# h5 = tb.open_file(path + 'ticks-AUDNZD-20200101120000-20210610112059.h5', 'w')
h5 = tb.open_file(path + 'ticks-AUDNZD-20110101120000-20210610094559.h5', 'w')
ts = h5.create_ts('/', 'ts', TsDesc)
ts.append(data)

print(type(ts))
print('end')
h5.close()

