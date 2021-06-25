import tstables as tstab
import pandas as pd
import tables as tb
import datetime as dt

# h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks.h5', 'r')
h5 = tb.open_file('C:/Users/ivanm/Documents/pythi/' + 'ticks-AUDNZD-2020.h5', 'r')
ts = h5.root.ts._f_get_timeseries()

read_start_dt = dt.datetime(2020, 1, 4, 00, 10, 0)
read_end_dt = dt.datetime(2020, 4, 4, 23, 50, 0)

rows = ts.read_range(read_start_dt, read_end_dt)
rows.info()
print(rows.head())
a = ts.min_dt()
print(a)
b = ts.max_dt()
print(b)
h5.close()
