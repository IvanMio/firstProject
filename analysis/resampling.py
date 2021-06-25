import tables as tb
import datetime as dt
from tables.hdf5extension import File
from tstables import TsTable


class TsDesc(tb.IsDescription):
    timestamp = tb.Int64Col(pos=0)
    Last = tb.Float64Col(pos=1)


path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
h5_input = File()
ts = TsTable(h5_input, '/', TsDesc)


def open_hdf5_files():
    global h5_input, ts
    # h5_input = tb.open_file(path + 'ticks.h5', 'r')
    # h5_input = tb.open_file(path + 'ticks-AUDNZD-20200101120000-20210610112059.h5', 'r')
    h5_input = tb.open_file(path + 'ticks-AUDNZD-20110101120000-20210610094559.h5', 'r')
    # h5_input = tb.open_file(path + 'ticks-AUDNZD-2020.h5', 'r')
    ts = h5_input.root.ts._f_get_timeseries()
    # h5_trades = tb.open_file(path + 'trades_AUDNZD.h5')
    # data.info()

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

