import tables as tb
import datetime as dt

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
filename = path + 'trades_AUDNZD.h5'
# exit_reason: 0=>Target; 1 => time; 2 => stop
h5 = tb.open_file(filename, 'w')
row_des = {
    'Entry_Date_Time': tb.StringCol(26, pos=1),
    'Entry_Price': tb.Float64Col(pos=2),
    'Exit_Date_Time': tb.StringCol(26, pos=3),
    'Exit_Reason': tb.Int8Col(pos=4),
    'Exit_Price': tb.Float64Col(pos=5),
    'P_L_100t': tb.Float64Col(pos=6),
    'Entry_hour': tb.Int8Col(pos=7),
    'Ticks_to_target': tb.Int16Col(pos=8),
    'Ticks_to_stop': tb.Int16Col(pos=9)
}
# rows = 100000
filters = tb.Filters(complevel=0)

# hour_range = range(0, 24, 1)
# for entry_hour in hour_range:
#     tab = h5.create_table('/', 'trades_h' + str(entry_hour), row_des,
#                           title='AUDNZD Trades with entry hour = ' + str(entry_hour),
#                           expectedrows=rows, filters=filters)
#     print(type(tab))
#     print(tab)
# h5.close()

rows = 1000000
# group = h5.create_group('/', 'trades_key')
tab = h5.create_table('/', 'trades', row_des,
                      title='AUDNZD all Trades ',
                      expectedrows=rows, filters=filters)
print(type(tab))
print(tab)
h5.close()
