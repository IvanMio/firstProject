import tables as tb
import datetime as dt
from tables import tableextension as te

path = 'C:/Users/ivanm/Documents/Currency/AUDNZD/'
filename = path + 'trades_AUDNZD.h5'

# iterator = te.Row.fetch_all_fields()

h5_trades = tb.open_file(filename, 'r')
# print(h5_trades.walk_groups())
print('____________________________________________________________________________________________________')
py = h5_trades.root.trades_key.table
# py = h5_trades.root.trades
print('____________________________________________________________________________________________________')
trades_row = py.row
print('____________________________________________________________________________________________________')

print(type(py))
print(py)

# query = '(Entry_hour == 1) & (Ticks_to_target == -71)'
query = '(Entry_hour == 21) '
iterator = py.where(query)
i = 0
print(type(iterator))
print('____________________________________________________________________________________________________')
# print(iterator.nrow)
print(iterator)
print('____________________________________________________________________________________________________')
for row in iterator:
#    print(row['Ticks_to_target'])
    print(row.fetch_all_fields())
#     j = iterator.nrow
    i += 1
print(i)
h5_trades.close()
