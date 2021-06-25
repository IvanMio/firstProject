import pandas as pd
import matplotlib as plt
from matplotlib import pyplot as mpl

plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'

path_a = 'C:/Users/ivanm/Documents/Currency/AUDNZD/analysis/'
day_grouped = pd.read_excel(path_a + 'day_grouped.xlsx', 'Sheet1')
week_grouped = pd.read_excel(path_a + 'week_grouped.xlsx', 'Sheet1')
month_grouped = pd.read_excel(path_a + 'month_grouped.xlsx', 'Sheet1')
year_grouped = pd.read_excel(path_a + 'year_grouped.xlsx', 'Sheet1')

print(day_grouped.head(20))
print(day_grouped.tail(20))
day_grouped.set_index('Datetime', inplace=True)
day_grouped.plot(figsize=(10, 6))
mpl.show()
print(week_grouped.head(20))
print(week_grouped.tail(20))
week_grouped.set_index('Datetime', inplace=True)
week_grouped.plot(figsize=(10, 6))
mpl.xlim(['2021-06-13', '2021-06-19'])
mpl.show()
print(month_grouped.head(20))
print(month_grouped.tail(20))
month_grouped.set_index('Datetime', inplace=True)
month_grouped.plot(figsize=(10, 6))
mpl.show()
year_grouped.set_index('Datetime', inplace=True)
year_grouped.plot(figsize=(10, 6))
mpl.show()