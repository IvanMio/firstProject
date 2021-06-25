# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import numpy as np
import pandas as pd
import matplotlib as plt
from matplotlib import pyplot as mpl


plt.style.use('seaborn')
mpl.rcParams['font.family'] = 'serif'
data = pd.read_csv('C:/ticks.csv', index_col=0, parse_dates=True, decimal=',')
data = pd.DataFrame(data['Last'], dtype=float)
data.dropna(inplace=True)
data.info()
print(data.tail())
print(data.head())
data['rets'] = np.log(data / data.shift(1))
data['vola'] = data['rets']
# data['vola'] = data['rets'].rolling(252).std() * np.sqrt(252)
data[['Last', 'vola']].plot(subplots=True, figsize=(10, 6));
mpl.show()
data.info()
print(data.tail())

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Strg+F8 to toggle the breakpoint.


#print_hi('Ivan')
# Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#    print_hi('Ivo')



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
