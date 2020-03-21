from statistics import mean
from stockstats import StockDataFrame
import pandas as panda

def read():
    # With Spark dataframe, I cannot get by the column name since there are named as _c1, _c2
    # As on the documentation, it is recommended to use Pandas dataframe
    return StockDataFrame.retype(panda.read_csv("ZTS_data.csv"))

# Reference for indicators: https://github.com/jealous/stockstats
def macd():
    stockDf = read()
    print("---------- Start of MACD ----------")
    print(stockDf['macd'])
    print("---------- End of MACD ----------\n")
    return stockDf['macd']

def boll():
    stockDf = read()
    print("---------- Start of BOLL ----------")
    print(stockDf['boll'])
    print("---------- END of BOLL ----------\n")

def adx():
    stockDf = read()
    print("---------- Start of ADX ----------")
    print(stockDf['adx'])
    print("---------- END of ADX ----------\n")

def rsi():
    stockDf = read()
    print("---------- Start of RSI ----------")
    # Choice of RSI between 6 or 12 days
    # print(stockDf['rsi_6'])
    print(stockDf['rsi_12'])
    print("---------- END of RSI ----------\n")

def kdj():
    stockDf = read()
    print("---------- Start of KDJ ----------")
    print(stockDf['kdjk'])
    # print(stockDf['kdjd'])
    # print(stockDf['kdjj'])
    print("---------- END of KDJ ----------\n")

# Reference: http://www.andrewshamlet.net/2017/08/12/python-tutorial-macd-moving-average-convergencedivergence/?fbclid=IwAR0O-71VZbxdx5IAU1xBoYu2-ItjcGFr1PDzr0LuuqaKc6M3onJj75cRpT4
def macdLong():
    stockDf = read()
    stockDf['ema12'] = stockDf['close'].ewm(span=12).mean()
    stockDf['ema26'] = stockDf['close'].ewm(span=26).mean()
    stockDf['macdLong'] = (stockDf['ema12'] - stockDf['ema26'])
    print(stockDf['macdLong'])
    return stockDf['macdLong']


# boll()
# adx()
# rsi()
# kdj()

macd = macd()
macdLong = macdLong()

df = read()
print(df.join(macd).join(macdLong))