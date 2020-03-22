from statistics import mean
from stockstats import StockDataFrame
import matplotlib.pyplot as plt
import pandas as panda


# import dask.dataframe as dd


def read(file_name):
    # With Spark dataframe, I cannot get by the column name since there are named as _c1, _c2
    # As on the documentation, it is recommended to use Pandas dataframe
    return StockDataFrame.retype(panda.read_csv(file_name))


# Reference for indicators: https://github.com/jealous/stockstats
def get_macd(data_frame):
    '''
    what is macdh:
    https://school.stockcharts.com/doku.php?id=technical_indicators:macd-histogram
    '''
    macdh = data_frame['macdh']
    macd = data_frame['macd']
    macds = data_frame['macds']
    temp_df = panda.concat([macd, macds, macdh], axis=1)
    return temp_df


def get_boll(data_frame):
    # print("---------- Start of BOLL ----------")
    # print(data_frame['boll'])
    boll_mid = data_frame['boll']
    boll_ub = data_frame['boll_ub']
    boll_lb = data_frame['boll_lb']
    temp_df = panda.concat([boll_mid, boll_lb, boll_ub], axis=1)
    return (temp_df)
    # print("---------- END of BOLL ----------\n")


def get_adx(data_frame):
    return (data_frame['adx'])


def get_rsi(data_frame):
    # Choice of RSI between 6 or 12 days
    return (data_frame['rsi_14'])


def get_kdj(data_frame):
    kdjk = data_frame['kdjk']
    kdjd = data_frame['kdjd']
    kdjj = data_frame['kdjj']
    temp_df = panda.concat([kdjk, kdjd, kdjj], axis=1)
    return temp_df


def get_ema(data_frame):
    ema_5 = data_frame['close_5_ema']
    ema_10 = data_frame['close_10_ema']
    ema_20 = data_frame['close_20_ema']
    ema_40 = data_frame['close_40_ema']
    temp_df = panda.concat([ema_5, ema_10, ema_20, ema_40], axis=1)
    return temp_df


# Reference: http://www.andrewshamlet.net/2017/08/12/python-tutorial-macd-moving-average-convergencedivergence/?fbclid=IwAR0O-71VZbxdx5IAU1xBoYu2-ItjcGFr1PDzr0LuuqaKc6M3onJj75cRpT4
def macdLong():
    stockDf = read()
    stockDf['ema12'] = stockDf['close'].ewm(span=12).mean()
    stockDf['ema26'] = stockDf['close'].ewm(span=26).mean()
    stockDf['macdLong'] = (stockDf['ema12'] - stockDf['ema26'])
    print(stockDf['macdLong'])
    return stockDf['macdLong']


if __name__ == '__main__':
    '''
    stockstats dataframe
    type: 'pandas.core.series.Series'
                 open   high    low  close   volume name
    date                                                
    2013-02-08  32.31  33.48  32.30  33.05  2599232  ZTS
    2013-02-11  33.06  33.50  32.88  33.26  1486115  ZTS
    '''
    df = read('ZTS_data.csv')

    # print(type(df))

    '''
    Indicator:
    1.MACD
    2.RSI
    3.BOLL
    4.KDJ
    5.ADX
    6.EMA
    '''

    '''
                   macd     macds     macdh
    date                                    
    2013-02-08  0.000000  0.000000  0.000000
    2013-02-11  0.004712  0.002618  0.002094
    2013-02-12  0.020888  0.010106  0.010783
    type: <class 'pandas.core.frame.DataFrame'>
    '''
    macd = get_macd(df)
    # print(type(macd))
    # print(macd)
    # macd.plot()
    # plt.show()

    """
    date
    2013-02-08           NaN
    2013-02-11    100.000000
    2013-02-12    100.000000
    type: class 'pandas.core.series.Series'
    overbought > 70
    oversold = 20-30
    """
    rsi = get_rsi(df)
    # print(type(rsi))
    # print(rsi)
    # rsi.plot()
    # plt.show()

    '''
                   boll    boll_lb    boll_ub
    date                                     
    2013-02-08  33.0500        NaN        NaN
    2013-02-11  33.1550  32.858015  33.451985
    2013-02-12  33.3500  32.642610  34.057390
    type: class 'pandas.core.frame.DataFrame'
    '''
    boll = get_boll(df)
    # print(type(boll))
    # print(boll)
    # boll.plot()
    # plt.show()

    '''
                     kdjk       kdjd       kdjj
    date                                       
    2013-02-08  54.519774  51.506591  60.546139
    2013-02-11  63.013183  55.342122  78.355304
    2013-02-12  70.244083  60.309442  90.113364
    type: class 'pandas.core.frame.DataFrame'
    overbought (over 80) or oversold (below 20).
    '''
    kdj = get_kdj(df)
    # print(type(kdj))
    # print(kdj)
    # kdj.plot()
    # plt.show()

    '''
    date
    2013-02-08           NaN
    2013-02-11    100.000000
    2013-02-12    100.000000
    type:<class 'pandas.core.series.Series'>
    '''
    adx = get_adx(df)
    # print(type(adx))
    # print(adx)
    # adx.plot()
    # plt.show()

    '''
               close_5_ema  close_10_ema  close_20_ema  close_40_ema
    date                                                             
    2013-02-08    33.050000     33.050000     33.050000     33.050000
    2013-02-11    33.176000     33.165500     33.160250     33.157625
    2013-02-12    33.443158     33.396445     33.373131     33.361535
    2013-02-13    33.487538     33.447035     33.424190     33.412242
    type: <class 'pandas.core.frame.DataFrame'>
    '''
    ema = get_ema(df)
    # print(type(ema))
    # print(ema)
    # ema.plot()
    # plt.show()

    '''
    Index(['open', 'high', 'low', 'close', 'volume', 'name', 'macd', 'macds',
       'macdh', 'close_-1_s', 'close_-1_d', 'rs_14', 'rsi_14', 'close_20_sma',
       'close_20_mstd', 'boll', 'boll_ub', 'boll_lb', 'rsv_9', 'kdjk_9',
       'kdjk', 'kdjd_9', 'kdjd', 'kdjj_9', 'kdjj', 'high_delta', 'um',
       'low_delta', 'dm', 'pdm', 'pdm_14_ema', 'pdm_14', 'tr', 'atr_14',
       'pdi_14', 'pdi', 'mdm', 'mdm_14_ema', 'mdm_14', 'mdi_14', 'mdi',
       'dx_14', 'dx', 'dx_6_ema', 'adx', 'adx_6_ema', 'adxr', 'close_5_ema',
       'close_10_ema', 'close_20_ema', 'close_40_ema']
    type(<class 'stockstats.StockDataFrame'>)   
    '''
    print(df.columns)
    print(type(df))
    df.to_csv('ZTS_data_indicator.csv')

