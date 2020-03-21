from pprint import pprint

# import alpha vantage
# https://github.com/RomelTorres/alpha_vantage
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators

import numpy as np
import pandas as pd
import dask.array as da
import dask.dataframe as dd
import csv
import matplotlib.pyplot as plt
import time

# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def get_sma(ti, stock_name, interval, time_period):
    data_ti, meta_data_ti = ti.get_sma(symbol=stock_name, interval=interval, time_period=time_period,
                                       series_type='close')
    df = data_ti
    return df


def get_macd(ti, stock_name, interval):
    data_ti, meta_data_ti = ti.get_macd(symbol=stock_name, interval=interval, series_type='close')
    df = data_ti
    return df


def get_rsi(ti, stock_name, interval, time_period):
    data_ti, meta_data_ti = ti.get_rsi(symbol=stock_name, interval=interval, series_type='close',
                                       time_period=time_period)
    df = data_ti
    return df


def get_adx(ti, stock_name, interval, time_period):
    data_ti, meta_data_ti = ti.get_adx(symbol=stock_name, interval=interval,
                                       time_period=time_period)
    df = data_ti
    return df


api_key = '193RYP0HN0HC1569'

stock_name = '.INX'

ts = TimeSeries(key=api_key, output_format='pandas')
ti = TechIndicators(key=api_key, output_format='pandas')

data_ts, meta_data_ts = ts.get_daily(symbol='.INX', outputsize='full')
data_ti_sma_5 = get_sma(ti, stock_name, 'daily', '5')
time.sleep(30)
data_ti_sma_10 = get_sma(ti, stock_name, 'daily', '10')
data_ti_sma_20 = get_sma(ti, stock_name, 'daily', '20')
time.sleep(30)
data_ti_sma_40 = get_sma(ti, stock_name, 'daily', '40')
data_ti_macd = get_macd(ti, stock_name, 'daily')
time.sleep(30)
data_ti_rsi = get_rsi(ti, stock_name, 'daily', '14')
data_ti_adx = get_adx(ti, stock_name, 'daily', '14')

df_sma_5 = data_ti_sma_5.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_sma_10 = data_ti_sma_10.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_sma_20 = data_ti_sma_20.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_sma_40 = data_ti_sma_40.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_macd = data_ti_macd.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_rsi = data_ti_rsi.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_adx = data_ti_adx.iloc[::-1].loc['2012-12-31':'2018-12-31']
df_price = data_ts.iloc[::-1].loc['2012-12-31':'2018-12-31']

print("=================================================")

# df2.index = df1.index
#
total_df = pd.concat([df_macd, df_rsi, df_adx, df_sma_5, df_sma_10, df_sma_20, df_sma_40, df_price], axis=1)
total_df.columns = ['macd', 'macd_signal', 'macd_hist', 'rsi', 'adx', 'sma_5', 'sma_10', 'sma_20', 'sma_40', 'open',
                    'high', 'low', 'close', 'volume']
# pprint(type(total_df))
total_df.to_csv('s&p.csv')
# https://www.investopedia.com/articles/forex/05/macddiverge.asp
plt.show()
#
# spark = init_spark()
# lines = spark.sparkContext.textFile('abc.csv')
# print(lines.count())

# print(type(data))
# pprint(data.head(2))
# # data = np.asarray(data)
# # print(data)
# # print(type(data))
# # df = pd.DataFrame(data)
# # print(type(df))
# df.to_csv('file.csv', index=False)
# # np.savetxt('test.csv', data, delimiter=',')
#
# # data = da.asarray(data)
# # print(type(data))

# print(list(data))
#
# spark = init_spark()
#
# print(type(data))
# data_frame = spark.createDataFrame(data, ('open', 'high', 'low', 'close volume'))
# print(data_frame.count())
# print(data_frame[1])
