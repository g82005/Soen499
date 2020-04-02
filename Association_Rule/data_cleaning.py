import os
import numpy as np
import pandas as pd
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn import metrics

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''


# Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: os.linesep.join([x, y]))
    return a + os.linesep


def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


spark = init_spark()
stock_data = 'all_stocks_5yr.csv'
data_frame = spark.read.csv(stock_data, header=True, mode="DROPMALFORMED")
data_frame = data_frame.replace("inf", None).na.drop()

# Since data_frame is used repeatedly it is recommended to persist it so that it does not need to be reevaluated
data_frame.persist()
# data_frame.show()

'''Use pyspark Window, lead, and lag function:
Ref:
https://riptutorial.com/apache-spark/example/22861/window-functions---sort--lead--lag---rank---trend-analysis

'''
'''
We want 500 stocks close individually lag 1 time period
Window partition:
Ref: https://knockdata.github.io/spark-window-function/
'''
stock_window = Window.partitionBy('Name')

'''
+----------+-----+------+------+-----+--------+----+-----+
|      date| open|  high|   low|close|  volume|Name|  lag|
+----------+-----+------+------+-----+--------+----+-----+
|2013-02-08|45.07| 45.35|  45.0|45.08| 1824755|   A| null|
|2013-02-11|45.17| 45.18| 44.45| 44.6| 2915405|   A|45.08|
|2013-02-12|44.81| 44.95|  44.5|44.62| 2373731|   A| 44.6|
|2013-02-13|44.81| 45.24| 44.68|44.75| 2052338|   A|44.62|
|2013-02-14|44.72| 44.78| 44.36|44.58| 3826245|   A|44.75|
|2013-02-15|43.48| 44.24| 42.21|42.25|14657315|   A|44.58|
|2013-02-19|42.21| 43.12| 42.21|43.01| 4116141|   A|42.25|
|2013-02-20|42.84| 42.85|42.225|42.24| 3873183|   A|43.01|
|2013-02-21|42.14| 42.14| 41.47|41.63| 3415149|   A|42.24|
|2013-02-22|41.83| 42.07| 41.58| 41.8| 3354862|   A|41.63|
|2013-02-25|42.09| 42.22| 41.29|41.29| 3622460|   A| 41.8|
|2013-02-26|40.62| 41.29| 40.19|40.97| 6185811|   A|41.29|
'''
new_df = data_frame.withColumn('lag', lag('close', 1).over(stock_window.orderBy(asc('date'))))
# new_df = new_df.sort(new_df.Name,new_df.date)
# new_df.show()


# Drop null value because it is not part of basket
new_df = new_df.filter(new_df.lag.isNotNull())

'''
Sorted by date(priority) and Name
|      date|   open|   high|    low|  close|   volume|Name|    lag|
+----------+-------+-------+-------+-------+---------+----+-------+
|2013-02-11|  45.17|  45.18|  44.45|   44.6|  2915405|   A|  45.08|
|2013-02-11|  14.89|  15.01|  14.26|  14.46|  8882000| AAL|  14.75|
|2013-02-11|  78.65|  78.91|  77.23|  78.39|   758016| AAP|   78.9|
|2013-02-11|68.0714|69.2771|67.6071|68.5614|129029425|AAPL|67.8542|
|2013-02-11|  36.13|  36.18|  35.75|  35.85|  6031957|ABBV|  36.25|
|2013-02-11|  46.85|   47.0|   46.5|  46.76|  1115888| ABC|  46.89|
|2013-02-11|  34.42|  34.49|  34.24|  34.26|  7928236| ABT|  34.41|
|2013-02-11|  73.09|  73.27|   72.1|  73.07|  1880055| ACN|  73.31|
|2013-02-11|  38.99|  39.05| 38.534|  38.64|  2333712|ADBE|  39.12|
|2013-02-11|  45.99|  46.14|  45.77|  46.08|  2382919| ADI|   45.7|
|2013-02-11|  30.26| 30.445|  30.13|  30.28|  4418487| ADM|  30.22|
|2013-02-11|  60.79|  60.98|  60.28|  60.34|  1366376| ADP| 60.925|
'''
new_df = new_df.sort(new_df.date, new_df.Name)
new_df.persist()
# new_df.show()

'''
Change back to RDD for a set of basket
[Row(date='2013-02-11', open='45.17', high='45.18', low='44.45', close='44.6', volume='2915405', Name='A', lag='45.08'),]
'''
rdd = new_df.rdd
# print(rdd.take(5))

'''
Help function:
we can change by different stock strategy.
0:percentage > 1%
1:percentage > 2%
2:percentage > 3%
Ideal format: 
('2013-02-11',0,['A','AA'])
('2013-02-11',1,['APPL','ADSK','AEP'])
('2013-02-11',2,['NAVD'])
'''


def compare(close_today, close_yesterday):
    if (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 > 3:
        return 3
    if (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 > 2:
        return 2
    elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 > 1:
        return 1
    else:
        return -1


'''
output:
[(('2013-02-11', -1), 'A'), (('2013-02-11', -1), 'AAL'), (('2013-02-11', -1), 'AAP'), (('2013-02-11', 1), 'AAPL'), (('2013-02-11', -1), 'ABBV')]
'''
rdd = rdd.map(lambda x: ((x[0], compare(x[4], x[7])), x[6]))
# print(rdd.take(5))

'''
output:
[(('2013-02-11', 1), 'AAPL'), (('2013-02-11', 1), 'AES'), (('2013-02-11', 1), 'AIG'), (('2013-02-11', 1), 'AIV'), (('2013-02-11', 2), 'AMD')]
'''
rdd = rdd.filter(lambda x: int(x[0][1]) > 0)
# print(rdd.take(5))

'''
(('2013-03-19', 1), ['BAC', 'BMY', 'CVS', 'DAL', 'DGX', 'DPS', 'EBAY', 'HCA', 'HRL', 'HSY', 'KMB', 'KO', 'MMC', 'NEE', 'PDCO', 'PG', 'SYY', 'UAL', 'ULTA', 'VFC', 'VRTX', 'WY']), (('2013-05-02', 2), ['AAL', 'ADBE', 'AGN', 'AIG', 'AMAT', 'AOS', 'AVGO', 'BAX', 'BDX', 'CBS', 'CELG', 'CMI', 'DE', 'DFS', 'DVN', 'EBAY', 'EL', 'EMR', 'ESRX', 'EW', 'FLR', 'GRMN', 'HCA', 'HES', 'HIG', 'HP', 'HRL', 'IPG', 'KIM', 'LUV', 'LYB', 'MS', 'NDAQ', 'PCAR', 'PCLN', 'PPG', 'SNI', 'TRIP', 'TSCO', 'TXT', 'VIAB', 'WHR', 'WYN'])
'''
rdd = rdd.groupByKey().mapValues(list)
#print(rdd.take(5))

# Test if there's stock increase more than 3% compare to yesterday price
# count: 1195
# rdd = rdd.filter(lambda x: int(x[0][1]) == 3)
# print(rdd.count())

'''
Sorted by the date and indicator
(('2013-02-11', 1), ['AAPL', 'AES', 'AIG', 'AIV', 'ANDV', 'ATVI', 'C', 'EMN', 'EQR', 'ETN', 'EXC', 'GLW', 'GPS', 'HOG', 'MAA', 'MHK', 'MNST', 'MSFT', 'NKE', 'NVDA', 'PNC', 'SNI', 'SWKS', 'TDG', 'USB', 'WFC', 'XYL']), (('2013-02-11', 2), ['BBY', 'GT', 'MU', 'NSC', 'PRGO', 'REGN']), (('2013-02-11', 3), ['AMD', 'CSX', 'MCO', 'NDAQ', 'SPGI']), 
'''
rdd = rdd.sortByKey()
print(rdd.take(5))

#Todo: RDD to dataframe which fit for pyspark.ml.fpm import FPGrowth
