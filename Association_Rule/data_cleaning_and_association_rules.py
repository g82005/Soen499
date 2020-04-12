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
from pyspark.ml.fpm import FPGrowth

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
'''
 data frame format:
 date| open|  high|   low|close|  volume|Name
'''

stock_data = 'all_stocks_5yr.csv'
data_frame = spark.read.csv(stock_data, header=True, mode="DROPMALFORMED")
data_frame = data_frame.replace("inf", None).na.drop()

# Since data_frame is used repeatedly it is recommended to persist it so that it does not need to be reevaluated
# data_frame.persist()
# data_frame.show()

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

for year in range(2013, 2018):

    for e in range(2):

        new_df = data_frame.withColumn('lag', lag('close', 1).over(stock_window.orderBy(asc('date')))).persist()
        # new_df = new_df.sort(new_df.Name,new_df.date)
        # new_df.show()

        '''
        if e = 0, all stocks. if e = 1, remove utilities and energy from all stocks
        '''
        if e == 1:
            # energy and utilities sectors are highly correlated, removing them out of the dataset to find out the association rule for the rest when e == 1.
            utilities = set(
                ["AES", "LNT", "AEE", "AEP", "AWK", "ATO", "CNP", "CMS", "ED", "D", "DTE", "DUK", "EIX", "ETR", "ES",
                 "EXC", "FE", "NEE", "NI", "NRG", "PNW", "PPL", "PEG", "SRE", "SO", "WEC", "XEL"])
            energy = set(
                ["APA", "BKR", "COG", "CVX", "CXO", "COP", "DVN", "FANG", "EOG", "XOM", "HAL", "HP", "HES", "HFC",
                 "KMI", "MRO", "MPC", "NOV", "NBL", "OXY", "OKE", "PSX", "PXD", "SCG", "SLB", "FTI", "VLO", "WMB"])
            utilities_and_energy = utilities.union(energy)
            new_df = new_df.filter((new_df.Name).isin(utilities_and_energy) == False)

        '''Use pyspark Window, lead, and lag function:
        Ref:
        https://riptutorial.com/apache-spark/example/22861/window-functions---sort--lead--lag---rank---trend-analysis

        '''
        '''
        We want 500 stocks close individually lag 1 time period
        Window partition:
        Ref: https://knockdata.github.io/spark-window-function/
        '''
        if year == 2013:
            new_df = new_df.filter(new_df["date"] < lit("2014-01-01"))
        if year == 2014:
            new_df = new_df.filter(new_df["date"] >= lit("2014-01-01")).filter(new_df["date"] < lit("2015-01-01"))
        if year == 2015:
            new_df = new_df.filter(new_df["date"] >= lit("2015-01-01")).filter(new_df["date"] < lit("2016-01-01"))
        if year == 2016:
            new_df = new_df.filter(new_df["date"] >= lit("2016-01-01")).filter(new_df["date"] < lit("2017-01-01"))
        if year == 2017:
            new_df = new_df.filter(new_df["date"] >= lit("2017-01-01")).filter(new_df["date"] < lit("2018-01-01"))

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
        new_df.show()

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
            if (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 >= 4:
                return 5
            elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 >= 2:
                return 3
            elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 >= 0:
                return 1
            elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 >= -2:
                return -1
            elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 >= -4:
                return -3
            elif (float(close_today) - float(close_yesterday)) / float(close_yesterday) * 100 < -4:
                return -5


        '''
        output:
        [(('2013-02-11', -1), 'A'), (('2013-02-11', -1), 'AAL'), (('2013-02-11', -1), 'AAP'), (('2013-02-11', 1), 'AAPL'), (('2013-02-11', -1), 'ABBV')]
        '''
        rdd = rdd.map(lambda x: ((x[0], compare(x[4], x[7])), x[6]))
        # print(rdd.collect())

        '''
        (('2013-03-19', 1), ['BAC', 'BMY', 'CVS', 'DAL', 'DGX', 'DPS', 'EBAY', 'HCA', 'HRL', 'HSY', 'KMB', 'KO', 'MMC', 'NEE', 'PDCO', 'PG', 'SYY', 'UAL', 'ULTA', 'VFC', 'VRTX', 'WY']), (('2013-05-02', 2), ['AAL', 'ADBE', 'AGN', 'AIG', 'AMAT', 'AOS', 'AVGO', 'BAX', 'BDX', 'CBS', 'CELG', 'CMI', 'DE', 'DFS', 'DVN', 'EBAY', 'EL', 'EMR', 'ESRX', 'EW', 'FLR', 'GRMN', 'HCA', 'HES', 'HIG', 'HP', 'HRL', 'IPG', 'KIM', 'LUV', 'LYB', 'MS', 'NDAQ', 'PCAR', 'PCLN', 'PPG', 'SNI', 'TRIP', 'TSCO', 'TXT', 'VIAB', 'WHR', 'WYN'])
        '''
        rdd = rdd.groupByKey().mapValues(list)
        total = rdd.count()
        # print(rdd.take(5))

        '''
        +---+--------------------+
        | id|               items|
        +---+--------------------+
        |  0|[AAPL, ABC, ADI, ...|
        |  1|[ABBV, ABT, ADI, ...|
        |  2|[AAPL, ABT, AIG, ...|
        |  3|[AES, APA, ATVI, ...|
        |  4|[AAPL, ABBV, ABT,...|
        |  5|[ADP, ADSK, AMAT,...|
        |  6|[A, AAL, AAP, AAP...|
        |  7|[AAP, ABT, ACN, A...|
        |  8|[A, AAP, AAPL, AC...|
        |  9|[ETFC, HP, NFLX, ...|
        | 10|[A, AAL, AAP, ABC...|
        | 11|[A, AAP, ABT, ACN...|
        | 12|[ABBV, ADM, AET, ...|
        | 13|[AAL, ADM, AES, A...|
        '''
        rdd = rdd.zipWithIndex().map(lambda row: (row[1], row[0][1]))

        # Todo: RDD to dataframe which fit for pyspark.ml.fpm import FPGrowth
        sp_df = spark.createDataFrame(rdd, ["id", "items"])

        fpGrowth = FPGrowth(itemsCol="items", minSupport=0.1, minConfidence=0.4)
        model = fpGrowth.fit(sp_df)

        freq_itemsets = model.freqItemsets.orderBy([size("items"), "freq"], ascending=[0, 0])
        association_rules = model.associationRules

        inner_join = association_rules.join(
            freq_itemsets, association_rules.consequent == freq_itemsets.items, 'left'
        ).withColumn(
            'interest', abs(association_rules.confidence - freq_itemsets.freq / total)
        ).orderBy([size("antecedent"), "interest", "confidence"], ascending=[0, 0, 0])

        interest = inner_join.select(
            ['antecedent', 'consequent', 'confidence', 'freq', 'interest'])

        # write to files

        freq_itemsets = freq_itemsets.withColumn('items', concat_ws(',', 'items'))
        # print(freq_itemsets)

        '''
        You can use spark dataframe to get csv file or use pandas to get csv file
        '''
        # freq_itemsets.coalesce(1).write.csv(str(year) + '_' + str(e) + '_' + 'freq_itemsets')
        freq_itemsets.coalesce(1).toPandas().to_csv('results/' + str(year) + '_' + str(e) + '_' + 'freq_itemsets.csv')

        interest = interest.withColumn('antecedent', concat_ws(',', 'antecedent')).withColumn('consequent',
                                                                                              concat_ws(',',
                                                                                                        'consequent'))
        '''
        You can use spark dataframe to get csv file or use pandas to get csv file
        '''
        # interest.coalesce(1).write.csv(str(year) + '_' + str(e) + '_' + '_' + 'interest')
        interest.coalesce(1).toPandas().to_csv('results/' + str(year) + '_' + str(e) + '_' + '_' + 'interest.csv')
