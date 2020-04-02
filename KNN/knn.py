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
from indicator import generate_indicator
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


def knn(stock_code):
    # Initialize a spark session.
    def init_spark():
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        return spark

    spark = init_spark()

    generate_indicator(stock_code)
    indicator_data = 'sp500_indicator/'+stock_code+'_indicator.csv'
    data_frame = spark.read.csv(indicator_data, header=True, mode="DROPMALFORMED")
    data_frame = data_frame.replace("inf", None).na.drop()
    # data_frame.show()

    '''
    calculate label column:
    if stock close price increase 5% compare to 5days ago, label should be 1 which mean buy
    if stock close price oscillate less than 5% compare to 5days ago, label should be 0 which mean hold
    if stock close price decrease 5% compare to 5days ago, label should be -1 which mean sell
    '''


    def precentage(new_data, old_data):
        ''' (current close price - shirft close price(5 days))*100/ shirft close price  '''
        pg = (new_data - old_data) / old_data * 100
        return pg


    # def comp_prev(column_close, column_lag):
    #     print(column_close.value)
    #     print(column_lag)
    #     if (precentage(column_close, column_lag) >= 5):
    #         return 1
    #     elif (precentage(column_close, column_lag) <= -5):
    #         return -1
    #     else:
    #         return 0

    ''' 
            date|close|  lag|
    +----------+-----+-----+
    |2013-02-08|33.05| null|
    |2013-02-11|33.26| null|
    |2013-02-12|33.74| null|
    |2013-02-13|33.55| null|
    |2013-02-14|33.27| null|
    type: <class 'pandas.core.frame.DataFrame'>
    
    Use pyspark window, lead, and lag function:
    Ref: https://riptutorial.com/apache-spark/example/22861/window-functions---sort--lead--lag---rank---trend-analysis
    '''
    new_df = data_frame.withColumn('lag', lag('close', 5).over(Window.orderBy(asc('date'))))
    # new_df.show()

    '''
    How to use pyspark dataframe do if else
    ref:https://stackoverflow.com/questions/39048229/spark-equivalent-of-if-then-else
    
    date|close|  lag| label
    +----------+-----+-----+
    2013-02-22|32.59|33.27|    0|
    |2013-02-25|32.07|33.98|   -1|
    |2013-02-26|32.04|33.84|   -1|

    '''

    new_df = new_df.withColumn('label',
                               when(col('lag').isNull(), 0).when(precentage(col('close'), col('lag')) <= -2, -1).when(
                                   precentage(col('close'), col('lag')) >= 2, 1).otherwise(0))

    # new_df.show()

    '''Change pyspark dataframe to pandas dataframe'''
    new_df = new_df.toPandas()

    '''KNN start'''
    # TODO One bug is If the input is default all columns: Input contains NaN, infinity or a value too large for dtype('float64')
    # I think some columns value is too big

    '''
    type: <class 'pandas.core.frame.DataFrame'>
    '''
    x = new_df[
        ['date', 'volume', 'macd', 'macds', 'macdh', 'rsi_14', 'boll', 'boll_ub', 'boll_lb', 'kdjk', 'kdjd',
         'kdjj', 'adx', 'close_5_ema', 'close_10_ema', 'close_20_ema', 'close_40_ema','vr']]  # drop columns here

    # reset index to date
    x = x.set_index('date').astype(float)
    # print(type(x))
    # print(x.head())

    '''Scale the dataset to 0-1'''
    min_max_scaler = MinMaxScaler()  # normalization, a lot of choice from sklearn
    x_scale = min_max_scaler.fit_transform(x)
    x = pd.DataFrame(x_scale)
    # print(x.head())
    # print(x.shape)

    y = new_df['label'].values
    # print(y)
    # print(y.shape)

    # Split dataset to training and testing
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, train_size=0.8, random_state=42,
                                                        stratify=y)
    # n_neighbors is the parameter for K values
    knn = KNeighborsClassifier(n_neighbors=7, metric='euclidean', p=2)
    knn.fit(x_train, y_train)

    y_pred = knn.predict(x_test)
    # print(y_pred)

    # Calculate accuracy and f1
    accuracy = metrics.accuracy_score(y_test, y_pred)
    f1 = metrics.f1_score(y_test, y_pred, average='weighted')
    print(stock_code+": Accuracy:", accuracy)
    print(stock_code+": F1_score:", f1)
    return (accuracy, f1)

