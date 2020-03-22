import csv
import os
import sys
import re
import pandas as pd
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from sklearn.neighbors import KNeighborsClassifier
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn import metrics

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''


# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


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


if __name__ == '__main__':
    spark = init_spark()
    data_frame = spark.read.csv('ZTS_data_indicator.csv', header=True, mode="DROPMALFORMED")
    data_frame = data_frame.na.fill('0')
    # data_frame.show()

    '''
    calculate label column:
    if stock close price increase 5% compare to 5days ago, label should be 1 which mean buy
    if stock close price oscillate less than 5% compare to 5days ago, label should be 0 which mean hold
    if stock close price decrease 5% compare to 5days ago, label should be -1 which mean sell
    '''

    def precentage(new_data, old_data):
        pg = (new_data - old_data) / old_data * 100
        return pg


    def comp_prev(df_column):
        print(df_column[1:])
        # if (precentage(df_column[1:], df_column[:1]) >= 5):
        #     return 1
        # elif (precentage(df_column[1:], df_column[:1]) <= -5):
        #     return -1
        # else:
        #     return 0


    print(data_frame.close.cast('integer'))
    # data_frame['label'] = comp_prev(data_frame['close'].cast(IntegerType()).values)
    # data_frame.show()

    # print(df.head())
    # x = df.drop(columns=['label', 'volume', 'rsi', 'adx', 'sma_5', 'sma_10', 'sma_20', 'sma_40'])
    # min_max_scaler = preprocessing.MinMaxScaler()
    # # x_scale = min_max_scaler.fit_transform(x)
    # # x = pd.DataFrame(x_scale)
    # # print(x_scale)
    # print(x.head())
    # print(x.shape)
    # y = df['label'].values
    # print(y)
    # print(y.shape)
    #
    # x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, train_size=0.7, random_state=42,
    #                                                     stratify=y)
    # x_scale = min_max_scaler.fit_transform(x_train)
    # x_train = pd.DataFrame(x_scale)
    # test_scale = min_max_scaler.fit_transform(x_test)
    # x_test = pd.DataFrame(test_scale)
    #
    # knn = KNeighborsClassifier(n_neighbors=7, metric='euclidean', p=2)
    # knn.fit(x_train, y_train)
    #
    # y_pred = knn.predict(x_test)
    #
    # print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
