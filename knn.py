import dask.dataframe as dd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from random import randint
import sklearn
from sklearn.neighbors import KNeighborsClassifier
from sklearn import neighbors
from sklearn import preprocessing
from sklearn.model_selection import cross_validate
from sklearn.model_selection import train_test_split
from sklearn import metrics

df = pd.read_csv('s&p.csv').set_index('date')
print(df.head())


def s(a):
    return int(a * 0 + randint(-1, 1))


df['label'] = df['macd'].map(s)
print(df.head())
x = df.drop(columns=['label', 'volume', 'rsi', 'adx', 'sma_5', 'sma_10', 'sma_20', 'sma_40'])
min_max_scaler = preprocessing.MinMaxScaler()
# x_scale = min_max_scaler.fit_transform(x)
# x = pd.DataFrame(x_scale)
# print(x_scale)
print(x.head())
print(x.shape)
y = df['label'].values
print(y)
print(y.shape)

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, train_size=0.7, random_state=42, stratify=y)
x_scale = min_max_scaler.fit_transform(x_train)
x_train = pd.DataFrame(x_scale)
test_scale = min_max_scaler.fit_transform(x_test)
x_test = pd.DataFrame(test_scale)

knn = KNeighborsClassifier(n_neighbors=7, metric='euclidean', p=2)
knn.fit(x_train, y_train)

y_pred = knn.predict(x_test)

print("Accuracy:",metrics.accuracy_score(y_test, y_pred))