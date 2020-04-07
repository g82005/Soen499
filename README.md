# Soen499

## Abstract
The stock market is highly unpredictable which makes it difficult for investors to know when to buy, sell, or hold their stock. This is particularly challenging when it comes to short term investing because stock prices tend to be more stochastic. With this project we will use data science analytics techniques to develop a system that helps investors make profitable investment decisions. Using the K-nearest neighbors (KNN) algorithm we will identify patterns in the stock price data with which we will use to recommend investors to buy, sell, or hold their stock. Additionally we will use the association rules technique to identify relationships between stocks.

## Introduction
Successful stock market investing is difficult due to the large number of complex factors that influence investment decisions such as fluctuating stock prices, expected stock price trends, and the sheer number of available stocks to choose from. 

With this project we will attempt to create a system that helps investors with finding profitable stocks and knowing when to buy, sell, or hold them. The main focus of our project will be short time investing since these trends tend to fluctuate more and are therefore more difficult for investors to understand and analyze.

It is important to note that actually predicting the price of a stock is not within the scope of our project. The stock market is an example of a second order chaos system, meaning that it responds to predication and thus becomes more unpredictable. Rather, our intention is to provide investors with additional insight into the data they are analyzing so that they are more informed when they make the final decision. 

Many of the challenges related to investment stem of human nature, by assisting investors with an unbiased system some of these difficulties can be alleviated. For instance, common mistakes made by investors are impatience, failing to diversify, and falling for the sunk cost fallacy. A system that helps investors decide when to buy, sell, or hold their stock can be useful to an impatient user or one who has invested a significant amount into a certain stock and is holding on to their stock with the intention of at least breaking even. Additionally, by finding correlations between stocks, our system can help investors diversify their portfolios and avoid the risk inherent in placing too much value in a single stock. 

Related work includes the ETL (extract, transform, load) process, machine learning, data mining techniques and data visualization.

## Materials and Methods
### Dataset:
Standard & Poor’s 500 (S&P 500) index includes 500 leading companies listed on the U.S. stock exchange. It is one of the best representations of the state U.S. stock market and covers approximately 80% of the available market capitalization. Therefore, the dataset we have chosen to analyze for this project includes all the companies listed in the S&P 500 index. From Kaggle, we have obtained an S&P 500 dataset which covers a period of 5 years from 2013 to 2018. Each company is represented by its symbol and for each day in the dataset there are prices for the close, open, high, low, volume, and change. The format of the dataset is shown in the figure below:
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581230-0b74d800-4f7b-11ea-9e6b-9ce2aab4ddf9.png">
  <p align="center">Fig.1: Column view of dataset</p>
</p>

### KNN:
We will be using a KNN classifier to provide an investor with a recommendation for certain stocks. Also, when a user is considering some stock, we will inform them of other stocks that are positively and negatively correlated. This will be implemented using the association rules technique. Figure 2 shows the data flow for both KNN and association rules. 

After retrieving the data from Kaggle, we will perform data pre-processing for the KNN model in order to obtain a set of features (stock indicators), to separate the training and test data sets, and determine the labels (buy, sell, hold). The labels will be decided based on the behavior of the stock price. For instance, if the stock price increases by a some amount over a certain period of time, then we will consider this as an indication to buy that stock. The precise thresholds will be discovered experimentally during this project. 

After data pre-processing, the feature and label data will be passed to the KNN model for training. Table 1 shows the input for KNN model. The output depends on the distance and K value. We will use the scikit-learn library for training and evaluating the KNN model. 

<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581314-08c6b280-4f7c-11ea-9e11-804e964ae531.png">
  <p align="center">Fig.2: Data flow</p>
</p>
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581343-71159400-4f7c-11ea-9d39-a5d8100ac5ba.png">
  <p align="center">Table 1: Example KNN - stock indicator(features) and stock recommendation(label)</p>
</p>

### Association Rule Learning:

Using the same data set, we will implement association rules to identify positively and negatively correlated stocks. An example of correlated stocks over a period of five years is presented in the table below. For all itemsets, "support" and "confidence" have to be calculated, we use these two evaluation metrics to find the correlation of the stock performance. The itemsets with low support will be pruned by applying Apriori Principle to reduce the amount of computation.

<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581403-0dd83180-4f7d-11ea-9135-5ec72d15874d.png">
  <p align="center">Table 2: Example of association rule</p>
</p>

### Prerequisite



