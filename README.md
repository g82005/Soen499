# Soen499 Project


## Abstract

The stock market is highly unpredictable which makes it difficult for investors to know when to buy, sell, or hold their stock. This is particularly challenging when it comes to short term investing because stock prices tend to be more stochastic. With this project we will use data science analytics techniques to develop a system that helps investors make profitable investment decisions. Using the K-nearest neighbors (KNN) algorithm we will generate buy, hold and sell indicators to support informed decision making when investing. Additionally, we will use the association rules to seek to highlight relatable stocks to help investors reduce risk through identifying repeatable patterns between selective stocks.


## Introduction

Successful stock market investing is difficult due to the large number of complex factors that influence investment decisions, such as fluctuating stock prices, expected stock price trends, and the sheer number of available stocks to choose from. 

With this project, we will attempt to create a system to support investors with finding profitable stocks and knowing when to buy, sell, or hold them. The main focus of our project will be based on short term investing since short term trends have a greater tendency to fluctuate, and are therefore more difficult for investors to analyze and correctly reach a profitable outcome.

It is important to note that the price prediction of a stock is not within the scope of our project. The stock market is an example of a second order chaos system, meaning that it responds to predication and thus becomes more unpredictable. Rather, our intention is to provide investors with additional insights into the data they are analyzing, and therefore are more informed when they make the final decision. 

Many of the challenges related to investment stem from human nature, whereby some of these difficulties can be alleviated by assisting investors with an unbiased system. For instance, common mistakes made by investors are impatience, failing to diversify, and falling for the sunk cost fallacy. A system that helps investors decide when to buy, sell, or hold their stock can be useful for an impatient user or one who has invested a significant amount into a certain stock and is holding on to their stock with the intention of at least breaking even. Additionally, by finding correlations between stocks, our system can help investors diversify their portfolios and avoid the risk inherent in placing too much value in a single stock. 

Related work includes the ETL (extract, transform, load) process, machine learning, data mining techniques and data visualization.


## Materials and Methods

Standard & Poor’s 500 (S&P 500) index includes 500 leading companies listed on the U.S. stock exchange. It is one of the best representations of the state U.S. stock market and covers approximately 80% of the available market capitalization[1]. Therefore, the dataset we have chosen to analyze for this project includes all companies listed in the S&P 500 index. From Kaggle, we have obtained 505 stocks, which covers a period of 5 years from 2013 to 2018. Each company is represented by its symbol and for each day in the dataset there are prices for the close, open, high, low, volume, and change. Figure 1 shows the original data format. Figure 2 shows the data flow diagram for both KNN and association rules. 
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581230-0b74d800-4f7b-11ea-9e6b-9ce2aab4ddf9.png">
  <p align="center">Figure.1: Column view of dataset</p>
</p>
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/74581314-08c6b280-4f7c-11ea-9e11-804e964ae531.png">
  <p align="center">Figure.2: Data Flow Diagram</p>
</p>


### Data Preprocessing:

According to the data flow diagram, the data will undergo data preprocessing before it moves to the KNN and association rule stage. For KNN, a set of features were obtained (14 stock indicators created by Stockstats library[2]), and labels (buy, sell, hold) were determined by comparing stock prices based on each row value to the stock’s row value 5 days prior in the obtained dataset. For instance, if a stock price increased by a certain amount over a certain period of time, then this would be considered as an indication to buy that stock. The precise threshold used in this project is 5% price change over a 5 day duration. In addition, min max scale smoothing was also applied to each feature to rescale the range to be within [0, 1] to ensure fairness in the weight of each feature. Table 1 shows an example after data prepossessing. For association rules, all non-related columns were removed (open price, volume, etc.). Then, the closing price from the previous day was added to each row to calculate the percentage change in price. Based on the percent change, each stock is placed into one of 6 predefined “baskets” (defined by the Basket ID). An example of correlated stocks that are placed in the same “basket” in one day are presented in Table 2.
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060011-52670d00-7c4e-11ea-9e09-691b71cf5498.png">
  <p align="center">Table 1: Example KNN - stock indicator(features) and stock recommendation(label)</p>
</p>
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79059974-f8fede00-7c4d-11ea-8736-e6d7ed2faf31.png">
  <p align="center">Table 2: Example - Association Rule Data Prepossessing</p>
</p>


### KNN:

After data pre-processing, the feature and label data will be passed to the KNN model for training.The scikit-learn KNN KNeighborsClassifier library was used for training and evaluating the KNN model[3]. Two types of KNN classifiers were used. Although both classifiers used euclidean distance to calculate the distances, one used a fixed K value of 5 nearest neighbors and another used a fixed K-Fold value of 10 with the best value for the nearest neighbors. The best K value is determined by having the best cross-validation score on average (as shown in Figure 3). The chance of overfitting or underfitting the model was reduced by using K-Fold. Afterwards, the model was verified by using the accuracy score and F1 score.
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060038-96f2a880-7c4e-11ea-90cf-66647e18ecbc.png">
  <p align="center">Figure 3: Best K Value for the Nearest Neighbor value for the ZTS stock</p>
</p>


### Association Rule:

Association rules were implemented to identify positively correlated stocks. The pyspark MLlib FP-Growth algorithm[4] was implemented to identify frequent itemsets and association rules. To find as many association rules as possible and ensure that only rules with valuable information are selected, a minimum support and confidence parameter were set to be 0.1 and 0.4  for all qualified frequent itemsets after testing different combinations. Association rules with an interest of less than 0.5 were also ignored.


## Results

### KNN:
For the KNN classifier, our model outputs the predictions by date, as shown in table 4, which suggest the user to buy, hold or sell a specific stock. However, the accuracy of the predictions cannot be determined. As a result, an accuracy score and F1 score were performed to demonstrate the correctness of the model. On average, for all stocks combined, an average of 74.44% for the accuracy score, and 74.21% for the F1 score were obtained. Another model was also built using K-Fold to verify the presence of any overfitting or underfitting. Hence, the average was 74.31% for the accuracy score and 74.05% for the F1 score using the K-Fold method. As shown in table 3(The details can be found in [here](https://github.com/vincentsun870530/Soen499/tree/master/KNN/results)), the 2 models performed similar in terms of accuracy.

</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060090-16807780-7c4f-11ea-82c0-b2b090a07a79.png">
  <p align="center">Table 3: Total Average for each type of accuracy</p>
</p>
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060121-3d3eae00-7c4f-11ea-8704-0a65969330f5.png">
  <p align="center">Table 4: Predictions for the ZTS stock</p>
</p>

### Association Rule:
For the application of association rules, there were some interesting finds. First of all, the output shows that in some industries, the stock prices are likely to perform similarly. For example, in the Energy and Utilities sectors, the stocks tend to have similar behaviors every year with frequencies at around 150.
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060176-d8378800-7c4f-11ea-8998-ac1126332ba8.png">
  <p align="center">Table 5: Energy stocks in frequent itemsets</p>
</p>

Secondly, different stocks from the same company are most likely to appear in the same frequent itemset with a very high frequency. For example, GOOG and GOOGL are both stocks of Alphabet Inc, which is the parent company of Google. This result is obvious, but it reminds investors not to invest in them at the same time if they wish to reduce risks.
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060180-e71e3a80-7c4f-11ea-9592-778de18d63de.png">
  <p align="center">Table 6: Stocks from the same companies</p>
</p>

Thirdly, some stocks in different fields also tend to behave similarly. For example, although Google and Mastercard are not often mentioned together, they do often appear in the same itemsets with a frequency of 147.
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060187-fdc49180-7c4f-11ea-9fd0-eeaeea22b10c.png">
  <p align="center">Table 7: Stocks in different fields</p>
</p>

Finally, association rules with high confidence and interest are derived(The details can be found in [here](https://github.com/vincentsun870530/Soen499/tree/master/Association_Rule/results)).
</br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/23330950/79060191-0e750780-7c50-11ea-8cb6-7863cac1d4fc.png">
  <p align="center">Table 8: Example of Association Rule Output</p>
</p>


## Discussion

Although KNN results show acceptable accuracy, the model still has specific limitations. Firstly, according to Top-Down Forecasting theory[5], the original dataset references an economic growth period (2013-2017), which means that the model may not predict well in an economic recession period, such as at the present year (2020). Secondly, although we have used the K-Fold method to reduce the overfit problem, KNN did not generate an appropriate weight distribution among a time series dataset. As a result, this may limit the accuracy of the KNN model. Thirdly, the creation of financial indicators and labels depends on people’s knowledge of the stock market, and may vary with different reasoning, which also limit the accuracy of the model. Therefore, future improvement can focus on integrating more dataset with different economic conditions, adding different weight values to features chronologically, and working closely with financial experts to further improve the generation of features and labels.

Nevertheless, the association rules provide investors with a set of stocks demonstrating high confidence and interest. Defining “baskets” became a limitation when applying association rules to the stock market analysis. Originally, when association rules were used in grocery stores analysis, “baskets” were naturally selected by consumers; whereas in our model, they are selected by simply splitting the ranges for every day price change. As a result, the association rules model may result in disappointing outputs. For example, if too many “baskets” are defined, there will be few stocks that fit into the same “basket”, which outputs very few frequent itemsets, or even zero itemset. However, if there are too few “baskets” defined, based on the changing price range, too many stocks may fit into these “baskets”, which consequently will generate too many valueless frequent itemsets and association rules. In addition, not only will the result be unhelpful, but it will also make the program run extremely slow. Therefore, more financial knowledge about the stock market will be needed to build more reasonable “baskets” for future improvements.

## Reference
[1] "SPX Quote - S&P 500 Index - Bloomberg Markets." https://www.bloomberg.com/quote/SPX:IND. Accessed 14 Feb. 2020.

[2] "jealous/stockstats: Supply a wrapper ... - GitHub." https://github.com/jealous/stockstats. Accessed 10 Apr. 2020.

[3] "sklearn.neighbors.KNeighborsClassifier — scikit-learn 0.22.1 ...." http://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html. Accessed 14 Feb. 2020.

[4]  "Frequent Pattern Mining - Apache Spark." https://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html. Accessed 10 Apr. 2020

[5] (2019, June 25). A Top-Down Approach to Investing - Investopedia. Retrieved April 9, 2020, from https://www.investopedia.com/articles/stocks/06/topdownapproach.asp
