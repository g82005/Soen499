# This python file is used to view the data visualization of the accuracy of the model

from one_stock_knn import knn

if __name__ == "__main__":
    # knn(stockName, showDataVisualization)
    # The current stock name is ZTS and could be replaced by any stock name available in the S&P 500
    # The showDataVisualization is a boolean, which when it is set to true, it will show the data visualization
    # knn("ZTS", True)
    knn("ZTS", False)