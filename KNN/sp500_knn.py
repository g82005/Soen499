from one_stock_knn import knn
import pandas as pd
import os
import time

if __name__ == "__main__":
    """ list of s_anp_p companies """
    start = time.time()
    sp500_file = []
    for file in os.listdir("sp500_price"):
        sp500_file.append(file[:-4])

    sp500_code = []
    sp500_accuracy = []
    sp500_f1 = []
    sp500_kfold_accuracy = []
    sp500_k_fold_f1 = []
    for stock_code in sp500_file:
        sp500_code.append(stock_code)
        knnResults = knn(stock_code, False)
        # sp500_accuracy.append(knn(stock_code, False)[0])
        # sp500_f1.append(knn(stock_code, False)[1])
        sp500_accuracy.append(knnResults[0])
        sp500_f1.append(knnResults[1])
        sp500_kfold_accuracy.append(knnResults[2])
        sp500_k_fold_f1.append(knnResults[3])
    sp500_knn = {"Stock_code": sp500_code, "Accuracy": sp500_accuracy, "F1_score": sp500_f1,
                 "K-Fold_accuracy": sp500_kfold_accuracy, "K-Fold_f1_score": sp500_k_fold_f1}
    df = pd.DataFrame(sp500_knn, columns=['Stock_code', 'Accuracy', 'F1_score', 'K-Fold_accuracy', 'K-Fold_f1_score'])
    df.to_csv('results/sp500_knn.csv', index=False, header=True)
    print("Done")
    end = time.time()
    print("Runing time: %s s" % (end - start))
