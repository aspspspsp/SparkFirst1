#!/usr/bin/python
#-*-coding:utf-8 -*-

from __future__ import print_function
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

def svm_training(master_url):
    # define spark application name, spark master url and spark executor's maximum memory size.
    sf = SparkConf()\
        .setMaster(master_url) \
        .setAppName("Spark SVM tutorial") \
        .set("spark.executor.memory", "8g")

    sc = SparkContext(conf=sf)

    # load iris dataset from csv file in hdfs
    data = SQLContext(sc).read.format('com.databricks.spark.csv') \
            .options(header='true', inferschema='true') \
            .load('hdfs://master32:9000/datasets/Iris_Dataset.csv')

    # convert string label to numeral form
    indexer = StringIndexer(inputCol="Species", outputCol="categoryIndex")
    data = indexer.fit(data).transform(data)

    print(type(data))
    #
    # data.show()

    # exit()

    # caching data which can speed up a whole training process
    # data.cache()
    lr = LogisticRegression(maxIter=10)
    # train
    model = lr.fit(data)

    # save SVM model to hdfs drive.
    svm.save(sc, "hdfs://master32:9000/result/svm100.txt")
    return

if __name__ == '__main__':
    master_url = "spark://master32:7077"
    master_url = "local"
    svm_training(master_url)