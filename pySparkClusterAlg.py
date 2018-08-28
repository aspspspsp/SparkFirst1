#!/usr/bin/python
#-*-coding:utf-8 -*-

from __future__ import print_function
from pyspark.mllib.clustering import KMeans
from pyspark import SparkContext, SparkConf
from math import sqrt
from numpy import array

def kmeans_training(master_url):
    sf = SparkConf()\
        .setMaster(master_url) \
        .setAppName("SparkSessionZipsExample") \
        .set("spark.executor.memory", "8g")

    sc = SparkContext(conf=sf)

    data = sc.textFile("hdfs://master32:9000/vectors/word_vector.vec")

    parsedData = data.map(lambda line: get_word_vec(line.encode('utf-8')))
    parsedData.cache() # 建立緩存

    clusters = KMeans.train(parsedData, 100, maxIterations=200, initializationMode="random")

    def error(point):
         center = clusters.centers[clusters.predict(point)]
         return sqrt(sum([x ** 2 for x in (point - center)]))

    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))

    clusters.save(sc, "hdfs://master33:9000/result/kmenas100.txt")
    return

def get_word_vec(line):
    x = []
    i = 0

    __ = line.split(" ")

    if(len(__) >= 100):
        for _ in __:
            if(i == 0):
                i = 1
                continue

            if(_ == ""):
                continue

            x.append(float(_))
            i = i + 1
    else:
        for i in range(0, 100):
            x.append(float(0))
    return array(x)

if __name__ == '__main__':
    master_url = "spark://master32:7077"
    # master_url = "local"
    kmeans_training(master_url)