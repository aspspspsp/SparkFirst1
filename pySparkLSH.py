#!/usr/bin/python
#-*-coding:utf-8 -*-

from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark import SparkContext, SQLContext, SparkConf
from math import sqrt
from numpy import array

def kmeans_training(master_url):
    sf = SparkConf()\
        .setMaster(master_url) \
        .setAppName("SparkSessionZipsExample") \
        .set("spark.executor.memory", "8g")

    sc = SparkContext(conf=sf)

    data = sc.textFile("hdfs://master32:9000/vectors/word_vector_sh.vec")

    def get_word_vec(line):
        x = []
        i = 0

        __ = line.split(" ")

        if (len(__) >= 100):
            for _ in __:
                if (i == 0):
                    i = 1
                    continue

                if (_ == ""):
                    continue

                x.append(float(_))
                i = i + 1
        else:
            for i in range(0, 100):
                x.append(float(0))
        return array(x)
    tmp = data.map(lambda line: get_word_vec(line.encode('utf-8')))
    df = SQLContext(sc).createDataFrame(tmp)
    df.show()

    return

if __name__ == '__main__':
    master_url = "spark://master32:7077"
    master_url = "local"
    kmeans_training(master_url)