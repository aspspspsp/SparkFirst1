#!/usr/bin/python
#-*-coding:utf-8 -*-
from pyspark.sql import SQLContext
from pyspark.ml.linalg import Vectors
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import BucketedRandomProjectionLSH

from pyhdfs import HdfsClient
import pandas as pd
import numpy as np

def read_csv():
    client = HdfsClient(hosts='master33:50070', user_name='hadoop')
    inputfile = client.open('/pdfs/output.csv')
    df = pd.read_csv(inputfile)

    print("read done")

    # 將vector string 轉換為 vector
    def transfer_vectorStr_to_vector(df):
        for i in range(0, len(df['vector'])):
            array = np.fromstring( \
                df['vector'][i].replace('[', '').replace(']', ''), \
                dtype=np.double, sep='  ')

            df.at[i, 'vector'] = Vectors.dense(array)
        return df
    df = transfer_vectorStr_to_vector(df)

    return df

def train_lsh_model():
    sf = SparkConf()\
            .setMaster("local") \
            .setAppName("Spark SVM tutorial") \
            .set("spark.executor.memory", "8g")
    sc = SparkContext(conf=sf)
    df = read_csv()

    sdf = SQLContext(sc).createDataFrame(df)

    brp = BucketedRandomProjectionLSH() \
         .setBucketLength(50.0) \
         .setNumHashTables(3) \
         .setInputCol("vector") \
         .setOutputCol("hash")

    model = brp.fit(sdf)
    model.transform(sdf).show()

    model.approxSimilarityJoin(sdf, sdf, 1.5)
train_lsh_model()
