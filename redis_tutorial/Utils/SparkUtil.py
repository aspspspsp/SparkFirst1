#!/usr/bin/python
#-*-coding:utf-8 -*-

from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.types import Row, StructType, StructField, StringType, DataType
from pyspark.sql.functions import split, regexp_replace
from pyspark.ml.linalg import Vectors

def get_conf(sparkUrl, appName, memSize):
    # define spark application name, spark master url and spark executor's maximum memory size.
    sf = SparkConf() \
        .setMaster(sparkUrl) \
        .setAppName(appName) \
        .set("spark.executor.memory", memSize)

    sc = SparkContext(conf=sf)

    return sc

# 載入句向量
def load_sentence_data_frame(sc, dataPath):
    df = SQLContext(sc).read.format('com.databricks.spark.csv') \
        .options(header='true', inferschema='true') \
        .load(dataPath)

    # 複製欄位(vector)
    df = df.withColumn("_vector", df['vector'])

    # 去除_vector的 [ 以及 ]
    df = df.select(df['id'], df['sentence'], df['vector'], regexp_replace(df['_vector'], "[\]\[]", "").alias("_vector"))

    # 分割_vector字串並且轉型
    df = df.select(df['id'], df['sentence'], df['vector'], split(df['_vector'], "  ").cast("array<double>").alias("_vector"))

    # 將double轉換為vectory再轉換為numpy array
    tmp = df.rdd.flatMap(lambda x: {
        Row(x['id'], x['sentence'], x['vector'], Vectors.dense(x['_vector']))
    })

    # 再轉換為dataframe
    df = SQLContext(sc).createDataFrame(tmp)\
            .selectExpr("_1 as id",
                        "_2 as sentence",
                        "_3 as vector",
                        "_4 as _vector")

    # 回傳dataframe
    return df

# 載入詞向量
def load_word_data_frame(sc, dataPath):
    tmp = sc.textFile(dataPath)
    tmp = tmp\
        .filter(lambda line: len(line.split(' ')) > 2)\
        .flatMap(lambda line: {
            Row(line.split(' ')[0],
                Vectors.dense(line.encode('utf-8').split(' ')[1:len(line.encode('utf-8').split(' '))-1]))
        })
    df = SQLContext(sc)\
            .createDataFrame(tmp)\
            .selectExpr("_1 as word",
                        "_2 as vector")
    return df