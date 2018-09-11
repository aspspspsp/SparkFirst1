#!/usr/bin/python
#-*-coding:utf-8 -*-

from pyspark import SQLContext
from Utils.SparkUtil import load_sentence_data_frame, get_conf
from Utils.HDFSUtil import WriteMetaToHDFS
import numpy as np
from pyspark.ml.param import TypeConverters
from pyspark.sql.types import Row

def train():
    sparkUrl = 'spark://ubuntu02:7077'

    file_path = 'hdfs://ubuntu02:9000/vectors/sentences_vector.csv'
    hdfs_url = 'http://ubuntu02:50070'
    user = 'hadoop'

    # 用來分桶的參數(愈小桶越多)
    r = 0.002

    # 所有句向量集合
    sc = get_conf(sparkUrl, 'LSH_train', "8g")
    df = load_sentence_data_frame(sc, file_path)

    # 隨機抽取一個向量v
    v = df.sample(False, 0.1, seed=0).rdd.first()['_vector']

    # 對每一個值算一個hash code: floor(dot(u,v) / r)
    tmp = df.rdd.flatMap(lambda x: {
        Row(x['id'],
            x['sentence'],
            x['vector'],
            TypeConverters.toInt(np.floor(x['_vector'].dot(v) / r)))
    })

    # 重新命名
    df = SQLContext(sc).createDataFrame(tmp) \
        .selectExpr("_1 as id",
                    "_2 as sentence",
                    "_3 as vector",
                    "_4 as hash_code")

    # 保存dataframe以加快速度
    df.persist()

    # 顯示各組分類情況
    summary = df.groupby("hash_code").count()
    summary.persist()

    # 取得所有桶的名稱
    names = summary.rdd.map(lambda x: x.hash_code).collect()

    # 歷遍每個分組
    for name in names:
        print('save to ' + str(name))

        tmp = df.filter(df['hash_code'] == name)

        # 刪除hash_code欄位以節省空間
        tmp = tmp.drop('hash_code')

        # 寫入hdfs(這個操作巨慢)
        tmp.toPandas().to_csv('/home/hadoop/new/' + str(name) + '.csv',
                              sep=',',
                              index=False,
                              encoding='utf-8')

    with open('/home/hadoop/new/meta.txt', 'w') as f:
        f.write('vector(v):\n')
        for e in v:
            f.write(str(e) + ',')

        f.write('\nnames:\n')
        for name in names:
            f.write(str(name) + ',')

    print('all done!!')
    return
train()