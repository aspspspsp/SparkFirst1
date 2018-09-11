#!/usr/bin/python
#-*-coding:utf-8 -*-

from Utils.SparkUtil import get_conf, load_word_data_frame
from pyspark.ml.feature import BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel

def get_model_save_path(savePath):
    brp_path = savePath + '/brp'
    model_path = savePath + '/model'

    return brp_path, model_path

def train(sparkUrl, dataForTrainPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_train', "8g")
    df = load_word_data_frame(sc, dataForTrainPath)

    # 開始訓練模型
    brp = BucketedRandomProjectionLSH() \
        .setBucketLength(BUCKET_LENGTH) \
        .setNumHashTables(NUM_HASH_TABLES) \
        .setInputCol("vector") \
        .setOutputCol("hash")
    model = brp.fit(df)

    # 存儲模型
    brp.save(brp_path)
    model.save(model_path)

    # 顯示大概結果
    model.transform(df).show()

# LSH model parameter
BUCKET_LENGTH = 1000.0
NUM_HASH_TABLES = 100
def main():
    local = False
    sparkUrl = 'spark://ubuntu02:7077'
    dataForTrainPath = 'hdfs://ubuntu02:9000/vectors/word_vector.vec'
    # dataForVaildPath = 'hdfs://master32:9000/pdfs/output_2018.csv'

    savePath = 'hdfs://ubuntu02:9000/outout/word_models'

    if local == True:
        sparkUrl = 'local'

    train(sparkUrl, dataForTrainPath, savePath)
main()