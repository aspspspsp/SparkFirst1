#!/usr/bin/python
#-*-coding:utf-8 -*-

from Utils.SparkUtil import get_conf, load_sentence_data_frame
from pyspark.ml.feature import BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel
from pyspark.ml.linalg import DenseVector

def get_model_save_path(savePath):
    brp_path = savePath + '/brp'
    model_path = savePath + '/model'

    return brp_path, model_path

def train(sparkUrl, dataForTrainPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_train', "16g")
    df = load_sentence_data_frame(sc, dataForTrainPath)

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
    return

def valid(sparkUrl, dataForTrainPath, dataForVaildPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_valid', "4g")
    dft = load_sentence_data_frame(sc, dataForTrainPath)
    dfv = load_sentence_data_frame(sc, dataForVaildPath)

    dft.cache()

    # 載入舊有模型
    brp = BucketedRandomProjectionLSH.load(brp_path)
    model = BucketedRandomProjectionLSHModel.load(model_path)

    sets = dfv.rdd.map(lambda x: {
        x['sentence'], x['vector']
    }).collect()

    # write result to file
    for set in sets:
        readFalse = False
        sent = None
        vect = None
        for element in set:
            if isinstance(element, DenseVector) == True and vect == None:
                vect = element
            elif  isinstance(element, DenseVector) == True and vect != None:
                print('vect_error', set)
                readFalse = True
            if isinstance(element, unicode) == True and sent == None:
                sent = element
            elif isinstance(element, unicode) == True and sent != None:
                print('sent_error', set)
                readFalse = True

        if sent == None or vect == None:
            readFalse = True

        if readFalse == True:
            print('read false')
            break

        print('=================================')
        print(sent)
        print('=================================')
        res = model.approxNearestNeighbors(dft, vect, 5)
        s_s = res.select('sentence').rdd.collect()
        for s in s_s:
            print(s['sentence'])
        print('************************************')

    return

# LSH model parameter
BUCKET_LENGTH = 50.0
NUM_HASH_TABLES = 100

def main():
    local = True
    sparkUrl = 'spark://master32:7077'
    dataForTrainPath = 'hdfs://master32:9000/pdfs/output.csv'
    dataForVaildPath = 'hdfs://master32:9000/pdfs/output_2018.csv'

    savePath = 'hdfs://master32:9000/outout/models'

    if local == True:
        sparkUrl = 'local'

    # train(sparkUrl, dataForTrainPath, savePath)
    valid(sparkUrl, dataForTrainPath, dataForVaildPath, savePath)
main()