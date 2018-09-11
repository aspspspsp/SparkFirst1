#!/usr/bin/python
#-*-coding:utf-8 -*-

from Utils.SparkUtil import get_conf, load_sentence_data_frame
from pyspark.ml.feature import BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel
from pyspark.ml.linalg import DenseVector
from pyspark.ml import Pipeline
from pyspark2pmml import PMMLBuilder

def get_model_save_path(savePath):
    brp_path = savePath + '/brp'
    model_path = savePath + '/model'

    return brp_path, model_path

# 需要載入PMML的JAR才能導出
def train_forPMML(sparkUrl, dataForTrainPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_train', "8g")
    df = load_sentence_data_frame(sc, dataForTrainPath)

    # 開始訓練模型
    brp = BucketedRandomProjectionLSH() \
        .setBucketLength(BUCKET_LENGTH) \
        .setNumHashTables(NUM_HASH_TABLES) \
        .setInputCol("vector") \
        .setOutputCol("hash")

    # 流水線: 先提取特徵, 再訓練模型
    pipeline = Pipeline(stages=[brp])
    pipeline_model = pipeline.fit(df)

    # 顯示大概結果
    # pipeline_model.transform(df).show()
    # 存儲模型至PMML
    pmmlBuilder = PMMLBuilder(sc, df, pipeline_model)
    pmmlBuilder.buildFile("~/pmmlModels/SM.pmml")
    return

def train_forSpark(sparkUrl, dataForTrainPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_train', "8g")
    df = load_sentence_data_frame(sc, dataForTrainPath)

    # 開始訓練模型
    brp = BucketedRandomProjectionLSH() \
        .setBucketLength(BUCKET_LENGTH) \
        .setNumHashTables(NUM_HASH_TABLES) \
        .setInputCol("vector") \
        .setOutputCol("hash")

    model = brp.fit(df)

    model.transform(df).show()

    # # 存儲模型
    # brp.save(brp_path)
    # model.save(model_path)
    return

def validForSpark(sparkUrl, dataForTrainPath, dataForVaildPath, savePath):
    # 取得模型存儲路徑
    brp_path, model_path = get_model_save_path(savePath)

    # 載入數據
    sc = get_conf(sparkUrl, 'LSH_valid', "8g")
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
BUCKET_LENGTH = 1000.0
NUM_HASH_TABLES = 100

def main():
    local = True
    sparkUrl = 'spark://ubuntu02:7077'
    dataForTrainPath = 'hdfs://ubuntu02:9000/vectors/sentences_vector_2018.csv'
    dataForVaildPath = 'hdfs://ubuntu02:9000/vectors/sentences_vector_2018.csv'

    savePath = 'hdfs://ubuntu02:9000/outout/sentence_models'

    if local == True:
        sparkUrl = 'local'

    train_forPMML(sparkUrl, dataForTrainPath, savePath)
    # valid(sparkUrl, dataForTrainPath, dataForVaildPath, savePath)
main()