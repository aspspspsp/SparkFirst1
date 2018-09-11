#!/usr/bin/python
#-*-coding:utf-8 -*-

from Utils.RedisUtil import GetRedisCluster
from Utils.HDFSUtil import GetSentenceVectorsFromHDFS, GetMetaDataFromHDFS
import numpy as np
import pandas as pd
import requests

def LoadMetaData():
    file_path = '/new/meta.txt'
    hdfs_url = 'http://ubuntu02:50070'
    vect, names = GetMetaDataFromHDFS(file_path, hdfs_url, 'hadoop')
    return vect, names

def CleanCache():
    redisconn = GetRedisCluster()
    redisconn.flushall()
    return

def GetTopKSentence(k, sentence, vect, r, names):
    hdfs_url = 'http://ubuntu02:50070'
    user = 'hadoop'

    # 取得句向量(通過之前寫的flask API)
    def get_setence_vector(sentence):
        payload = {'sentence': sentence}
        req = requests.get('http://ubuntu01:5000/getSentenceVector', params=payload)
        vector = np.asarray(req.json()['sent_vec'].replace("[", '').replace(']', '').replace(' ', '').split(',')).astype(np.float)
        return vector
    vector = get_setence_vector(sentence)

    # 取得該向量的hash code
    def get_hash_code(vector, vect, r):
        hash_code = int(np.floor(vector.dot(vect) / r))
        return hash_code
    hash_code = get_hash_code(vector, vect, r)

    # redis連接
    redisconn = GetRedisCluster()

    # 取得topK
    result = list()
    while(len(result) < k and len(names) > 0):
        # 取得最接近hash code
        if hash_code not in names:
            hash_code = min(names, key=lambda x: abs(x - hash_code))

        # 將使用過的hash表去除
        names.remove(hash_code)
        hash_id = 'sent_' + str(hash_code)

        # 取得hash表(redis或者hdfs)
        def GetDf(hash_code, hash_id):
            df = redisconn.get(hash_id)
            if(df == None):
                df = GetSentenceVectorsFromHDFS('/new/' + str(hash_code) + '.csv', hdfs_url, user)
                redisconn.set(hash_id, df.to_json(orient='split'))
            else:
                df = pd.read_json(df, orient='split')
            return df
        df = GetDf(hash_code, hash_id)

        # 暫時用歐式距離計算相似度
        df['similarity'] = df['vector_'].map(lambda x: np.linalg.norm(vector - x))

        # 依照相似度來排序(歐式距離越小，則相似度越高)
        df.sort_values("similarity", inplace=True, ascending=True)

        # 取得topK
        topKs = df.head(k)['sentence'].values
        for topK in topKs:
            if (len(result) >= k):
                break
            result.append(topK)

    return result

r = 0.002
vect, names = LoadMetaData()
sentence = '杜邹等:疏水型阳离子水性聚氨醋的合成与性能研究1. 2. 2 膜的制备称取一定量WPU 乳液，将其倒在表面皿中，室温下放置7d ，放入80'
# for _ in range(1, 1000):
print(sentence)
print('=====================================')
result = GetTopKSentence(10, sentence, vect, r, names)
for res in result:
    print(res)