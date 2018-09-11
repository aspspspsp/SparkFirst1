#!/usr/bin/python
#-*-coding:utf-8 -*-

from hdfs import InsecureClient
import pandas as pd
import numpy as np

def GetSentenceVectorsFromHDFS(file_path, hdfs_url, user):
    hdfs_client = InsecureClient(hdfs_url, user=user)
    with hdfs_client.read(file_path) as reader:
        df = pd.read_csv(reader)

        # 文字記錄的向量轉換為以numpy紀錄
        df['vector_'] = df['vector'].map(lambda x:
                np.asarray(x.replace("]", '')
                            .replace("[", '')
                            .split('  '))
                            .astype(np.float)
        )
        return df
    return

def WriteMetaToHDFS(v, names, mata_data_path, hdfs_url, user):
    hdfs_client = InsecureClient(hdfs_url, user=user)
    with hdfs_client.write(mata_data_path) as writer:
        writer.write('v:')

        tmp = ''
        for float in v:
            tmp = tmp + str(float) + ','
        writer.write(tmp)

        tmp = ''
        writer.write('names:')
        for name in names:
            tmp = tmp + str(name) + ','
        writer.write(tmp)

def GetMetaDataFromHDFS(mata_data_path, hdfs_url, user):
    vect = None
    names = set()
    def remove_last_comma(line):
        return line[0:len(lines[1])-2]

    hdfs_client = InsecureClient(hdfs_url, user=user)

    with hdfs_client.read(mata_data_path) as reader:
        lines = reader.readlines()

        vect = np.asarray(remove_last_comma(lines[1]).split(',')).astype(np.float)
        for name in remove_last_comma(lines[3]).split(','):
            if(name == ''):
                continue
            names.add(int(name))

    return vect, names