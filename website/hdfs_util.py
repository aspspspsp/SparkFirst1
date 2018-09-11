#!/usr/bin/python
#-*-coding:utf-8 -*-
from hdfs import InsecureClient
import numpy as np

class HdfsHelper:
    HDFS_ADDR = ''
    STOPWORDS_PATH = ''
    VEC_PATH = ''
    IMG_FOLDER = ''

    # 關於雙namenode的backup處理方式，還需要再進行撰寫
    def __init__(self, hdfs_addr='http://ubuntu02:50070', stopwords_path='/stopwords/stopwords.txt', vec_path='/vectors/word_vector.vec', img_folder='/image'):
        self.HDFS_ADDR = hdfs_addr
        self.STOPWORDS_PATH = stopwords_path
        self.VEC_PATH = vec_path
        self.IMG_FOLDER = img_folder

    def upload_img_to_hdfs(self, upload_file_path):
        print "test",upload_file_path
        try:
            client = InsecureClient(self.HDFS_ADDR, user='hadoop')
            client.upload(self.IMG_FOLDER,
                          upload_file_path,
                          overwrite=False,
                          n_threads=0,
                          temp_dir=None,
                          chunk_size=65536,
                          progress=None,
                          cleanup=True)
        except NameError as n:
            print(n)
            return {'error_msg': 'HDFS 上傳失敗'}

        # 顯示圖片上傳hdfs的路徑
        _ = upload_file_path.split('/')
        path_for_show = self.IMG_FOLDER + "/" +_[len(_) - 1]
        return {'success': "上傳成功，hdfs路徑:" + path_for_show}

    def get_stopwords_from_hdfs(self):
        stopwords_set = set()

        print "loading stopwords..."
        try:
            client = InsecureClient(self.HDFS_ADDR, user='hadoop')
            with client.read(self.STOPWORDS_PATH, encoding="utf-8", delimiter="\n") as reader:
                for _ in reader:
                    stopwords_set.add(_.encode('utf-8'))
                print "done!!"

        except NameError as n:
            print "hdfs取得數據(停用詞)失敗", n
        return stopwords_set

    def get_vec_from_hdfs(self):
        word_vec_dict = dict()

        print "loading word vectors..."
        try:
            client = InsecureClient(self.HDFS_ADDR, user='hadoop')
            # delimiter="\n" 讀取分隔符為空格
            with client.read(self.VEC_PATH, encoding="utf-8", delimiter="\n") as reader:
                for _ in reader:
                    _ = _.split(' ')
                    key = _[0].encode('utf-8')
                    value = np.array(_[1:len(_) - 1]).astype(np.float)
                    word_vec_dict[key] = value

            print "done!!"
        except NameError as n:
            print "hdfs取得數據(詞向量)失敗", n

        return word_vec_dict
