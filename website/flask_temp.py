#!/usr/bin/python
#-*-coding:utf-8 -*-

from flask import Flask
from flask_restful import Resource, Api
from flask import request
from werkzeug.utils import secure_filename
import numpy as np
import os
import json
import random
import datetime

app = Flask(__name__)
api = Api(app)

# 可以不用讓服務下線，就可以更新
class GetTopK(Resource):
    def get(self):
        sentence = request.args.get('sentence').encode('utf-8')
        return {'sentence': sentence}
api.add_resource(GetTopK, '/getTopK/')

# Get vector of sentence by restful API
class GetSentenceVector(Resource):
    def contructSentenceVector(self, sentence):
        from pyhanlp import *
        sent_vec = np.zeros(100)

        words = HanLP.segment(sentence)

        i = 0
        for _ in words:
            word = _.word.encode('utf-8')

            if (word in stopwords_set):
                continue

            if (word_vec_dict.has_key(word) == False):
                continue

            word_vec = word_vec_dict[word]

            sent_vec = sent_vec + word_vec
            i = i + 1

        if (i > 0):
            sent_vec = sent_vec / i

        return sent_vec.tolist()
    def get(self):
        sentence = request.args.get('sentence').encode('utf-8')
        sent_vec = self.contructSentenceVector(sentence)

        # convert numpy array to json format
        sent_vec = json.dumps(sent_vec)

        return {'sent_vec': sent_vec}
api.add_resource(GetSentenceVector, '/getSentenceVector')

class GetWordVector(Resource):
    def getWordVec(self, word):
        if(word not in word_vec_dict.keys()):
            return "None"
        return word_vec_dict[word]
    def get(self):
        word = request.args.get('word').encode('utf-8')
        word_vec = self.getWordVec(word)
        if(word_vec != "None"):
            # convert numpy array to json format
            word_vec = json.dumps(word_vec.tolist())
            return {'word_vec': word_vec}
        else:
            return {'error_msg': "就沒有咩"}

api.add_resource(GetWordVector, '/getWordVector')

# 要實驗一個圖片上傳的工具
class PutImageToHDFS(Resource):
    upload_folder_path = '/home/hadoop/upload_tmp'
    allowed_extensions = set(['png', 'jpg', 'jpeg', 'gif'])
    def allowed_file(self, filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in self.allowed_extensions
    def check_upload_folder(self, path):
        if(os.path.isdir(path) == False):
            os.mkdir(self.upload_folder_path, 0755)
            print("建立資料夾:", self.upload_folder_path)
    def generate_filename(self, upload_file):
        upload_filename = secure_filename(upload_file.filename) # 讓文檔名可以安全不被惡意執行

        _ = upload_filename.split('.')
        new_prefix = _[0] + "_" + str(datetime.datetime.now().strftime('%Y%m%dT%H%M')) + "_" + str(random.randint(1000, 9999))
        postfix = upload_filename.split('.')[len(_) - 1]
        return new_prefix + '.' + postfix
    def upload_file(self):
        if('file' not in request.files):
            return {'error_msg': "你沒上圖片啦~"}

        upload_file = request.files['file']
        if upload_file.filename == '':
            return {'error_msg': "你沒選圖片啦~"}
        if(self.allowed_file(upload_file.filename) == False):
            return {'error_msg': "檔案只支持jpg png gif"}

        # 檢查暫存資料夾是否存在，不存在則建立一個
        self.check_upload_folder(self.upload_folder_path)

        # 先上傳至本地暫存路徑
        upload_filename = self.generate_filename(upload_file)
        upload_path = os.path.join(self.upload_folder_path, upload_filename)
        upload_file.save(upload_path)

        # 將暫存檔存入hdfs
        msg = hdfsHelper.upload_img_to_hdfs(upload_path)

        return msg
    def post(self):
        msg = self.upload_file()

        return msg
    def get(self):
        return {'error_msg': "請用post方法"}
api.add_resource(PutImageToHDFS, '/putImageToHDFS')

# 載入中文支援
def Load_Chinese_Support():
    import sys
    default_encoding = 'utf-8'
    if sys.getdefaultencoding() != default_encoding:
        reload(sys)
        sys.setdefaultencoding(default_encoding)

hdfsHelper = None
word_vec_dict = None
stopwords_set = None
from hdfs_util import HdfsHelper
if __name__ == '__main__':

    Load_Chinese_Support()
    print "prepare for loading"
    hdfsHelper = HdfsHelper()

    stopwords_set = hdfsHelper.get_stopwords_from_hdfs()
    word_vec_dict = hdfsHelper.get_vec_from_hdfs()

    app.run(debug=True,
            host='ubuntu01')