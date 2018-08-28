#!/usr/bin/python
#-*-coding:utf-8 -*-
import os
import requests
import csv
import re

def foreach_dict(root, output_path):
    paths = [x[0] for x in os.walk(root)]

    for path in paths:
        files = os.listdir(path)
        for file in files:
            fp = path + "/" + file
            if os.path.isfile(fp) == True:
                dict = read_file(fp)
                write_to_csv(file, dict, output_path)
                os.remove(fp)

    print("all done !!")

def write_to_csv(file, _dict, output_path):
    id = file.replace(".txt", "")
    with open(output_path, 'aw') as csvfile:
        print ("write:" + file)
        # 建立 CSV 檔寫入器
        writer = csv.writer(csvfile)
        for key in _dict.keys():
            _dict[key] = _dict[key].replace(",", " ")
            writer.writerow([id, key, _dict[key]])

        print ("write_done:" + file)

def write_first_line(output_path):
    with open(output_path, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['id', 'sentence', 'vector'])

def read_file(file_path):
    _dict = dict()

    with open(file_path, 'r') as f:
        print ("open:" + file_path)
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n')\
                .replace("\r", "") \
                .replace(',', '，') \
                .replace('"', '＂')

            sent_vect = get_sentence_vector(line)
            _dict[line] = sent_vect
    print ("read_done:" + file_path)
    return _dict

def get_sentence_vector(sentence):
    param = {'sentence': sentence}
    r = requests.get('http://master32:5000/getSentenceVector', param)

    if r.status_code == requests.codes.ok:
        return r.json()['sent_vec']

    return None

# _dict = dict()
# _dict['aaa'] = [0,0,0,0,0,0]
# write_to_csv(file="aa", _dict=_dict, output_path="/home/hadoop/output.csv" )

root = "/home/hadoop/pys/PDFReorganization"
output_path="/home/hadoop/output.csv"
# write_first_line(output_path)
foreach_dict(root=root, output_path=output_path)