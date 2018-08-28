#!/usr/bin/python
#-*-coding:utf-8 -*-

import pandas as pd

from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn2pmml import PMMLPipeline, sklearn2pmml

iris = load_iris()

# 創建帶有特徵名稱的 DataFrame
iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)

# 創建模型管道
iris_pipeline = PMMLPipeline([
    ("classifier", RandomForestClassifier())
])

# 訓練模型
iris_pipeline.fit(iris_df, iris.target)

# 導出模型到 RandomForestClassifier_Iris.pmml 文件
sklearn2pmml(iris_pipeline, "RandomForestClassifier_Iris.pmml")