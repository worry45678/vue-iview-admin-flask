from flask import json
from bson.objectid import ObjectId
from datetime import time, date
import pandas as pd

def trueReturn(data, msg):
    return {
        "status": True,
        "data": data,
        "msg": msg
    }


def falseReturn(data, msg):
    return {
        "status": False,
        "data": data,
        "msg": msg
    }

# 数据清洗方法
def data_clean(df):
    df2 = df.describe()
    df2.loc['iqr'] = df2.loc['75%'] - df2.loc['25%']
    mi = df2.loc['25%']-2.5*df2.loc['iqr']
    ma = df2.loc['75%']+2.5*df2.loc['iqr']
    return df[(df< ma)&(df> mi)]

# 自定义json序列化转化ObjectId方法
class ObjectIdEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o,ObjectId):
            return str(o)
        if isinstance(o, time):
            return o.strftime('%H:%M')
        if isinstance(o, date):
            return o.strftime('%Y-%m-%d %H:%M')
        return json.JSONEncoder.default(self,o)
