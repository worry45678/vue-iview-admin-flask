import os

SECRET_KEY = 'my sercet key'
MONGO_DBNAME = 'data_user'
MONGO_URI = 'mongodb://root:123456@39.104.64.142:27017/data_user'

SQLALCHEMY_DATABASE_URI = 'mssql+pymssql://yang:yangfan10241022@192.168.0.51/TapWater?charset=cp936'
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_BINDS = {
    'waterdd': 'mysql+pymysql://root:123456@192.168.222.100/waterdd?charset=utf8',
    'zmms': 'mysql+pymysql://root:123456@192.168.222.100/zmms?charset=utf8'
}
JSON_AS_ASCII = False

if os.getenv('PYTHON_CONFIG') == 'office':
    ZMMS_WEB_API = 'http://192.168.222.90:8899/' 
else:
    ZMMS_WEB_API = 'http://192.168.222.90:8899/' 
    #ZMMS_WEB_API = 'http://222.191.224.42:8899/'