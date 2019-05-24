from flask import jsonify, g
from . import scada
from app.auth import multi_auth, basic_auth, token_auth
from app import mongo, serializer
from app.models import Head, Parameter, allParaList
from datetime import datetime

d = [
    {'name': '灭火器1', 'isCheck': True, 'date': datetime(2019, 5, 1)},
    {'name': '灭火器1', 'isCheck': True, 'date': datetime(2019, 5, 1)},
    {'name': '灭火器1', 'isCheck': True, 'date': datetime(2019, 4, 1)},
    {'name': '灭火器1', 'isCheck': False, 'date': datetime(2019, 5, 12)},
    {'name': '灭火器1', 'isCheck': False, 'date': datetime(2019, 5, 10)},
    {'name': '灭火器1', 'isCheck': True, 'date': datetime(2018, 10, 11, 22, 30, 0)}]

@scada.route('/test/')
def test():
    return jsonify({'data': d})
 