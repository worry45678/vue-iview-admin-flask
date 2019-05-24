from flask import jsonify, g, request
import json
from datetime import datetime
from . import scada
from app.auth import multi_auth, basic_auth, token_auth
from app import mongo, serializer
from app.models import Head, Parameter, allParaList

@scada.route('/paralist/')
@multi_auth.login_required
def paralist():
    return jsonify({'items': allParaList(), 'code': 20000})

@scada.route('/query/',methods=['POST'])
@multi_auth.login_required
def query():
    data = request.get_json()
    params = [i.split('-') for i in data['params']]
    date = [datetime.strptime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in data['date']]
    time = [datetime.strptime(i, '%Y-%m-%dT%H:%M:%S.000Z').time() for i in data['time']]
    print(params, date, time)
    return jsonify({'items': data, 'code': 20000})