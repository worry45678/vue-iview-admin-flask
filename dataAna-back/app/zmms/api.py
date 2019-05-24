from flask import jsonify, g, request
import os
from dateutil import parser, tz
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import json
from . import zmms
from .models import ZMMSModel
from app.auth import multi_auth, basic_auth, token_auth
from app import mongo, serializer, photos
from bson.objectid import ObjectId
from app.config import ZMMS_WEB_API
from app.common import data_clean
from app.models import Head, Parameter, Node
from sklearn.linear_model import LinearRegression

ZMMSModel = ZMMSModel(ZMMS_WEB_API)


@zmms.route('/meter_configs/')
@multi_auth.login_required
def meter_configs():
    if request.args.get('zone'):
        zone = {'zone_id': ObjectId(request.args.get('zone'))}
    else:
        zone = {}
    pagesize = int(request.args.get('pagesize'))
    skip = (int(request.args.get('page')) - 1) * int(pagesize)
    # 指定分区包含的终端列表
    cid = [i['CID'] for i in mongo.db.zone_rtu.find(zone)]
    # CID_LIST对应的所有表详情
    rtu_list = list(mongo.db.meter_configs.find(
        {'CID': {'$in': cid}}).limit(pagesize).skip(skip))
    total = len(cid)
    return jsonify({'items': rtu_list, 'total': total})


@zmms.route('/meter_config/<string:cid>/', methods=['GET', 'PUT'])
@multi_auth.login_required
def meter_config(cid):
    if request.method == 'GET':
        meter = mongo.db.meter_configs.find_one({'CID': cid})
        meter_images = mongo.db.meter_images.find(
            {'meterID': meter.pop('_id')}, {'_id': 0, 'meterID': 0})
        # print(list(meter_images))
        return jsonify({'item': meter, 'code': 20000, 'images': list(meter_images)})
    else:
        data = json.loads(request.data)
        mongo.db.meter_configs.update_one({'CID': cid}, {'$set': data})
        return jsonify({'item': data, 'code': 20000})


@zmms.route('/upload/<string:cid>/', methods=['POST'])
@multi_auth.login_required
def upload_image(cid):
    name = f'{cid}-{datetime.now().strftime("%Y-%m-%d")}.jpg'
    meter = mongo.db.meter_configs.find_one({'CID': cid})
    filename = photos.save(request.files['photo'], name=name)
    mongo.db.meter_images.insert_one(
        {'name': filename, 'uploadtime': datetime.now(), 'meterID': meter['_id'], 'CID': cid})
    return jsonify({'code': 20000, 'msg': 'ok', 'name': filename})


@zmms.route('/remove_image/', methods=['DELETE'])
@multi_auth.login_required
def remove_image():
    data = json.loads(request.data)
    mongo.db.meter_images.remove({'name': data['name']})
    from run import app
    os.remove(os.path.join(app.root_path, 'static', 'media', data['name']))
    return jsonify({'code': 20000, 'msg': 'test'})


@zmms.route('/all_rtus/', methods=['GET'])
@multi_auth.login_required
def all_rtus():
    return jsonify({'all_rtus': ZMMSModel.get_all_rtus()})


@zmms.route('/flowrate/', methods=['GET', 'POST'])
@multi_auth.login_required
def flowrate():
    """
    查询单个终端的详细日水量，包含实际值value，修正值fix, 移动平均average
    """
    params = json.loads(request.data)
    date_s, date_e = parser.parse(params['date'][0]).astimezone(tz.tzlocal()).strftime(
        '%Y-%m-%d'), parser.parse(params['date'][1]).astimezone(tz.tzlocal()).strftime('%Y-%m-%d')
    rtuid = mongo.db.meter_configs.find_one(
        {'CID': params['CID']}, {'RTU_ID': 1})['RTU_ID']
    df = ZMMSModel.rtu_flowrate(rtuid, date_s, date_e)
    df['average'] = df['value'].rolling(3).mean().round(0)
    df['date'] = df.index
    df.fillna(0, inplace=True)  # 有Nan在前端无法转化为对象
    df['value'] = df.value.astype(float) - df.fix.astype(float)
    return jsonify({'index': df.index.tolist(), 'columns': df.columns.tolist(), 'table': df.to_dict('records')})


@zmms.route('/rtus_flowrate/', methods=['GET', 'POST'])
@multi_auth.login_required
def rtus_flowrate():
    """
    查询指定分区包含的所有流量计的水量信息,日数据，无需清洗
    """
    params = json.loads(request.data)
    date_s, date_e = parser.parse(params['date'][0]).astimezone(tz.tzlocal()).strftime(
        '%Y-%m-%d'), parser.parse(params['date'][1]).astimezone(tz.tzlocal()).strftime('%Y-%m-%d')
    #df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find({'zone_id': ObjectId(params['zone'])})))
    if len(params['zone']) == 1:
        df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find(
            {'zone_id': ObjectId(params['zone'][0])})))
    elif len(params['zone']) == 2:
        templist = [i['rtu_name'] for i in list(mongo.db.zone_rtu.find(
            {'zone_id': ObjectId(params['zone'][0])}, {'_id': 0, 'rtu_name': 1}))]
        df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find(
            {'zone_id': ObjectId(params['zone'][1]), 'rtu_name': {'$in': templist}})))
    temp = ZMMSModel.get_rtuid(df_rtus.CID.tolist())
    df_rtus.index = df_rtus.CID.apply(lambda x: temp[x])
    df_rtus.index.name = 'rtuid'
    # 查询所有终端流量
    df = ZMMSModel.rtus_flowrate(df_rtus.index.tolist(
    ), date_s, date_e, datatype=params['datatype'], cycle=params['cycle'])
    rename = {i: df_rtus.loc[i, 'rtu_name'] for i in df_rtus.index}
    df = df.rename(columns=rename)
    df['date'] = df.index
    df.fillna(0, inplace=True)  # 有Nan在前端无法转化为对象
    return jsonify({'index': df.index.tolist(), 'columns': df.columns.tolist(), 'table': df.to_dict('records')})


@zmms.route('/zone_list/', methods=['GET'])
@multi_auth.login_required
def zone_list():
    """
    获取指定级别的分区列表
    """
    zone_list = list(mongo.db.zone_configs.find())
    return jsonify({'zone_list': zone_list})


@zmms.route('/zone_rtus/<string:zoneid>/', methods=['GET'])
@multi_auth.login_required
def zone_rtus(zoneid):
    """
    返回指定分区包含的终端列表 [{'name'：rtu名称, 'direction'： 方向, 'CID': 客户编号},……]
    """
    s = zoneid.split(',')
    if len(s) == 1:
        zone_rtus = [{'rtu_name': i['rtu_name'], 'direction':i['direction'], 'CID': i['CID'],
                      'multiplicator': i['multiplicator']} for i in mongo.db.zone_rtu.find({'zone_id': ObjectId(s[0])})]
    elif len(s) == 2:
        templist = [i['rtu_name'] for i in list(mongo.db.zone_rtu.find(
            {'zone_id': ObjectId(s[0])}, {'_id': 0, 'rtu_name': 1}))]
        zone_rtus = [{'rtu_name': i['rtu_name'], 'direction':i['direction'], 'CID': i['CID'], 'multiplicator': i['multiplicator']}
                     for i in mongo.db.zone_rtu.find({'zone_id': ObjectId(s[1]), 'rtu_name':{'$in': templist}})]
    return jsonify({'zone_rtus': zone_rtus})


@zmms.route('/zone_flowrate/', methods=['GET', 'POST'])
@multi_auth.login_required
def zone_flowrate():
    """
    查询指定的多个分区的流量
    接受参数：zone_list, daterange, datatype, cycle
    """
    params = json.loads(request.data)
    date_s, date_e = parser.parse(params['date'][0]).astimezone(tz.tzlocal()).strftime(
        '%Y-%m-%d'), parser.parse(params['date'][1]).astimezone(tz.tzlocal()).strftime('%Y-%m-%d')
    df = ZMMSModel.zone_flowrate(
        params['zone_list'], date_s, date_e, datatype=params['datatype'], cycle=params['cycle'])
    se = df.sum()
    se_percent = (se / se.sum()).round(3)
    sum = pd.concat([se, se_percent], axis=1).T
    return jsonify({'values': df.to_dict('list'), 'index': df.index.tolist(), 'columns': df.columns.tolist(), 'sum': sum.to_dict('records')})


@zmms.route('/xiuzheng/', methods=['POST'])
@multi_auth.login_required
def xiuzheng():
    """
    修正日水量数据
    """
    params = json.loads(request.data)
    r = ZMMSModel.fix_flowrate(
        date=params['RTIME'], CID=params['CID'], value=params['value'])
    return jsonify(r.text)


@zmms.route('/multi_day_rtu/', methods=['POST'])
@multi_auth.login_required
def multi_day_rtu():
    """
    多日单点横向对比数据
    """
    params = json.loads(request.data)
    date_e = parser.parse(params['date']).astimezone(
        tz.tzlocal()) + timedelta(days=1)
    date_s = date_e - timedelta(days=params['period'])
    df = ZMMSModel.get_hour_data(params['cid'], 105, date_s.strftime(
        '%Y-%m-%d'), date_e.strftime('%Y-%m-%d'))
    if df is None:
        return jsonify({'error': '该终端无数据'})
    if params['dataclean']:
        df = data_clean(df)
    df['day'] = df.index.date
    df['day'] = df.day.apply(lambda x: x.strftime('%Y-%m-%d'))
    df['time'] = df.index.time

    df = df.pivot_table(columns='day', index='time',
                        values=df.columns[0]).fillna('')

    # 计算预测线
    #model = mongo.db.predict_model.find_one()['model']
    # if model.get(params['cid']):
    #    wf = Head.DataFrame([(1,121),(4,121),(6,121),(7,121)], date_e-timedelta(days=1), date_e-timedelta(days=1), 'H')
    #    wf = wf.shift(1)
    #    wf.columns= ['中桥', '锡东', '锡澄', '雪浪']
    #    wf.index = wf.index.time
    #    df['预测'] = (wf[['锡澄', '雪浪', '中桥', '锡东']] * model[params['cid']][0:4]).sum(axis=1) + model[params['cid']][4]

    predict_model = mongo.db.predict_model2.find_one(
        {'cid': params['cid'], 'status': 'actived'})
    if predict_model is not None:
        model = predict_model['model']
        wf = Head.DataFrame([(1, 121), (4, 121), (6, 121), (7, 121)],
                            date_e-timedelta(days=1), date_e-timedelta(days=1), 'H')
        wf = wf.shift(1)
        wf.index = wf.index.time
        wf.columns = ['中桥', '锡东', '锡澄', '雪浪']
        df['预测'] = (wf[['锡澄', '雪浪', '中桥', '锡东']] *
                    model[0:4]).sum(axis=1) + model[4]

    df.fillna('', inplace=True)

    return jsonify({'index': df.index.tolist(), 'columns': df.columns.tolist(), 'table': df.to_dict('records')})


@zmms.route('/day_rtu/', methods=['POST'])
@multi_auth.login_required
def day_rtu():
    """
    单点数据查询
    """
    params = json.loads(request.data)
    date_s, date_e = parser.parse(params['date_s']).astimezone(tz.tzlocal()).strftime(
        '%Y-%m-%d'), parser.parse(params['date_e']).astimezone(tz.tzlocal()).strftime('%Y-%m-%d')
    df = ZMMSModel.get_data(
        params['cid'], params['datatype'], date_s, date_e, 'hour')
    if df is None:
        return jsonify({'error': '该终端无数据'})
    if params['dataclean']:
        df = data_clean(df)
    df2 = df.resample('D').min().round(2)
    df2.columns = ['日最小流量']
    df2['日均流量'] = df.resample('D').mean().round(2)
    df2['日最大流量'] = df.resample('D').max().round(2)
    df2 = df2.head(-1)
    df2.fillna('', inplace=True)
    return jsonify({'index': df2.index.tolist(), 'columns': df2.columns.tolist(), 'table': df2.to_dict('records')})


@zmms.route('/day_zone/', methods=['POST'])
@multi_auth.login_required
def day_zone():
    """
    单分区数据查询
    """
    params = json.loads(request.data)
    date_s, date_e = parser.parse(params['date_s']).astimezone(tz.tzlocal()).strftime(
        '%Y-%m-%d'), parser.parse(params['date_e']).astimezone(tz.tzlocal()).strftime('%Y-%m-%d')
    df = ZMMSModel.get_data(
        params['cid'], params['datatype'], date_s, date_e, 'hour')
    if df is None:
        return jsonify({'error': '该终端无数据'})
    df = data_clean(df)
    df2 = df.resample('D').min().round(2)
    df2.columns = ['日最小流量']
    df2['日均流量'] = df.resample('D').mean().round(2)
    df2['日最大流量'] = df.resample('D').max().round(2)
    df2 = df2.head(-1)
    return jsonify({'index': df2.index.tolist(), 'columns': df2.columns.tolist(), 'table': df2.to_dict('records')})


@zmms.route('/rtu_check_log/<string:cid>/', methods=['GET', 'POST'])
@multi_auth.login_required
def rtu_check_log(cid):
    """
    远传终端预测报警设置和检查记录
    """
    if request.method == 'GET':
        config = mongo.db.predict_model2.find_one(
            {'cid': cid, 'status': 'actived'})
        if config is None:
            config = {'cid': cid,
                      'date': '',
                      'type': '',
                      'status': '',
                      'model': '',
                      'content': '无预测模型'}
        return jsonify({'data': list(mongo.db.rtu_check.find({'CID': cid})), 'config': config})
    else:
        params = json.loads(request.data)
        params['q_date'] = parser.parse(params['q_date']).astimezone(
            tz.tzlocal()).strftime('%Y-%m-%d')
        params['r_date'] = parser.parse(params['r_date']).astimezone(
            tz.tzlocal()).strftime('%Y-%m-%d')
        if not params['complete']:
            params.pop('_id')
            mongo.db.rtu_check.insert_one(params)
        else:
            oid = ObjectId(params.pop('_id'))
            mongo.db.rtu_check.update_one(
                {'_id': oid}, {'$set': params}, upsert=True)
        return jsonify({'data': list(mongo.db.rtu_check.find({'CID': cid}))})


@zmms.route('/zone_configs/', methods=['GET', 'POST'])
@multi_auth.login_required
def zone_config():
    """
    管理分区配置
    """
    if request.method == 'GET':
        return jsonify({'data': list(mongo.db.zone_configs.find())})
    else:
        params = json.loads(request.data)


@zmms.route('/zone_rtus_relationship/<string:zoneid>/', methods=['POST'])
@multi_auth.login_required
def zone_rtus_relationship(zoneid):
    """
    管理分区与流量计关系
    """
    params = json.loads(request.data)
    mongo.db.zone_rtu.delete_many({'zone_id': ObjectId(zoneid)})
    zone_name = mongo.db.zone_configs.find_one(
        {'_id': ObjectId(zoneid)}, {'_id': 0, 'name': 1})['name']
    for i in params:
        i['zone_name'] = zone_name
        i['zone_id'] = ObjectId(zoneid)
        mongo.db.zone_rtu.update_one({'CID': i['CID'], 'zone_id': ObjectId(zoneid)}, {
                                     '$set': i}, upsert=True)
    return jsonify({'message': '关系编辑成功'})


@zmms.route('/create_zone/', methods=['GET', 'POST'])
@multi_auth.login_required
def create_zone():
    """
    创建分区
    """
    params = json.loads(request.data)
    if params.get('Superior'):
        params['Superior'] = ObjectId(params['Superior'])
    else:
        params['Superior'] = None
    mongo.db.zone_configs.insert_one(params)
    return jsonify({'message': '分区创建成功'})


@zmms.route('/get_zone_data/', methods=['GET', 'POST'])
@multi_auth.login_required
def get_zone_data():
    """
    获取分区水量数据和相关流量计数据，加入数据清洗功能
    """
    params = json.loads(request.data)
    if isinstance(params['zone'], str):
        zone_list = [params['zone']]
    else:
        zone_list = params['zone']
    start_date = parser.parse(params['date'][0]).astimezone(
        tz.tzlocal()).strftime('%Y-%m-%d')
    end_date = parser.parse(params['date'][1]).astimezone(
        tz.tzlocal()).strftime('%Y-%m-%d')
    df_zone, df_rtu = ZMMSModel.get_zone_data(zone_list, params.get(
        'datatype'), start_date, end_date, params.get('cycle'))
    if params['dataclean']:
        df_zone = data_clean(df_zone)
        df_rtu = data_clean(df_rtu)
    df_zone.fillna('', inplace=True)  # 有Nan在前端无法转化为对象
    df_rtu.fillna('', inplace=True)
    return jsonify({'zone': {'index': df_zone.index.tolist(), 'columns': df_zone.columns.tolist(), 'table': df_zone.to_dict('records')}, 'rtus': {'index': df_rtu.index.tolist(), 'columns': df_rtu.columns.tolist(), 'table': df_rtu.to_dict('records')}})


@zmms.route('/get_alert_log/', methods=['GET', 'POST'])
@multi_auth.login_required
def get_alert_log():
    """
    获取指定日期的报警记录
    """
    params = json.loads(request.data)
    if params.get('date'):
        date = parser.parse(params['date']).astimezone(tz.tzlocal())
        query = {'date': datetime(date.year, date.month, date.day)}
    else:
        query = {}
    alert_log = list(mongo.db.alert_log.find(query).sort("偏差值"))
    return jsonify(alert_log)


@zmms.route('/set_predict_model/', methods=['POST'])
@multi_auth.login_required
def set_predict_model():
    """
    设置预测模型
    """
    params = json.loads(request.data)
    date = parser.parse(params['date']).astimezone(tz.tzlocal())

    df_q = Head.DataFrame([(1, 121), (4, 121), (6, 121),
                           (7, 121)], date, date+timedelta(days=6), 'H')
    df_q.columns = [i[0:2] for i in df_q.columns.tolist()]
    df_q = df_q.resample('H').mean()

    df = ZMMSModel.get_data(params['CID'], 105, date.strftime(
        '%Y-%m-%d'), (date+timedelta(days=7)).strftime('%Y-%m-%d'), 'hour')
    df = data_clean(df.shift(-1).head(-1))

    model = LinearRegression()
    model.fit(df_q[['锡澄', '雪浪', '中桥', '锡东']], df)
    rtu_model = model.coef_.tolist()[0] + [model.intercept_[0]]

    mongo.db.predict_model2.update_one({'cid': params['CID'], 'status': 'actived'}, {
                                       '$set': {'status': 'history'}})
    mongo.db.predict_model2.insert_one(
        {'cid': params['CID'], 'date': date, 'type': 'liner regression', 'status': 'actived', 'model': rtu_model, 'content': params['content']})
    return jsonify({'message': '模型设置成功'})


@zmms.route('/test2')
def test2():
    date = datetime.now()
    df = Head.DataFrame(
        [(1, 121), (4, 121), (6, 121), (7, 121)], date, date, 'H')
    return df.to_html()
