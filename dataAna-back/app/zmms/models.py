import requests
import json
import pandas as pd
import pymssql
from app import mongo
from dateutil import parser, tz
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pymongo import MongoClient

class ZMMSModel():
    """
    分区计量系统数据查询类，通过'http://192.168.222.90:8899/FenQuManage/DanDian/SelectSinglePointData_ForWeb'接口查询流量计数据
    payload: 查询参数字典，{'DataType': 101,
        'RtuID': '1201700022',
        'Start': '2018-6-4 00:00',
        'End': '2018-6-5 00:00',
        'Cycle': 'hour'
        }
    DANDIAN: 查询接口'http://192.168.222.90:8899/FenQuManage/DanDian/SelectSinglePointData_ForWeb'
    """
    payload = {}
    WATERFACTORY = {'WF20180004': 4971,'WF20180008': 4969, 'WF20180009': 5184, 'WF20180011': 4938, 'WF20180012': 4970}
    WaterFactory = { 4938: '锡澄水厂', 4969: '锡东水厂', 4970:'雪浪水厂', 4971: '中桥水厂'}

    def __init__(self, web_api):
        """
        获取SessionId
        """
        self.LOGIN = f'{web_api}Login/CheckLogin'
        self.DANDIAN = f'{web_api}FenQuManage/DanDian/SelectSinglePointData_ForWeb'
        self.OTHER = f'{web_api}FenQuManage/DanDian/SelectSinglePointData_ForWeb_Other'
        self.RISHUILIANG = f'{web_api}FenQuManage/WaterFactoryCB/GetBranchCompanyWaterRSLTotal'
        self.XIUZHENG = f'{web_api}FenQuManage/DanDian/UpdateCorrectedValue'
        self.headers = {'X-Requested-With': 'XMLHttpRequest'}
        self.login()
        self.conn = pymssql.connect(server='192.168.222.22',user='sa',password='SmSc@2016',database='ZMMS')
        self.conn2 = pymssql.connect(server='192.168.90.192',user='sa',password='Password*123',database='MSCADA')
        self.conn_mongo = MongoClient(host='39.104.64.142',port=27017)
        self.mongo = self.conn_mongo.data_user
        self.mongo.authenticate('root','123456')

    def login(self):
        r = requests.post(url=self.LOGIN, headers={'X-Requested-With': 'XMLHttpRequest'}, data={'username':'0001','password':'ae2867f5551b545f0a74c241cd418206','autologin':0})
        self.headers['Cookie'] = 'ASP.NET_SessionId=' + r.cookies['ASP.NET_SessionId']
        self.login_time = datetime.now()

    def check_login(self):
        if (datetime.now() - self.login_time).seconds > 3500:
            self.login()

    def get(self, url, params):
 
        try:
            r = requests.get(url=url, params=params, headers=self.headers)
            response = json.loads(r.text)
            print("get请求结果为：%s"%response)
 
        except BaseException as e:
            print("get请求错误，错误原因：%s"%e)
 
    def post(self, url, params):
 
        data = json.dumps(params)
        try:
            r = requests.post(url=url, json=data, headers=self.headers)
            response = json.loads(r.text)
            print("post请求结果为：%s" %response)
 
        except BaseException as e:
            print("post请求错误，错误原因：%s" % e)
    
    def get_all_rtus(self):
        """
        查询所有可用远传表的cid,rtuid,customer_name
        """
        sql = "SELECT CID, RTU_ID, CUSTOMER_NAME FROM [dbo].[METER_CONFIGS] where cid <> '' and RTU_ID <> '' and CUSTOMER_NAME <> ''"
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        all_rtus = [{'title': i[2],'cid': i[0], 'rtuid': i[1]} for i in cursor.fetchall() if i[1] not in ('000000001', '000000')]
        return all_rtus
    
    def get_rtuid(self, cid_list):
        if len(cid_list) == 1:
            sql = f"""select CID, RTU_ID from [dbo].[METER_CONFIGS] WHERE CID = '{cid_list[0]}' AND CUSTOMER_NAME IS NOT NULL AND CUSTOMER_NAME != '测试'"""
        else:
            sql = f"""select CID, RTU_ID from [dbo].[METER_CONFIGS] WHERE CID IN {tuple(cid_list)} AND CUSTOMER_NAME IS NOT NULL AND CUSTOMER_NAME != '测试'"""
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        res = {i[0]:i[1] for i in cursor.fetchall()}
        cursor.close()
        return res

    def get_rtuids(self, cid):
        """
        查询指定客户编号的{rtuid：名称}的字典
        """
        if type(cid) is str:
            cid_string = f''' = '{cid}' '''
        else:
            cid_string = f''' IN {self.list_to_str(cid)}'''
        sql = f'''select RTU_ID, CUSTOMER_NAME from [dbo].[METER_CONFIGS] WHERE CID {cid_string}'''
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        rtuid_dict = {i[0]:i[1] for i in cursor.fetchall()}
        cursor.close()
        return rtuid_dict

    def rtus_flowrate(self, rtu_list, start_date, end_date, datatype=101, cycle='hour'):
        """
        查询多个rtu流量，可更改周期，不包含修正值
        datatype: 105. 水量 101. 瞬时流量 102. 累计值
        cycle: hour. 实时 day. 小时 month. 日
        """
        self.check_login()
        res = pd.DataFrame()
        self.payload['Start'] = start_date
        self.payload['End'] = (parser.parse(end_date) - timedelta(days=1)).strftime('%Y-%m-%d')
        self.payload['Cycle'] = cycle
        self.payload['DataType'] = datatype
        for i in rtu_list:
            if i not in self.WATERFACTORY:
                self.payload['RtuID'] = i
                r = requests.get(url=self.DANDIAN, params=self.payload, headers=self.headers)
                df = pd.DataFrame(r.json()).replace('暂无', 0)
                df.index = pd.DatetimeIndex(df.RTIME)
                #df.set_index('RTIME', inplace=True)
                # df.AVG.name = i
                try:
                    res[i] = df.AVG.astype(float)
                    # res = res.append(pd.to_numeric(df.AVG))
                except BaseException as e:
                    print("%s--get请求错误,错误原因：%s" % (i,e))
                    continue
            elif i in self.WATERFACTORY:
                tree_id = self.WATERFACTORY[i]
                sql = f'''SELECT *
                        FROM [dbo].[CB_WaterFactoryRiCB]
                        WHERE
                        dbo.CB_WaterFactoryRiCB.Tree_Id = '{tree_id}' AND
                        dbo.CB_WaterFactoryRiCB.D_ChaoBiaoRQ BETWEEN '{start_date}' AND '{end_date}'
                        '''
                wf = pd.read_sql(sql, self.conn)
                wf.index = pd.DatetimeIndex(wf['D_ChaoBiaoRQ'])
                res[i] = wf['I_RiLiuLiang']
        return res

    def rtu_flowrate(self, rtuid, start_date, end_date, datatype=105, cycle='month'):
        """
        查询单个rtu流量，仅有日水量数据，包含修正值
        datatype: 105. 水量 101. 瞬时流量 102. 累计值
        cycle: hour. 实时 day. 小时 month. 日
        """
        self.check_login()
        res = pd.DataFrame()
        self.payload['Start'] = start_date
        self.payload['End'] = (parser.parse(end_date) - timedelta(days=1)).strftime('%Y-%m-%d')
        self.payload['Cycle'] = cycle
        self.payload['DataType'] = datatype
        self.payload['rows'] = 1000
        self.payload['page'] = 1
        self.payload['sord'] = 'asc'
        self.payload['RtuID'] = rtuid
        if rtuid not in self.WATERFACTORY:
            r = requests.get(url=self.OTHER, params=self.payload, headers=self.headers)
            df = pd.DataFrame(r.json()['rows']).replace('暂无', 0)
            df.set_index('RTIME', inplace=True)
            df = df[['WaterVield', 'XiuZhengZ']]
            df.columns = ['value', 'fix']
        elif rtuid in self.WATERFACTORY:
            tree_id = self.WATERFACTORY[rtuid]
            sql = f'''SELECT *
                    FROM [dbo].[CB_WaterFactoryRiCB]
                    WHERE
                    dbo.CB_WaterFactoryRiCB.Tree_Id = '{tree_id}' AND
                    dbo.CB_WaterFactoryRiCB.D_ChaoBiaoRQ BETWEEN '{start_date}' AND '{end_date}'
                    '''
            df = pd.read_sql(sql, self.conn)
            df.index = pd.DatetimeIndex(df['D_ChaoBiaoRQ'])
            df['value'] = df['I_RiLiuLiang']
            df['fix'] = 0
        return df
    
    def zone_flowrate(self, zone_list, start_date, end_date, datatype=105, cycle='month'):
        """
        查询指定的多个分区的流量
        datatype: 105. 水量 101. 瞬时流量 102. 累计值
        cycle: hour. 实时 day. 小时 month. 日
        """
        # 所有指定分区包含的终端列表
        zone_list = [ObjectId(i) for i in zone_list]
        df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find({'zone_id':{'$in': zone_list}})))
        # 查询终端列表对应的rtuid
        # temp1 = {i['CID']:i['RTU_ID'] for i in mongo.db.meter_configs.find({'CID': {"$in":df_rtus.CID.tolist()}},{'RTU_ID':1,'CID':1})}
        # 舍弃上面旧方法，改为从90.192中读取rtuid，避免rtuid变化的问题
        temp = self.get_rtuid(df_rtus.CID.tolist())
        df_rtus.index = df_rtus.CID.apply(lambda x: temp[x])
        df_rtus.index.name = 'rtuid'
        # 转换为分区终端关系对应表
        df_rtus = df_rtus.pivot_table(index=df_rtus.index,columns='zone_name',values='direction')
        # 查询所有终端流量
        df4 = self.rtus_flowrate(df_rtus.index.tolist(), start_date, end_date, datatype=datatype, cycle=cycle)
        res = pd.DataFrame()
        for i in zone_list:
            name = mongo.db.zone_configs.find_one({'_id': i})['name']
            res[name] = df4.mul(df_rtus[name]).sum(axis=1)
        return res

    def fix_flowrate(self, date, CID, value):
        """
        日水量修正
        """
        self.check_login()
        params = {'RTIME': date}
        data = {'XiuZhengZ': value}
        params['RtuID'] = self.get_rtuid([CID])[CID]
        params['S_CID'] = CID
        r = requests.post(url=self.XIUZHENG, params=params, data=data, headers=self.headers)
        return r


    # 新版model ，逐渐删除旧版接口 ***********************************************************************************************   
    # 新版model ，逐渐删除旧版接口 ***********************************************************************************************   
    # 新版model ，逐渐删除旧版接口 ***********************************************************************************************   
    # 新版model ，逐渐删除旧版接口 ***********************************************************************************************   
    # 新版model ，逐渐删除旧版接口 ***********************************************************************************************   
    def list_to_str(self, l):
        return str(list(l)).replace('[', '(').replace(']', ')')

    def get_zone(self, level):
        """
        返回指定level的分区 格式[{ 'name': 分区名称, '_id': 分区id }, ……]
        """
        return [{'name':i['name'],'_id':str(i['_id'])} for i in self.mongo.zone_configs.find({'level': level})]

    def rtu_to_cid(self, rtuid):
        # 查询改名字典
        sql = f'''select CID, RTU_ID
                from [dbo].[METER_CONFIGS]
                WHERE RTU_ID IN {self.list_to_str(rtuid)}
                AND [dbo].[METER_CONFIGS].CUSTOMER_NAME <> '' and [dbo].[METER_CONFIGS].CUSTOMER_NAME <> '测试'  '''
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        res_dict = {i[1]:i[0]  for i in cursor.fetchall()}
        cursor.close()
        return res_dict

    def cid_to_name(self, cid):
        # 查询改名字典
        sql = f'''select CID, CUSTOMER_NAME
                from [dbo].[METER_CONFIGS]
                WHERE CID IN {self.list_to_str(cid)}
                AND [dbo].[METER_CONFIGS].CUSTOMER_NAME <> '' and [dbo].[METER_CONFIGS].CUSTOMER_NAME <> '测试'  '''
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        res_dict = {i[0]:i[1]  for i in cursor.fetchall()}
        cursor.close()
        return res_dict

    def get_waterfactory_day_volume(self, start_date, end_date):
        """
        查询指定日期四个水厂的日产水量，参数start_date, end_date
        """ 
        sdate = parse(start_date)
        edate = parse(end_date)
        sql = f"SELECT * FROM [dbo].[CB_WaterFactoryRiCB] where Tree_Id in {tuple(self.WaterFactory.keys())} \
            AND D_ChaoBiaoRQ BETWEEN '{sdate.strftime('%Y-%m-%d')}' and '{edate.strftime('%Y-%m-%d')}';"
        df = pd.read_sql(sql, self.conn)
        df = df.pivot_table(index='D_ChaoBiaoRQ', values='I_RiLiuLiang', columns='Tree_Id')
        df.rename(columns=self.WaterFactory, inplace=True)
        return df

    def get_cids_by_zone(self, zone=None):
        """
        根据分区名称或分区id返回相关流量计CID,
        参数：zone, 无参数则返回全部
        """
        if zone:
            try: query = {'zone_id': ObjectId(zone)}
            except: query = {'zone_name': zone}
        else:
            query = {}
        return [i['CID'] for i in self.mongo.zone_rtu.find(query,{'_id':0,'CID':1})]

    def dev_detail(self, cid=None):
        """
        返回所有流量计详细资料,包含分区关系
        参数：CID
        """
        if cid:
            query = {'CID':{'$in': cid}}
        else:
            query = {}
        # 查询分区关系
        df = pd.DataFrame(list(self.mongo.zone_rtu.find(query,{'CID':1, 'zone_name':1,'direction':1})))
        df = df.pivot_table(index='CID', values='direction', columns='zone_name')
        df.fillna('-', inplace=True)
        # 查询详细资料
        sql = f'''SELECT
                    CUSTOMER_NAME,
                    [dbo].[METER_CONFIGS].RTU_ID,
                    CID,
                    LONGITUDE,
                    LATITUDE,
                    INSTALL_ADDRESS,
                    AnZhuangFS,
                    GongDianFS,
                    METER_COMPANY,
                    CALIBER_NAME
                FROM
                    [dbo].[METER_CONFIGS]
                INNER JOIN [dbo].[RV_RTUS] ON [dbo].[METER_CONFIGS].RTU_ID = [dbo].[RV_RTUS].RTU_ID
                INNER JOIN [dbo].[DEV_RTUS] ON [dbo].[METER_CONFIGS].RTU_ID = [dbo].[DEV_RTUS].RTU_ID
                INNER JOIN [dbo].[METER_CALIBERS] ON [dbo].[METER_CONFIGS].CALIBER = [dbo].[METER_CALIBERS].CALIBER_ID
                WHERE
                    CID IN {self.list_to_str(cid)}
                AND [dbo].[METER_CONFIGS].CUSTOMER_NAME <> ''
                AND [dbo].[METER_CONFIGS].CUSTOMER_NAME <> '测试' '''
        df2 = pd.read_sql(sql, self.conn2)
        df2.index = df2.CID
        # 合并分区关系与详细资料
        df3 = pd.concat([df, df2],axis=1, sort=False)
        return df3

    def get_one_rtuid(self, cid):
        """
        返回指定cid对应的rtuid
        """
        sql = f'''select RTU_ID from [dbo].[METER_CONFIGS] WHERE CID  = {cid}'''
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        rtuid = cursor.fetchone()[0]
        cursor.close()
        return rtuid

    def get_measure_id(self, rtuid=None, datatype_id=101):
        """
        查询指定参数的measure_id
        101-瞬时流量 102-累计流量 105-水量 103-正向累计流量 104-反响累计流量
        """
        if type(rtuid) is str:
            rtuid_string = f''' = '{rtuid}' '''
        else:
            rtuid_string = f''' IN {self.list_to_str(rtuid)}'''
        sql = f'''SELECT [MEASURE_ID] FROM [dbo].[DEV_MEASURES] where DATATYPE_ID = {datatype_id} AND [RTU_ID] {rtuid_string}'''
        cursor = self.conn2.cursor()
        cursor.execute(sql)
        measure_list = [i[0] for i in cursor.fetchall()]
        cursor.close()
        return measure_list

    def get_hour_data(self, cid, datatype_id, start_date, end_date):
        # 查询指定参数的流量计历史数据
        # 参数：RTU_ID, MEASURE_ID, START_DATE, END_DATE
        rtuid_dict = self.get_rtuids(cid)
        measure_list = self.get_measure_id(rtuid_dict.keys(), datatype_id)
        sdate = parser.parse(start_date)
        edate = parser.parse(end_date)
        sql = f'''SELECT [HOUR], [SUM], [RTU_ID]
                FROM [dbo].[DATA_HOUR]
                WHERE
                dbo.DATA_HOUR.RTU_ID IN {self.list_to_str(rtuid_dict.keys())} AND
                dbo.DATA_HOUR.MEASURE_ID IN {self.list_to_str(measure_list)} AND
                dbo.DATA_HOUR.[HOUR] BETWEEN {sdate.strftime('%Y%m%d%H')} and {edate.strftime('%Y%m%d%H')}
                ORDER BY [HOUR]'''
        df = pd.read_sql(sql, self.conn2)
        if len(df) == 0:
            return None
        df.index = df.HOUR.apply(lambda x: datetime.strptime(str(x), '%Y%m%d%H'))
        df = df.pivot_table(values='SUM', columns='RTU_ID', index=df.index)
        return df

    def get_corrected_data(self, cid, start_date, end_date):
        """
        获取单个流量计的日水量和修正值数据
        """
        rtuid_dict = self.get_rtuids(cid)
        measure_list = self.get_measure_id(rtuid_dict.keys(), datatype_id=105)
        sdate = parser.parse(start_date)
        edate = parser.parse(end_date)
        sql1 = f"SELECT DAY, SUM FROM [dbo].[DATA_DAY] where RTU_ID IN {self.list_to_str(rtuid_dict.keys())} and DAY BETWEEN {sdate.strftime('%Y%m%d')} and {edate.strftime('%Y%m%d')} and MEASURE_ID = {measure_list[0]} ORDER BY DAY;"
        sql2 = f"SELECT D_CorrectedRQ, D_CorrectedValue FROM [dbo].[DATA_DAY_Corrected] where RTU_ID IN {self.list_to_str(rtuid_dict.keys())} AND D_CorrectedRQ BETWEEN '{sdate.strftime('%Y-%m-%d')}' and '{edate.strftime('%Y-%m-%d')}' ORDER BY D_CorrectedRQ;"
        print(sql1)
        df1 = pd.read_sql(sql1, self.conn2)
        df1.index =df1.DAY.apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
        df2 = pd.read_sql(sql2, self.conn2, index_col='D_CorrectedRQ')
        df = pd.concat([df1,df2], axis=1)
        df.drop(columns='DAY', inplace=True)
        return df

    def get_day_data(self, cid, datatype_id, start_date, end_date):
        # 查询指定参数的流量计历史数据
        # 参数：RTU_ID, MEASURE_ID, START_DATE, END_DATE
        rtuid_dict = self.get_rtuids(cid)
        measure_list = self.get_measure_id(rtuid_dict.keys(), datatype_id)
        sdate = parser.parse(start_date)
        edate = parser.parse(end_date)
        sql = f'''SELECT [DAY], [SUM], [RTU_ID]
                FROM [dbo].[DATA_DAY]
                WHERE
                dbo.DATA_DAY.RTU_ID IN {self.list_to_str(rtuid_dict.keys())} AND
                dbo.DATA_DAY.MEASURE_ID IN {self.list_to_str(measure_list)} AND
                dbo.DATA_DAY.[DAY] BETWEEN {sdate.strftime('%Y%m%d')} and {edate.strftime('%Y%m%d')}
                ORDER BY [DAY]'''
        df = pd.read_sql(sql, self.conn2)
        if len(df) == 0:
            return None
        df.index = df.DAY.apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
        df = df.pivot_table(values='SUM', columns='RTU_ID', index=df.index)
        return df

    def get_month_data(self, cid, datatype_id, start_date, end_date):
        # 查询指定参数的流量计历史数据
        # 参数：RTU_ID, MEASURE_ID, START_DATE, END_DATE
        rtuid_dict = self.get_rtuids(cid)
        measure_list = self.get_measure_id(rtuid_dict.keys(), datatype_id)
        sdate = parser.parse(start_date)
        edate = parser.parse(end_date)
        sql = f'''SELECT [MONTH], [SUM], [RTU_ID]
                FROM [dbo].[DATA_MONTH]
                WHERE
                dbo.DATA_MONTH.RTU_ID IN {self.list_to_str(rtuid_dict.keys())} AND
                dbo.DATA_MONTH.MEASURE_ID IN {self.list_to_str(measure_list)} AND
                dbo.DATA_MONTH.[MONTH] BETWEEN {sdate.strftime('%Y%m')} and {edate.strftime('%Y%m')}
                ORDER BY [MONTH]'''
        df = pd.read_sql(sql, self.conn2)
        if len(df) == 0:
            return None
        df.index = df.MONTH.apply(lambda x: datetime.strptime(str(x), '%Y%m'))
        df = df.pivot_table(values='SUM', columns='RTU_ID', index=df.index)
        return df

    def corrected_day_data(self, cid, date, value):
        rtuid = self.get_one_rtuid(cid)
        cursor = self.conn2.cursor()
        date = parser.parse(date)
        # 调用数据库日水量修正的方法, 参数： rtu_id, s_cid, value, date, operator
        cursor.callproc('P_UpdateCorrectedValue', (rtuid, cid, value, date.strftime('%Y-%m-%d'), '超级管理员'))
        self.conn2.commit()
        cursor.close()
        return 'success'


    def get_data(self, cid, datatype_id, start_date, end_date, cycle):
        """
        获取历史数据
        cid: list 客户编号列表
        datatype_id: int 数据类型，105-水量 101-瞬时流量 102-累计流量
        start_date: dateLike 起始日期
        end_date: dateLike 结束日期
        cycle: str 查询周期，hour-小时 day-日 month-月
        """
        func = { 'hour': self.get_hour_data, 'day': self.get_day_data, 'month': self.get_month_data }
        return func[cycle](cid, datatype_id, start_date, end_date)
        
    def get_zone_data_origin(self, zone_list, datatype_id, start_date, end_date, cycle):
        """
        获取指定多个分区所有流量计数据，乘以水流方向和乘子,返回分区流量，相关流量计流量
        """
        # ObjectId转换
        zone_list = [ObjectId(i) for i in zone_list]
        # 获取数据计算关系表
        df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find({'zone_id':{'$in': zone_list}})))
        cid_to_name = {df_rtus.loc[i,'CID']:df_rtus.loc[i, 'rtu_name'] for i in df_rtus.index}
        df_rtus.index = df_rtus.CID
        # 转换为分区终端关系对应表
        df_rtus = df_rtus.pivot_table(index=df_rtus.index,columns='zone_name',values='direction')
        df = self.get_data(df_rtus.index.tolist(), datatype_id, start_date, end_date, cycle=cycle)
        rtuid_to_cid = self.rtu_to_cid(df.columns.tolist())
        df.rename(columns = rtuid_to_cid, inplace=True)
        res = pd.DataFrame()
        for i in zone_list:
            name = mongo.db.zone_configs.find_one({'_id': i})['name']
            res[name] = df.mul(df_rtus[name]).sum(axis=1)
        df.rename(columns = cid_to_name, inplace=True)
        return res, df

    def get_zone_data(self, zone_list, datatype_id, start_date, end_date, cycle):
        """
        获取指定多个分区所有流量计数据，乘以水流方向和乘子,返回分区流量，相关流量计流量
        """
        print(zone_list)
        # ObjectId转换
        zone_list = [ObjectId(i) for i in zone_list]
        # 获取数据计算关系表
        df_rtus = pd.DataFrame(list(mongo.db.zone_rtu.find({'zone_id':{'$in': zone_list}})))
        df_rtus.multiplicator = df_rtus.multiplicator.astype(float) # 将字符转为float，便于pivot_table统计
        cid_to_name = {df_rtus.loc[i,'CID']:df_rtus.loc[i, 'rtu_name'] for i in df_rtus.index}
        # 转换为分区终端关系对应表
        df_rtus = df_rtus.pivot_table(index=['CID'],columns='zone_name',values=['direction','multiplicator'])
        df = self.get_data(df_rtus.index.tolist(), datatype_id, start_date, end_date, cycle=cycle)
        rtuid_to_cid = self.rtu_to_cid(df.columns.tolist())
        df.rename(columns = rtuid_to_cid, inplace=True)
        res = pd.DataFrame()
        for i in zone_list:
            name = mongo.db.zone_configs.find_one({'_id': i})['name']
            #res[name] = df.mul(df_rtus.direction[name]).mul(df_rtus.multiplicator[name]).sum(axis=1)
            try:
                res[name] = df.mul(df_rtus.direction[name]).mul(df_rtus.multiplicator[name]).sum(axis=1)
            except: 
                res[name] = None
        df.rename(columns = cid_to_name, inplace=True)
        return res, df


    def fenqu_shuiliang(self, start, end):
        payload = {'StartDate': start, 'EndDate': end}
        r = requests.post(url=self.RISHUILIANG, data=payload, headers=self.headers)
        df = pd.DataFrame()
        for i in r.json():
            df[i['Name']] = [x['value'] for x in i['ShuiLiangMX']]
        return df

    def generate_jiaoyan_liuliang(self, date):
        """
        生成制定日期的流量校验数据
        """
        if isinstance(date, datetime) is not True:
            date = datetime.strptime(date, '%Y-%m-%d')
        df = self.get_all_liuliang2(date, date + timedelta(days=1)).head(24)/3.6
        jiaoyan_name = {self.RTU_DICT[i]:i for i in self.RTU_DICT}
        df = df.rename(columns=jiaoyan_name)
        df.index = pd.to_datetime(df.index)
        df = df.abs()
        with open('校验' + date.strftime('%Y%m%d') + '流量.dat', 'w') as f:
            f.write(';Link\tHour\tL//s\n')
            for c in df:
                for r in df[c].index:
                    f.write('%s\t%s\t%.2f\n' %(c,r.hour, df.loc[r,c]))
        return df