from . import db
from sqlalchemy import or_, and_
import pandas as pd



class Head(db.Model):  # 压力数据表model
    __tablename__ = "tsbl20101030"  # 表名

    # 表结构,具体更多的数据类型自行百度
    id = db.Column("ID", db.Integer, primary_key=True, autoincrement=True)
    date = db.Column("DateTime", db.DateTime)

    ParCode = db.Column("ParameterCode", db.Integer)
    value = db.Column("CurrentValue", db.Float)

    # NodeCode关系定义
    NodeCode = db.Column("NodeCode", db.ForeignKey('tblNode.NodeCode'))

    # NodeRelation = db.relationship('Node',backref=db.backref('NodeCode', lazy='dynamic'))

    # def __repr__(self):
    #    return 'date: %s ,value %f ' %(self.date,self.value)

    @classmethod  # 修改表名的方法
    def tablename(cls, name):
        cls.__table__.name = name.strftime('tsbl%Y%m%d')
        return 'change query TableName to：%s' % cls.__table__.name

    @classmethod
    def query_day(cls, codelist, querydate):
        cls.tablename(querydate)
        queryfield = or_()
        for i, j in codelist:
            queryfield = or_(queryfield, and_(cls.NodeCode == i, cls.ParCode == j))
        return db.session.query(cls.date, cls.value, cls.NodeCode * 1000 + cls.ParCode).filter(queryfield).all()

    @classmethod
    def query_mday(cls, codelist, start_date, end_date):
        res = []
        for day in pd.date_range(start_date, end_date):
            print(day)
            res = res + cls.query_day(codelist, day)
        return res

    @classmethod
    def DataFrame(cls, codelist, start_date, end_date, fre='3Min'):
        """
        返回查询结果的pandas.DataFrame数据
        缺少无效数据剔除，压力数据小于0.05和大于0.5的应剔除，需判断是否为压力数据
        频次取值方式为mean。
        """
        codename = Parameter.querydict(codelist)
        #return pd.DataFrame(cls.query_mday(codelist, start_date, end_date),
        #                    columns=['date', 'value', 'code']).set_index(['date', 'code']).unstack().resample(
        #    fre).mean().rename(columns=codename)
        import math
        df = pd.DataFrame()
        d = pd.date_range(start_date,end_date)
        for i in range(math.ceil(len(d)/30)):
            s = d[i * 30]
            if i * 30 + 30 > len(d):
                e = d[len(d)-1]
            else:
                e = d[i * 30 + 29]
            df = df.append(pd.DataFrame(cls.query_mday(codelist, s, e),
                            columns=['date', 'value', 'code']).pivot_table(
                                index='date',columns='code',values='value'
                                ).resample(fre).mean().rename(columns=codename))
        return df


    @classmethod
    def dataDict(cls, codelist, start_date, end_date, freq):
        """
        接受的数据，`表单名称、起止日期、查询参数或参数列表、频次`
        返回用于pyecharts绘制图标的数据dict
        index 格式化输出，data去除nan,剔除大于100000的数据
        """
        res = Head.DataFrame(codelist, start_date, end_date, freq)
        res = res[res < 100000].fillna(method='ffill').fillna(0)
        data = {}
        index = [i for i in res.index.strftime('%Y%m%d-%H:%M')]
        for i in res.columns:
            data[i] = [v for v in res[i]]
        return index, data



class Node(db.Model):  # 节点名称表
    __tablename__ = 'tblNode'  # 表名

    # 表结构
    id = db.Column('ID', db.Integer, primary_key=True, autoincrement=True)
    NodeCode = db.Column('NodeCode', db.Integer)
    NodeName = db.Column('NodeName', db.CHAR)
    NodeType = db.Column('NodeType', db.Integer)

    @classmethod  # 查询指定编号的节点名称
    def name(cls, code):
        return cls.query.filter(cls.NodeCode == code).first().NodeName

    @classmethod
    def NodeList(cls):
        return db.session.query(cls.NodeCode, cls.NodeName, cls.NodeType)


class Parameter(db.Model):  # 参数名称表
    __tablename__ = 'tblParameter'  # 表名

    # 表结构
    id = db.Column('ID', db.Integer, primary_key=True, autoincrement=True)
    NodeCode = db.Column('NodeCode', db.Integer)
    ParaCode = db.Column('ParameterCode', db.Integer)
    ParaName = db.Column('ParameterName', db.CHAR)
    NodeType = db.Column('ParameterType', db.Integer)

    @classmethod  # 查询指定编号的参数名称
    def pname(cls, code):
        return cls.query.filter(cls.NodeCode == code[0]).filter(cls.ParaCode == code[1]).first().ParaName

    @classmethod  # 查询指定编号的节点+参数名称
    def fullname(cls, code):
        return '%s->%s' % (Node.name(code[0]), cls.pname(code))

    @classmethod  # 节点列表转化为 编号：名称的字典
    def querydict(cls, qlist):
        return {int(i) * 1000 + int(j): cls.fullname((i, j)) for i, j in qlist}

    @classmethod  # 返回对应NodeCode的参数列表
    def ParaList(cls, NodeCode):
        return db.session.query(cls.ParaCode, cls.ParaName).filter(cls.NodeCode == NodeCode).filter(
            cls.ParaName != "预留".encode('gbk')).filter(Parameter.ParaCode != -10)


def allParaList():
    UNUSED = ['梅园水厂', '充山水厂', '小湾里水源厂', '马山水厂', '长安水厂', '中桥水厂2', '华庄水厂', '测压点集合', '水表厂']
    re = []
    for i, j, m in Node.NodeList().all():
        if j not in UNUSED:
            for k, l in Parameter.ParaList(i).all():
                re.append({'key': f'{i}-{k}', 'label': f'{j}-{l}', 'type': m})
    return re

# waterdd管网模型节点、管段和测压点名称对照表

class ModelNode(db.Model):  # 模型节点坐标列表
    __tablename__ = 'net_node'  # 表名
    __bind_key__ = 'waterdd'

    # 表结构
    id = db.Column('ID', db.CHAR, primary_key=True)
    NodeId = db.Column('Node', db.CHAR)
    x_axis = db.Column('x', db.Float)
    y_axis = db.Column('y', db.Float)

    def __repr__(self):
        return '''{"id":"%s","x_axis":"%s","y_axis":"%s"}''' %(self.id, self.x_axis, self.y_axis)

class ModelLink(db.Model):  # 模型管段列表
    __tablename__ = 'net_pipes'  # 表名
    __bind_key__ = 'waterdd'

    # 表结构
    id = db.Column('index', db.CHAR, primary_key=True)
    nodestart = db.Column('NODE1', db.CHAR)
    nodeend = db.Column('NODE2', db.CHAR)
    x_start = db.Column('x1', db.Float)
    x_end = db.Column('x2', db.Float)
    y_start = db.Column('y1', db.Float)
    y_end = db.Column('y2', db.Float)

    def __repr__(self):
        return '''{"id":"%s", "nodestart":"%s","nodeend":"%s", "x_start":"%s","x_end":"%s","y_start":"%s","y_end":"%s"}''' %(
    self.id.strip(), self.nodestart, self.nodeend, self.x_start, self.x_end, self.y_start, self.y_end)

class CheckName(db.Model):  # 调度系统与管网模型名称对照表
    __tablename__ = 'checkname'  # 表名
    __bind_key__ = 'waterdd'

    id = db.Column('id', db.CHAR, primary_key=True)
    name = db.Column('name', db.CHAR)
    address = db.Column('address', db.CHAR)
    diameter = db.Column('diameter', db.CHAR)
    level = db.Column('level', db.Integer)

    def __repr__(self):
        return '''{"id":"%s","name":"%s","address":"%s","diameter":"%s","level":"%s"}''' %(
                    self.id, self.name, self.address, self.diameter, self.level)
