#!/usr/bin/env python
#!coding=utf-8

'''
#Copyright (c) 2011 Chen MuSheng (sheng.2179@gmail.com)
#
#URL: http://code.google.com/p/heysqlware/
#
#Licensed under the MIT license:
#   http://www.opensource.org/licenses/mit-license.php
'''
## __all__=[
##     'DbWare','QueryValue','TableWare','ModuleWare'

##     ]
import sqlalchemy
from sqlalchemy import create_engine,Column,null,event
from sqlalchemy.orm import sessionmaker,query
#from sqlalchemry.orm.session import Session as _Session
from sqlalchemy.sql.expression import and_,or_,not_,_BinaryExpression
from sqlalchemy.orm.attributes import InstrumentedAttribute as iattr
from sqlalchemy.ext.declarative import DeclarativeMeta
from decimal import Decimal
from .record import Record,RecordSet,QueryValue
from datetime import datetime

try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict
    
#数据库已创建表，select * from pg_statio_user_tables


SessionMaker=sessionmaker()

_sql_rl=_sql_relate={'and':and_,'or':or_}
_exp_where_op={'=':Column.__eq__,'>':Column.__gt__,'<':Column.__lt__,'>=':Column.__ge__,'!=':Column.__ne__,'<>':Column.__ne__,'like':Column.like,'not like':lambda *fvs:not_(Column.like(*fvs)),'in':Column.in_,'is':Column.__eq__,'is not':Column.__ne__,'<=':Column.__le__,'not is':Column.__ne__}

_orm_where_op={'=':iattr.__eq__,'>':iattr.__gt__,'<':iattr.__lt__,'>=':iattr.__ge__,'!=':iattr.__ne__,'<>':iattr.__ne__,'like':iattr.like,'not like':lambda *fvs:not_(iattr.like(*fvs)),'in':iattr.in_,'is':iattr.__eq__,'is not':iattr.__ne__,'not is':iattr.__ne__,'<=':iattr.__le__}

ENGINE_STRING='%(dialect)s+%(driver)s://%(user)s:%(password)s@%(host)s/%(dbname)s?%(paras)s'

_n1='eq,ne,gt,ge,lt,le,nu,nn,in,ni'.split(',')
_o1='=,<>,>,>=,<,<=,is null,not is null,is not null,in,not in'.split(',')
_n2='bw,bn,ew,en,cn,nc'.split(',')
_o2='like,not like,like,not like,like,not like'.split(',')
_v2='%s%%,%s%%,%%%s,%%%s,%%%s%%,%%%s%%'.split(',')

_op1=dict(zip(_n1,_o1))
_op2=dict(zip(_n2,zip(_o2,_v2)))

def sql_filter(field_filter):
    '''
    将字典类型的查询语句转为列表
    '''
    if field_filter['op'] in _op1:
        return (field_filter['field'],_op1[field_filter['op']],field_filter['data'])
    elif field_filter['op'] in _op2:
        o=_op2[field_filter['op']]
        return (field_filter['field'],o[0],o[1] % field_filter['data'])
    elif field_filter['op'] in _o1:
        return (field_filter['field'],field_filter['op'],field_filter['data'])
    elif field_filter['op'] in _o2:
        o=field_filter['op']
        return (field_filter['field'],o,_v2[_o2.index(o)] % field_filter['data'])
    raise Exception('sql operator not support."%s"' % field_filter['op'])
    
default_db={'mssql':'master','postgresql':'postgres'}

db_exist_sql={'mssql':"select count(name) from master..sysdatabases where name ='%s';",
              'postgresql':"select count(datname) from pg_stat_database where datname='%s'; "
              
              }
table_exist_sql={'mssql':"select count(name) from sysobjects where xtype='U' and name='%s';",
              'postgresql':"select count(relname) from pg_stat_user_tables where schemaname='public' and relname='%s'; "
              }


def filter2condition(filters):
    '''
    字典类型的查询参数
    filters:{'groupOp':'and','rules':[{'op':'>','field':'MOCTA.TA001','data':'2201'}]}
    '''
    rules=[]
    for i,f in enumerate(filters.get('rules',[])):
        if 'groupOp' in f:
            rules.append(filter2condition(f))
        else:
            rules.append(sql_filter(f))
    
    return (filters['groupOp'].lower(),rules) if filters else []#[sql_filter(f) for f in filters['rules']]) if filters else []


# class ExceptListener(sqlalchemy.interfaces.PoolListener):
#     def __init__(self,engine_name,retry_times=3,retry_interval=5,retry_fun=None):
#         '''鏈接關閉後將嘗試會重新鏈接
#         @engine_name:鏈接使用的engine名稱
#         @retry_times:連續最大重試次數
#         @retry_interval:兩次連續重試間隔分鐘數
#         '''
#         self.EngineName=engine_name
#         self.retried = False
#         self.MaxTryTimes=retry_times
#         self.RetryTimes=0
#         self.LastTryTime=datetime.now()
#         self.RetryInterval=retry_interval
#         self.RetryFun=retry_fun

#     def checkout(self, dbapi_con, con_record, con_proxy):
#         print('--ExceptionListener Checkout--: is closed ',dbapi_con.closed)
#         if dbapi_con.closed:
#             if self.RetryTimes<self.MaxTryTimes:
#                 self.RetryTimes+=1
#                 self.RetryFun and self.RetryFun()
#             elif int((datetime.now()-self.LastTryTime).seconds/60)<self.RetryInterval:
                
                


class DbWareMeta(type):
    '''
    保存全局鏈接設置
    _config,保存可鏈接的服務器及鏈接選項
    _Engine,默認數據庫引擎,等於_Engines.values[0],_Engines['Default_Engine']
            print('*'*8,r)
            print('*'*8,r)
    _Engines,保存數據庫鏈接引擎
    
    '''
    _config={}
    _Engine=None
    _Engines=OrderedDict()

    def __new__(meta,cls,base,attrs):
        return type.__new__(meta,cls,base,attrs)

class DbWare(object, metaclass=DbWareMeta):
    @staticmethod
    def engines():
        return DbWareMeta._Engines

    @staticmethod
    def getEngine(engine_name):
        '''
        @engine_name str:engine相應的名稱
        '''
        return DbWareMeta._Engines.get(engine_name)

    @staticmethod
    def sys_engine(engine_paras={}):
        pass

    @staticmethod
    def sync(tables,engine_name):
        '''
        創建指定的數據表
        @tables,list:sqlalchemy ORM Table列表
        '''
        from sqlalchemy import MetaData
        en=DbWareMeta._Engines[engine_name]
        for t in tables:
            t.metadata.bind=en
        tables[0].metadata.create_all()

    @staticmethod
    def createDatabase(dbname,engine_name,paras={}):
        '''
        創建數據庫
        @dbname:數據庫名稱
        @engine_name:數據庫鏈接別名
        @paras:其它參數
        '''
        en=DbWareMeta._Engines[engine_name]
        if en.dialect.name=='mssql':
            collation=" collation %s " % paras['collation'] if 'collation' in paras else ''
            en.execute("create database %s %s ;" % (dbname,collation))
        if en.dialect.name=='postgresql':
            con=en.connect()
            con.connection.set_isolation_level(0)
            encoding=" with encoding \'%s\' " % paras['encoding'] if 'encoding' in paras else ''
            con.execute('create database "%s" %s ;' % (dbname,encoding))
            con.connection.set_isolation_level(1)
        elif en.dialect.name=='mysql':
            encoding=" character set%s " % paras['encoding'] if 'encoding' in paras else ''
            en.execute("create database %s %s;" % (dbname,encoding))


    @staticmethod
    def is_db_exist(dbname,engine_name):
        '''
        數據服務器中是否存在數據庫
        @dbname:查詢的數據庫名稱
        @engine_nane:數據庫鏈接別名
            數據庫爲postgresql時鏈接必須是指向postres庫
        '''
        en=DbWareMeta._Engines[engine_name]
        return en.execute(db_exist_sql[en.dialect.name] % dbname).first()[0]>0

    @staticmethod
    def is_table_exist(tablename,engine_name):
        '''
        數據庫中是否存在指定的數據表
        @tablename:指定的數據表
        @engine_name:數據庫鏈接別名
        '''
        en=DbWareMeta._Engines[engine_name]
        return en.execute(table_exist_sql[en.dialect.name] % tablename).first()[0]>0
        
    @staticmethod
    def reconnect4Closed(dbapi_connection, connection_record):
        #print('---engine event--- connection is closed:',dir(dbapi_connection))#.closed!=0)
        pass


    @staticmethod
    def createEngine(engine_name,engine_paras,options={}):
        en=DbWareMeta._Engines.get(engine_name)
        dialect=engine_paras['dialect']
        if en:
            return en
        if dialect=='sqlite':
            ens="sqlite://%s" % engine_paras.get('dbname','')
            args={}
        else:
            hst=dict(engine_paras)
            hst.setdefault('dbname',default_db[dialect])
            ens=ENGINE_STRING % hst
        options.update(engine_paras.get('options',{}))
        # options['listeners']=[ExceptListener(engine_name,retry_fun)]
        en=create_engine(ens,**options)
        event.listen(en,'checkin',DbWare.reconnect4Closed)
        #print('engine',en)
        DbWareMeta._Engines[engine_name]=en
        # if en.dialect.name=='postgresql':
        #     if en.driver=='psycopg2':
        #         from psycopg2 import extras
        #         try:
        #             extras.register_hstore(en.raw_connection(), True)
        #         except:
        #             pass

        return en

    @staticmethod
    def session(engine_name):
        en=DbWareMeta._Engines.get(engine_name,None)
        return en and SessionMaker(bind=en) or None

is_db_exist=DbWare.is_db_exist
is_table_exist=DbWare.is_table_exist
get_session=DbWare.session
db_session=DbWare.session
get_engine=DbWare.getEngine
get_engines=DbWare.engines
create_engines=DbWare.createEngine
create_database=DbWare.createDatabase


class TableWare(object):

    def __init__(self,table,fields=(),join={},join_fields=[],order_by=[],session=None):
        '''
        創建數據表關聯對象
        table,sqlalchemy.Table類
        fields,指定使用的字段名稱,可使用Column/Column Mapper/字段名稱字符(如果不是主表,需要使用數據表名作為前綴,"."作為分隔符)
        join,left join語句使用的內容{table:on條件}
            [table,條碼列表,多個條碼使用and]
        join_fields,字段名稱,可使用Column/Column Mapper/字段名稱字符
        order_by,["字段名稱 [desc]",...]
        '''
        super(TableWare,self).__init__()
        self._Session=session
        self._Table_=table
        if isinstance(join,(list,tuple)):
            join=OrderedDict([(j[0],and_(*tuple(j[1:]))) for j in join])
        if isinstance(order_by,str):
            order_by=order_by.split(',')
        if isinstance(fields,str):
            fields=fields.split(',')
        self._JoinTable_=join
        self._JoinField_=join_fields
        self._OrderBy_=[]
        self._TableFields=self._genTableMapper(table)
        self._AllFields=self._genColumnMapper()
        self._SelectedField=self.getFieldMapper(fields) if fields else OrderedDict(self._TableFields)
        self._OrderBy_= self.getOrderBy(order_by) if order_by else self.getPrimaryKeyMapper()
        self._SelectedField.update(self.getFieldMapper(join_fields))
        self._AddColumns=[]
        self._Values=[]
        self._RecNo=0
        self._ValuesCount=0

    def getPrimaryKeyMapper(self):
        m=[c for c in self._Table_.__table__.c if c.primary_key]
        return [self._AllFields[str(c)] for c in m]

    def getTableFields(self):
        return list(self._Table_.__table__.columns.keys())

    def findColumnInMaper(self,column):
        '''
        根據Column查找相應的Column Mapper
        '''
        pass

    def getFieldMapper(self,fields=[]):
        '''
        將指定的字段統一轉化為InstrumentedAttribute對象
        '''
        while '' in fields:
            fields.remove('')
        #while None in fields:
            #fields.remove(None)
        flds=OrderedDict()
        for c in fields:
            if isinstance(c,str):
                if '.' not in c:
                    c="%s.%s" % (self._Table_.__tablename__,c)
            elif isinstance(c,Column):
                c=str(c)
            elif isinstance(c,iattr):
                c=str(c).split(' ')[-1].replace('>','')
            flds[c]=self._AllFields[c]
        return flds


    def _genTableMapper(self,table=None):
        flds=OrderedDict()
        t=table if table else self._Table_
        for c in list(t.__table__.c.keys()):
            flds['%s.%s' % (t.__tablename__,c)]=t.__dict__[c]
        return flds


    def _genColumnMapper(self,joinmappers=[]):
        flds=OrderedDict()
        tn=self._Table_.__tablename__
        tmps=list(self._JoinTable_.keys())
        tmps.extend(joinmappers)
        for c in list(self._Table_.__table__.c.keys()):
            flds['%s.%s' % (self._Table_.__tablename__,c)]=self._Table_.__dict__[c]
        for t in tmps:
            for c in list(t.__table__.c.keys()):
                flds['%s.%s' % (t.__tablename__,c)]=t.__dict__[c]
        return flds


    def getColumnMapper(self,columns=None):
        cc=[]
        cms=columns if columns else self._FieldList
        for c in cms:
            if isinstance(c,iattr):
                cc.append(c)
            elif isinstance(c,Column):
                pass
        return c

    def save(self):
        try:
            self._Session.commit()
            r=None
        except Exception as e:
            self._Session.rollback()
            r=e
        return r

    def getOrderBy(self,order_by):
        '''
        排序
        order_by;[字段 DESC/ASC,]
        '''
        fns=[]
        desc=[]
        flds=[]
        if not order_by:
            return flds
        for o in order_by:
            o=o.strip().split(' ')
            fns.append(o[0])
            if len(o)==2:
                if o[1].upper()=='DESC':
                    desc.append(True)
                else:
                    desc.append(False)
            else:
                desc.append(False)
        fns=self.getFieldMapper(fns)
        for i,c in enumerate(list(fns.values())):
            if desc[i]:
                flds.append(c.desc())
            else:
                flds.append(c)
        return tuple(flds)

    def __iter__(self):
        return self

    def __getitem__(self,index):
        #print('heysqlware 397:',self._Values,dir(self._Values))
        if isinstance(index,slice):
            return self.__getslice__(index.start,index.stop)
        if index==self._Values.count():
            r= None
        else:
            r= self._Values[index]
        fs=list(self._queryFields.keys())
        return Record(r,fs)
        #return QueryValue(list(self._queryFields.keys()),r,self._Table_.__tablename__)
        
    def __getslice__(self,start,end):
        rr=self._Values[start:end]
        fs=list(self._queryFields.keys())
        return RecordSet(rr,fs)
        #return [Record(r,fs) for r in rr]
        #return [QueryValue(self._queryFields,r,self._Table_.__tablename__) for r in rr]

    def getslice(self,start,end):
        return self.__getslice__(start,end)
        
    def __len__(self):
        return self._ValuesCount# if self._Values else 0
            
    def all(self):
        fs=list(self._queryFields.keys())
        return RecordSet(self._Values.all(),fs)

    def __next__(self):
        fs=list(self._queryFields.keys())
        if self._RecNo==self._ValuesCount:
            self._RecNo=0
            raise StopIteration
        else:
            #print('heysqlware next 431',self._RecNo)
            r=self[self._RecNo]#]._Values[self._RecNo]
            self._RecNo+=1
            return r#self[Record(r,fs)


    def last(self):
        fs=list(self._queryFields.keys())
        if self._ValuesCount>0:
            self._RecNo=self._ValuesCount-1
            r=self._Values[self._RecNo]
        else:
            self._RecNo=0
            r=[]
        return Record(r,fs)

    def first(self):
        self._RecNo=0
        fs=list(self._queryFields.keys())
        if self._ValuesCount>0:
            r=self._Values[self._RecNo]
        else:
            r=[]
        return Record(r,fs)

    def join(self,table,onclause):
        '''
        table,left join表
        onclause,left join on語句
        使用orm表達式
        table=COPTH
        onclause=COPTG.TG001==COPTH.TH001 and COPTG.TG002==COPTH.TH002
        '''
        self._JoinTable_[table]=onclause
        return self

    def addColumn(self,column):
        self._AddColumns.append(column)

    def delete(self,keys=[],condition=[]):
        '''刪除記錄
        @keys:主鍵值列表,刪除指定的記錄
        @condition:刪除條件,['and'/'or',[('field_name','op',value),...]]
            條件中的字段"field_name"需要指定數據表名前綴,如COPMG.MG001
        '''
        pk=self.getPrimaryKeyMapper()
        klen=min(len(pk),len(keys))
        ks=zip(pk[:klen],keys[:klen])
        cnd=('and',[(k,'=',v) for k,v in ks]) if keys else []
        condition=cnd if cnd else condition
        rr= self._Session.query(self._Table_).filter(self.where(condition))
        print('heysqlware delete',rr,cnd)
        if rr.count()>0:
            rr=rr.delete('fetch')
        else:
            rr=0
        return rr

    def get(self,*keys):
        '''
        使用主鍵值獲取相應記錄
        @keys:主鍵值列表
        '''
        pk=self.getPrimaryKeyMapper()
        klen=min(len(pk),len(keys))
        ks=zip(pk[:klen],keys[:klen])
        cnd=('and',[(k,'=',v) for k,v in ks]) if keys else []
        r=self._Session.query(self._Table_).filter(self.where(cnd))
        fs=list(self._SelectedField)
        r=list(r[0]) if r.count() else None
        return Record(r,fs)
        
    def set(self,*keys,**kws):
        '''
        更新指定
        '''

    def insert(self,*args,**kws):
        '''插入数据
        
        @args,list:按字段顺序设置相应的值
        @kws,dict:按字段名称设置对应的值
        '''
        d={}
        for k in kws:
            d[k.split('.')[-1]]=kws[k]
        t=self._Table_(*args,**d)
        self._Session.add(t)

    def update(self,condition=[],*args,**kws):
        '''更新指定條件記錄

        @condition:條件,['and'/'or',[('field_name','op',value),...]]
            條件中的字段"field_name"需要指定數據表名前綴,如COPMG.MG001
        @args:按數據表字段順序傳入字段值
        @kws:字典形式傳入更新內容
        '''
        rr= self._Session.query(self._Table_)
        w=self.where(condition)
        rr=rr.filter(w)
        fields=list(self._TableFields.keys())
        for r in rr:
            if 'kws' in kws:
                kws=kws['kws']
            for i,a in enumerate(args):
                r[self._TableFields[i].name]=a
            for k,v in list(kws.items()):
                k=k.split('.')
                if len(k)>1:
                    if k[0]==self._Table_.__tablename__:
                        k=k[1]
                    else:
                        continue
                else:
                    k=k[0]
                r[k]=v

    def select(self,condition=[],limit=None,order_by=[],fields=()):
        '''
        @condition,查询條件,['and'/'or',[('field name','op',value),...]]
             字典方式表達
             {'groupOp':'and','rules':[{'op':'>','field':'MOCTA.TA001','data':'2201'}]}
             但會使用filter2condition轉換為['and'/'or',[('field name','op',value),...]]形式
        @limit,返回記錄數量
        @order_by:排序字段
        @fields:列表,返回的字段
        '''
        if isinstance(condition,dict):
            condition=filter2condition(condition)
        if isinstance(order_by,str):
            order_by=order_by.split(',')
        if isinstance(fields,str):
            fields=fields.split(',')

        fields=self.getFieldMapper(fields) if fields else self._SelectedField
        fds=list(fields.values())
        qr=self._Session.query(*fds).select_from(self._Table_)
        if hasattr(self,'_JoinTable_') and self._JoinTable_:
            jt=list(self._JoinTable_.items())
            qr=qr.outerjoin(*jt)
        qr.add_columns(*self._AddColumns)
        orderby=self.getOrderBy(order_by) if order_by else self._OrderBy_
        self._queryFields=fields
        #w=self.where(condition)
        self._Values=  qr.filter(self.where(condition)).order_by(*orderby) if condition else qr.order_by(*orderby)
        self._RecNo=0
        self._ValuesCount=self._Values.count()
        return self._Values
    
    def clause(self,clause):
        '''
        返回单个条件表达
        clause,[字段名称,操作符,值]
        '''
        col=clause[0] if isinstance(clause[0],str) else str(clause[0])
        assert col in self._AllFields,'the field "%s" not in field list.' % col
        c=self._AllFields.get(col)
        if isinstance(c,Column):
            op=_exp_where_op.get(clause[1])
        else:
            op=_orm_where_op.get(clause[1])
        assert op,'operator "%s" error.' % clause[1]
        r=op(c,clause[2])
        return op(c,clause[2])
        
    def where(self,clauses):
        '''
        sql條件語句
        每一個語句包括字段名稱、操作符、值。
        clause:一個sql條件表達語句,格式為:["and"/"or",[(字段名称,操作符,值),...]]
        '''
        if not clauses:
            return None
        if clauses[0] in _sql_rl:
            rl=_sql_rl[clauses[0]]
        else:
            raise Exception("sql where clause list  must start with 'and' or 'or'")
        wl=[]
        for c in clauses[1]:
            c=list(c)
            if c[0] in _sql_rl:
                wl.append(self.where(c))
            elif isinstance(c,_BinaryExpression):
                wl.append(c)
            elif len(c)==3 and c[1] in _exp_where_op:
                if c[2]=='null':
                    c[2]=null()
                wl.append(self.clause(c))
            else:
                raise Exception("sql clause error,%s" % (clauses))
        return rl(*wl)

    @property
    def Rec(self):
        if self._Values.count()>self._RecNo:
            return self._Values[self._RecNo]
        else:
            return self._Values

    @property
    def TableName(self):
        return self._Table_.__tablename__

    @staticmethod
    def newSession(engine=None):
        if not engine and not DbWareMeta._Engine:
            engine=DbWareMeta._Engine=IConfig.createEngine()
        elif not engine and DbWareMeta._Engine:
            engine=DbWareMeta._Engine
        return SessionMaker(bind=engine)

class ModuleWare(object):

    def __init__(self,session=None):
        super(ModuleWare,self).__init__()
        self._Session= session if session else self.session()
        self._Master=None
        self._Slaver=None
        self._RecNo=0
        self._RelateKey=[]

    def __iter__(self):
        return self

    def __next__(self):
        if self._RecNo==len(self._Master):
            raise StopIteration
        else:
            r=self._Master[self._RecNo]
            cnd=[['%s.%s' % (self._Slave.TableName,k[1]),'=',r[list(r.keys()).index(k[0])]] for k in self._RelateKey]
            cnd=['and',cnd]
            self._Slave.select(condition=cnd)
            self._RecNo+=1
            return r

    def commit(self):
        e=None
        try:
            self._Session.commit()
        except Exception as e:
            self._Session.rollback()
        return e

    def rollback(self):
        self._Session.rollback()

    def prenew(self):
        '''
        返回新建單據新的默認值
        '''

    def new(self,data):
        '''
        新建單據
        通過定義的主數據表獲取相應的單據編號,並將數據保存到數據中
        '''
        
    def insertItem(self,data):
        '''
        新增記錄
        '''

    def insert(self,data):
        '''
        '''

    def query(self,fields=(),condition=()):
        '''
        查詢數據
        '''
        self._Master.select(fields,condition)
        return self._Master

    def queryItem(self,fields=(),condition=()):
        '''
        查詢單據明細
        '''
        return self._Slave.select(fields,condition)

    def delete(self,billno):
        '''
        刪除單據
        '''

    def removeItem(self,bill_item):
        '''
        刪除單據子項
        '''

    def discare(self,billno):
        '''
        傷廢單據
        '''

    def midify(self):
        '''
        修改單據內容
        '''

    def midifyItem(self,data):
        '''
        修改項目內容
        '''

    @property
    def Session(self):
        return self._Session

    @property
    def Items(self):
        return self._Slave
