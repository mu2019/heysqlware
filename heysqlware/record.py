#!/usr/bin/env python
#!coding=utf-8

from decimal import Decimal
try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict


class Record(object):
    def __init__(self,data,keys=[],read_only=True):
        '''
        按記錄順序的轉換為字典字
        data,list或tuple，一條記錄
        keys,字段名稱，對應data順序
            沒指明keys，使用默認名稱fieldNUMBER，作為字段名稱
        '''
        if data and data[0]=='__RECORD__':
            self.load(data)
        else:
            if keys and data and len(data)!=len(keys):
                raise Exception(101,'data length and field length not equal.')
            if not data and keys:
                data=[None]*len(keys)
            elif data and not keys:
                keys=['field%s' % i+1 for i in range(len(data))]
            self._ReadOnly=read_only
            data=[float(d) if isinstance(d,Decimal) else d for d in data]
            self._data=OrderedDict(zip(keys,data))# if keys else OrderedDict(data)

    def __getitem__(self,item):
        if isinstance(item,str):#,unicode)):
            return self._data[item]
        elif  isinstance(item,(int,slice,float)):
            return list(self._data.values())[item]
        else:
            raise Exception(102,'item"%s" type error' % item)

    def __getattr__(self,attr):
        attr=attr.replace('_','.',1) if '_' in attr else attr
        return self.__getitem__(attr)

    def __iter__(self):
        self._index=0
        self._DataItems=list(self._data.items())
        return self

    def __len__(self):
        return __len__(self._data)

    def next(self):
        if self._DataItems:
            return self._DataItems.pop(0)
        else:
            raise StopIteration

    def isEmpty(self):
        return list(filter(None,self._data.values()))==[]

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()
    
    def pack(self):
        '''
        返回,['RECORD',(字段名稱,字段內容),...]
        '''
        r=list(self._data.items())
        r.insert(0,'__RECORD__')
        return r

    def load(self,datas):
        '''
        將列表轉為記錄對象
        datas,['RECORD',(字段名稱,字段內容),...]
        '''
        if datas[0]!='__RECORD__':
            raise Exception(103,'data not math to load as Record.')
        if len(datas)<2:
            datas={}
        self._data= OrderedDict(datas[1:])

class RecordSet:
    def __init__(self,records,fields=[],read_only=True):
        '''
        數據集
        多個記錄的集合
        所有記錄對應一行字段，不是每一行記錄都有對應的字段，以節省空間及傳輸數據數量
        @records,list:數據列表，可以為RecordSet或Record打包後的列表
        @fields,list:字段名稱列表
        @read_only,boolean:是否為只讀，默認為True/只讀
        '''
        if records and records[0]=='__RECORDSET__':
            self.load(records)
        else:
            if records and records[0]=='__RECORD__':
                rec,field=[],[]
                for f,r in records[1:]:
                    field.append(f)
                    rec.append(r)
                records,fields=[rec],field
            r=records and records[0]
            if r and fields and len(r)!=len(fields):
                raise Exception(101,'data length and field length not equal.')
            if not r and fields:
                records=[]#None]*len(fields)
            elif r and not fields:
                fields=['field%s' % i+1 for i in range(len(r))]
            self._Fields=fields
            self._Records=records
        self._ReadOnly=read_only

    @property
    def Fields(self):
        return tuple(self._Fields)
        

    def __getitem__(self,item):
        '''
        返回指定item內容的Record對象
        '''
        r=self._Records[item]
        return Record(r,self._Fields,self._ReadOnly)

    def __len__(self):
        return len(self._Records)

    def __iter__(self):
        self._index=0
        return self

    def __next__(self):
        if self._index<len(self._Records):
            r=self._Records[self._index]
            self._index+=1
            return Record(r,self._Fields,self._ReadOnly)
        else:
            raise StopIteration
        
    def isEmpty(self):
        return len(self._Records)==0

    def pack(self):
        da=[]
        for row in self._Records:
            if row:
                da.append([float(d) if isinstance(d,Decimal) else d  for d in row])
        #da=[float(d) if isinstance(d,Decimal) else d  for d in for row in self._Records]
        return ['__RECORDSET__',self._Fields,da]

    def load(self,datas):
        if datas[0] not in ('__RECORDSET__','__RECORD__'):
            raise Exception(103,'data not math to load as RecordSet or Record.')
        if datas[0]=='__RECORD__':
            rec,field=[],[]
            for f,r in datas[1:]:
                field.append(f)
                rec.append(r)
            datas=[field,[rec]]
        self._Fields=datas[1]
        self._Records=datas[2]
        

class QueryValue(object):
    def __init__(self,keys,values,table_name):
        '''
        使用字段與相應的值成績有序字典
        並可使用屬性或下標進行引用
        '''
        self._tableName=table_name
        if not values:
            values=[None]*len(keys)
        self._values=values
        self._dict=OrderedDict(list(zip(keys,values)))

    def __getitem__(self,item):
        if item in self._dict or isinstance(item,str):
            return self._dict[item]
        return self._values[item]
            
    def __getattr__(self,attr):
        i=attr.find('_')
        if i==-1 and self._tableName:
            attr='%s.%s' % (self._tableName,attr)
        else:
            attr='%s.%s' % (attr[:i],attr[i+1:])
        return self._dict[attr]

    def __repr__(self):
        return "QueryValue<%s,%s>" % (list(self._dict.keys()),list(self._dict.values()))

    def values(self):
        return self._values[:]

    def keys(self):
        return list(self._dict.keys())

    def pack(self):
        return (self.keys(),self.values(),self._tableName)
