#!/usr/bin/env python
#!coding=utf-8

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table,Column,Integer,String,Numeric,MetaData,DateTime,Date

from ._base import BaseInit,Base

'''
員工刷卡原始數據表
'''


class ATTTB(BaseInit,Base):
    __tablename__='ATTTB'

    TB001=Column(String(20),nullable=False,primary_key=True,doc='員工編號')
    TB002=Column(DateTime,nullable=False,primary_key=True,doc='打卡時間')
    TB003=Column(Date,nullable=False,doc='出勤日期')
    TB004=Column(String(30),default='',doc='員工姓名')
    TB005=Column(String(10),default='',doc='員工部門編號')
    TB006=Column(String(30),default='',doc='員工部門編號')
    TB007=Column(String(10),default='',doc='員工職務編號')
    TB008=Column(String(30),default='',doc='員工職務名稱')
    TB009=Column(String(10),default='',doc='卡鍾代號')
    TB010=Column(String(30),default='',doc='卡鍾名稱')
    TB011=Column(String(1),default='',doc='打卡上下班識別')
    TB012=Column(String(1),default='',doc='出勤狀態')

    
