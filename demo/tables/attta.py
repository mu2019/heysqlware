#!/usr/bin/env python
#!coding=utf-8

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table,Column,Integer,String,Numeric,MetaData,DateTime

from ._base import BaseInit,Base

'''
員工刷卡原始數據表
'''


class ATTTA(BaseInit,Base):
    __tablename__='ATTTA'

    TA001=Column(String(20),nullable=False,primary_key=True,doc='IC卡編號')
    TA002=Column(DateTime,nullable=False,primary_key=True,doc='打卡時間')
    TA003=Column(String(10),default='',doc='卡鍾設備號')
    TA004=Column(String(20),default='',doc='員工代號')

    

    
