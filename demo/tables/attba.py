#!/usr/bin/env python
#!coding=utf-8

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table,Column,Integer,String,Numeric,MetaData,DateTime

from ._base import BaseInit,Base

'''
員工資料表
'''


class ATTBA(BaseInit,Base):
    __tablename__='ATTBA'

    BA001=Column(String(20),nullable=False,primary_key=True,doc='員工代號')
    BA002=Column(String(30),default='',doc='員工姓名')
    BA003=Column(String(10),default='',doc='員工部門編號')
    BA004=Column(String(30),default='',doc='員工部門編號')
    BA005=Column(String(10),default='',doc='員工職務編號')
    BA006=Column(String(30),default='',doc='員工職務名稱')
    

    
