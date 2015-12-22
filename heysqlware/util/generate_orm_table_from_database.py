#!/usr/bin/python
# -*- coding:utf-8 -*-

def mssql(driver,database):
    '''
    獲取數據庫
    SELECT name
    FROM     HSDyeingERP..sysobjects
    WHERE    xtype = 'U'
    ORDER BY name
    '''
