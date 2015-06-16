#!/usr/bin/env python
#!coding=utf-8

from heysqlware import DbWare,create_engines
from tables.attta import ATTTA
from tables.atttb import ATTTB
from tables.attba import ATTBA

'''
創建sqlite數據庫attendance.db及創建數據表ATTBA,ATTTA,ATTTB
'''

db_paras={'dialect':'sqlite',
          'dbname':'/attendance.db'
          }
create_engines('db',db_paras)
DbWare.sync([ATTTA,ATTTB,ATTBA],'db')





