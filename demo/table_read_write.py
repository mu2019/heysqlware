#!/usr/bin/env python
#!coding=utf-8
from heysqlware import TableWare,create_engines,get_session
from tables.attta import ATTTA
from tables.atttb import ATTTB
from tables.attba import ATTBA

from datetime import datetime
from time import sleep

db_paras={'dialect':'sqlite',
          'dbname':'/attendance.db'
          }
create_engines('attendance',db_paras)
dbse=get_session('attendance')
ta=TableWare(ATTTA,session=dbse)

##########
#select
ta.select()
r=ta.first()
print('--A--:',r)
print('--B--:',r.pack())
print('--C--:',r.isEmpty())
print('--D--:',r.ATTTA_TA001)
rr=ta.all()
print('--E--:',rr)
print('--F--:',rr.pack())
print('--G--:',rr.isEmpty())
if not rr.isEmpty():
    print('--H--:',rr[0].ATTTA_TA001)

##########
#insert

list_data=['12345',datetime.now(),'01','10090']

ta.insert(*list_data)
ta.save()

sleep(1)
dict_data={'TA001':'12345','TA002':datetime.now(),'TA003':'02','TA004':'10090'}
ta.insert(**dict_data)
ta.save()

##########
#update
update_data={'TA004':'20010','TA001':'23456'}
ta.update(['and',[('ATTTA.TA001','=','12345'),('ATTTA.TA003','=','02')]],**update_data)
ta.save()

##########
#select with condition

ta.select(['and',[('ATTTA.TA004','=','20010')]],order_by=['ATTTA.TA004','ATTTA.TA002'])
rr=ta.all()
print('--I--:',rr.pack())
          
##########
#select with join

list_data=['10090','Sam','D01','Sales','T01','Clerk']
ba=TableWare(ATTBA,session=dbse)
ba.insert(*list_data)
ba.insert(*['20010','John','D01','Sales','T02','Manager'])
ba.save()


join=[(ATTBA,ATTBA.BA001==ATTTA.TA004)]
join_fields=[ATTBA.BA002]
jta=TableWare(ATTTA,join=join,join_fields=join_fields,session=dbse)
jta.select(fields=['ATTTA.TA001','ATTTA.TA002',ATTTA.TA004,ATTBA.BA002])
print('--J--:',jta.all().pack())


