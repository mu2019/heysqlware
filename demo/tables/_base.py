#!/usr/bin/env python
#!coding=utf-8

from sqlalchemy.ext.declarative import declarative_base
Base=declarative_base()

class BaseInit(object):
   
    def __init__(self,*args,**kws):
        super(BaseInit,self).__init__()
        _fds=list(self.__class__.__table__.columns.keys())
        if args and isinstance(args[0],list) and isinstance(args[1],dict):
            kv.update(args[1])
            args=args[0]
        kws.update(dict([(_fds[i],v) for i,v in enumerate(args)]))
        self.__dict__.update(kws)
        
    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__,[self.__dict__.get(n) for n in list(self.__class__.__table__.columns.keys())])
            
    def __getitem__(self,name):
        print(('name',name))
        return self.__dict__[name]

    def __setitem__(self,name,value):
        self.__setattr__(name,value)    