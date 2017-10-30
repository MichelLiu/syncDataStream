#!/usr/bin/env python
# coding=utf8
# MichelLiu

import os
import sys
from bson import json_util
from bson.objectid import ObjectId
import pinyin

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from userformat import *

class DataFormat:
    def __init__(self,fromdb,todb,jobName):
        self.fromdb = fromdb
        self.todb = todb
        self.jobName = jobName

    def typeTransfer(self,value,datatype):
        try:
            if datatype in ["int"]:
                return int(value)
            elif datatype in ["double","float"]:
                return float(value)
            elif datatype in ["long"]:
                return long(value)
            elif datatype in ["string","str"]:
                return str(value)
            else:
                dt = globals()[self.jobName]
                return getattr(dt,datatype)(value)
        except:
            return None

    def mapFields(self,value,targetFields):
        info = {}
        for k,item in enumerate(targetFields):
            tmp = self.typeTransfer(value,item["format"])
            if tmp != None:
                info[item["field"]] = {"value":tmp,"tabName":item["tableName"],"primary":item["primary"]}
        return info

    def format(self,fieldMap,document):
        item = {}
        primary = {}
        if type(document) is dict:
            for k,v in document.items():
                if fieldMap.has_key(k) and type(fieldMap[k]) is list:
                    if v != None:
                        tmp = self.mapFields(v,fieldMap[k])
                        for m,n in tmp.items():
                            if item.has_key(n["tabName"]) == False:
                                item[n["tabName"]] = {}
                            if n["primary"]:
                                primary[m]=k
                            item[n["tabName"]][m] = n["value"]
        elif type(document) is list:
            for index,info in enumerate(document):
                tmpItem = {}
                for k,v in info.items():
                    if fieldMap.has_key(k) and type(fieldMap[k]) is list:
                        if v != None:
                            tmp = self.mapFields(v, fieldMap[k])
                            for m, n in tmp.items():
                                if tmpItem.has_key(n["tabName"]) == False:
                                    tmpItem[n["tabName"]] = {}
                                if n["primary"]:
                                    primary[m] = k
                                tmpItem[n["tabName"]][m] = n["value"]
                for k,v in tmpItem.items():
                    if item.has_key(n["tabName"]) == False:
                        item[n["tabName"]] = []
                    item[n["tabName"]].append(v)

        if len(item) < 1:
            return None,None
        return item,primary