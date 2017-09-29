#!/usr/bin/env python
# coding=utf8
# author = wei.liu@gemdata.net

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

    def format(self,fieldMap,document):
        item = document
        if type(document) is dict:
            for k,v in document.items():
                if fieldMap.has_key(k) and type(fieldMap[k]) is dict:
                    if v != None:
                        tmp = self.typeTransfer(v,fieldMap[k]["dataFormat"])
                        if tmp != None:
                            item[k] = tmp
        elif type(document) is list:
            return None

        if len(item) <= 1:
            return None
        return item