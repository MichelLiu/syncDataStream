#!/usr/bin/env python
# coding=utf8
__author__ = 'Liu Wei (MichelLiu)'

import sys

import threadpool as threadpool
from bson import json_util

reload(sys)
sys.setdefaultencoding('utf8')

import ConfigParser
from pymongo import MongoClient

class dataCenter:
    def __init__(self,eventEnvCfg, cioEnv):
        confkey = cioEnv + "-Base"
        Opkey = cioEnv + "-Optional"
        required_ops = [("Sandbox-Base", "AccessKeyId"), ("Sandbox-Base", "AccessKeySecret"),
                        ("Sandbox-Base", "Endpoint"), ("Sandbox-Base", "MongoDB"), ("Online-Base", "AccessKeyId"),
                        ("Online-Base", "AccessKeySecret"), ("Online-Base", "Endpoint"), ("Online-Base", "MongoDB")]
        optional_ops = [("Sandbox-Optional", "SecurityToken"), ("Online-Optional", "SecurityToken")]

        parser = ConfigParser.ConfigParser()
        parser.read(eventEnvCfg)
        for sec, op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec, op, eventEnvCfg))
                sys.stderr.write("Read README to get help inforamtion.\n")
                sys.exit(1)

        mongoConnection = parser.get(confkey, "MongoDB")
        self.mongoClient = MongoClient(mongoConnection)
        self.mongoDataBase = self.mongoClient.enterprise

    def queryCompanyId(self, company):
        company_info = self.mongoDataBase['EnterpriseBaseInfo'].find_one({'ENTNAME': company})
        if company_info is None or False == company_info.has_key('PID') or len(company_info['PID']) <= 0:
            return None
        return company_info["PID"]

    def queryCompanyName(self, pid):
        company_info = self.mongoDataBase['EnterpriseBaseInfo'].find_one({'pid': pid})
        if company_info is None or False == company_info.has_key('ENTNAME') or len(company_info['ENTNAME']) <= 0:
            return None
        return company_info["ENTNAME"]

    def queryEventInfo(self, pid, eventType):
        if pid is None or len(pid) <= 0:
            return None

        where = None
        collection = ""
        if eventType == 1:
            collection = ''
            dbKey = ''
        elif eventType == 2:
            collection = ''
            dbKey = ''
        elif eventType == 3:
            collection = ''
            dbKey = ''
        elif eventType == 4:
            collection = ''
            dbKey = ''
        elif eventType == 5:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'PUNISHINFO'
        elif eventType == 6:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'INDBUSINFO'
        elif eventType == 7:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'CLEARINGINFO'
        elif eventType == 8:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'SPOTCHECK'
        elif eventType == 9:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'ASSISTINFO'
        elif eventType == 10:
            collection = 'TaxArrears'
            dbKey = ''
        elif eventType == 11:
            collection = 'TaxIllegal'
            dbKey = ''
        elif eventType == 12:
            collection = 'TaxUnusual'
            dbKey = ''
        elif eventType == 13:
            collection = 'Enforcement'
            dbKey = ''
        elif eventType == 14:
            collection = 'Dishonest'
            dbKey = ''
        elif eventType == 15:
            collection = 'OpenAnno'
            dbKey = ''
        elif eventType == 16:
            collection = 'CourtAnno'
            dbKey = ''
        elif eventType == 17:
            collection = 'RefereeDoc'
            dbKey = ''
        elif eventType == 18:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'ILLINFO'
        elif eventType == 19:
            collection = ''
            dbKey = ''
        elif eventType == 20:
            collection = ''
            dbKey = ''
        elif eventType == 21:
            collection = ''
            dbKey = ''
        elif eventType == 22:
            collection = ''
            dbKey = ''
        elif eventType == 23:
            collection = ''
            dbKey = ''
        elif eventType == 24:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'INDBUSINFO'
        elif eventType == 25:
            collection = 'EnterpriseBaseInfo'
            dbKey = 'ILLINFO'
        else:
            return None

        company_info = None
        if collection == '':
            return None
        elif collection == "EnterpriseBaseInfo":
            company_info = self.mongoDataBase[collection].find_one({'PID': pid})
        else:
            cursor = self.mongoDataBase[collection].find({'PID': pid})
            if cursor is None:
                return None
            company_info = []
            for document in cursor:
                company_info.append(document)

        if company_info is None:
            return None

        if dbKey != '':
            if company_info.has_key(dbKey) == False:
                return None
            return company_info[dbKey]

        return company_info


class BaseBatchEvent(object):
    def __init__(self,mongoDataBase,eventGenerator):
        self.occurs = 16
        self.mongoDataBase = mongoDataBase
        self.collection = mongoDataBase[self.__class__.__name__]
        self.docCnt = self.collection.find({}).count()
        if self.docCnt <= self.occurs:
            self.pageSize = self.docCnt
        else:
            self.pageSize = int(round(self.docCnt/self.occurs))
        self.eventGenerator = eventGenerator
        self.pool = threadpool.ThreadPool(self.occurs)
        print self.pageSize,self.docCnt,self.__class__.__name__

    def queryCompanyId(self, company):
        company_info = self.mongoDataBase['EnterpriseBaseInfo'].find_one({'ENTNAME': company})
        if company_info is None or False == company_info.has_key('PID') or len(company_info['PID']) <= 0:
            return None
        return company_info["PID"]

    def queryCompanyName(self, pid):
        company_info = self.mongoDataBase['EnterpriseBaseInfo'].find_one({'pid': pid})
        if company_info is None or False == company_info.has_key('ENTNAME') or len(company_info['ENTNAME']) <= 0:
            return None
        return company_info["ENTNAME"]

    def getPageSize(self,pageSize):
        return self.pageSize

    def setOccurs(self,occurs):
        self.occurs = occurs

    def pushEvent(self,document):
        pass

    def callback(self,startpos):
        tmp = startpos
        cursor = self.collection.find({}).skip(startpos)
        cursor.batch_size(500)
        for document in cursor:
            if tmp + self.pageSize <= startpos:
                cursor.close()
                break
            self.pushEvent(document)
            startpos += 1

    def run(self):
        docMap = []
        for step in range(0,int(round(self.docCnt/self.pageSize))):
            if len(docMap) == 1:
                break
            skippos = step*self.pageSize
            docMap.append((None,{'startpos':skippos}))
        print docMap
        requests = threadpool.makeRequests(self.callback, docMap)
        [self.pool.putRequest(req) for req in requests]
        self.pool.wait()


class EnterpriseBaseInfo(BaseBatchEvent):
    def pushEvent(self,document):
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 5, document["PUNISHINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 6, document["INDBUSINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 7, document["CLEARINGINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 8, document["SPOTCHECK"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 9, document["ASSISTINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 18, document["ILLINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 24, document["INDBUSINFO"])
        self.eventGenerator.pushEvent(document["ENTNAME"], document["PID"], 25, document["ILLINFO"])


class TaxArrears(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 10, eventDocs)

class TaxIllegal(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 11, eventDocs)

class TaxUnusual(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 12, eventDocs)

class Enforcement(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        pid = None
        if document.has_key("ENFNAME") and len(document["ENFNAME"]) >= 0:
            if document.has_key("PID") and len(document["PID"]) >= 0:
                pid = document["PID"]
            else:
                pid = self.queryCompanyId(document["ENFNAME"])
            print document["ENFNAME"], pid

        if pid == None or len(pid) <= 0:
            return
        else:
            document["PID"] = pid
        self.eventGenerator.pushEvent(document["ENFNAME"], document["PID"], 13, eventDocs)

class Dishonest(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        pid = None
        if document.has_key("ENFNAME") and len(document["ENFNAME"]) >= 0:
            if document.has_key("PID") and len(document["PID"]) >= 0:
                pid = document["PID"]
            else:
                pid = self.queryCompanyId(document["ENFNAME"])
            print document["ENFNAME"], pid

        if pid == None or len(pid) <= 0:
            return
        else:
            document["PID"] = pid
        self.eventGenerator.pushEvent(document["ENFNAME"], document["PID"], 14, eventDocs)

class OpenAnno(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 15, eventDocs)

class CourtAnno(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 16, eventDocs)


class RefereeDoc(BaseBatchEvent):
    def pushEvent(self,document):
        eventDocs = []
        eventDocs.append(document)
        pid = None
        if document.has_key("COMPANYNAME") and len(document["COMPANYNAME"]) >= 0:
            if document.has_key("PID") and len(document["PID"]) >= 0:
                pid = document["PID"]
            else:
                pid = self.queryCompanyId(document["COMPANYNAME"])
            print document["COMPANYNAME"],pid

        if pid == None or len(pid) <= 0:
            return
        else:
            document["PID"] = pid
        self.eventGenerator.pushEvent(document["COMPANYNAME"], document["PID"], 17, eventDocs)



