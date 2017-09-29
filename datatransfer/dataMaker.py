#!/usr/bin/env python
# coding=utf8
# author = wei.liu@gemdata.net

import os
import sys
from pymongo import MongoClient
import MySQLdb
from bson import json_util
from bson.objectid import ObjectId
import pinyin

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from lib.threadPool import ThreadPool
from lib.opensearchSDK import V3Api
from dataformat.dataFormat import DataFormat

class dataTransfer(ThreadPool):
    def __init__(self,configContent,logPath,jobName):
        self.configContent = configContent
        self.source = self.configContent["source"]
        self.target = self.configContent["target"]
        self.mongoClients = {"source":{},"target":{}}
        self.sqlserverClients = {"source":{},"target":{}}
        self.mysqlClients = {"source":{},"target":{}}
        ThreadPool.__init__(self,occurs = self.configContent["occurs"])
        self.logPath = logPath + "/" + self.source["connection"]["collection"]
        if os.path.exists(self.logPath) == False:
            os.makedirs(self.logPath)
        self.docFormator = DataFormat(self.source["dbtype"],self.target["dbtype"],jobName)

    def customize_variable(self,startpos):
        if self.source["dbtype"] == 'mongo':
            self.mongoClients["source"][str(startpos)]=MongoClient(self.source["connection"]["connectString"])[self.source["connection"]["db"]][self.source["connection"]["collection"]]
        if self.target["dbtype"] == 'mongo':
            self.mongoClients["target"][str(startpos)]=MongoClient(self.target["connection"]["connectString"])[self.target["connection"]["db"]][self.target["connection"]["collection"]]
        if self.target["dbtype"] == 'mysql':
            self.mysqlClients["target"][str(startpos)]=MySQLdb.connect(host=self.target["connection"]["host"],user=self.target["connection"]["user"],passwd=self.target["connection"]["passwd"],db=self.target["connection"]["db"],charset=self.target["connection"]["charset"])

    def __close_connection(self,startpos):
        for k,v in self.mongoClients.items():
            if self.mongoClients[k].has_key(str(startpos)) and type(self.mongoClients[k][str(startpos)]) is MongoClient:
                self.mongoClients[k][str(startpos)].close()
        for k,v in self.mysqlClients.items():
            if self.mysqlClients[k].has_key(str(startpos)) and type(self.mysqlClients[k][str(startpos)]) is MySQLdb.Connection:
                self.mysqlClients[k][str(startpos)].close()

    def getDocCnt(self):
        if self.source["dbtype"] == 'mongo':
            tmpClient = MongoClient(self.source["connection"]["connectString"])
            docCnt = tmpClient[self.source["connection"]["db"]][self.source["connection"]["collection"]].count()
            tmpClient.close()
            return docCnt
        elif self.source["dbtype"] == 'sqlserver':
            return 0
        elif self.source["dbtype"] == 'mysql':
            return 0
        else:
            return 0

    def getCursor(self,startpos):
        if self.mongoClients["source"].has_key(str(startpos)):
            projection = {}
            for k,v in self.configContent["fieldMap"].items():
                projection[k] = 1
            return self.mongoClients["source"][str(startpos)].find(self.source["query"],projection).skip(startpos)
        elif self.source.has_key('sqlserver'):
            return 0
        elif self.source.has_key('mysql'):
            return 0
        else:
            return 0

    def update(self,document,startpos):
        item = self.docFormator.format(self.configContent["fieldMap"],document)
        if item == None:
            return
        if self.target["dbtype"] == 'mongo':
            ret = self.mongoClients["target"][str(startpos)].update({'_id': document["_id"]}, {"$set": item},self.configContent["upsert"],True)
            print ret["updatedExisting"],item
        elif self.target["dbtype"] == 'mysql':
            keys = self.configContent["fieldMap"].keys()
            targetKeys = []
            targetKeysType = {}
            for index,keyv in enumerate(keys):
                targetKeys.append(self.configContent["fieldMap"][keyv]["targetField"])
            sql = "INSERT IGNORE INTO " + self.target["connection"]["db"] + "." + self.target["connection"][
                "collection"] + \
                  " (`" + "`,`".join(targetKeys) + "`) VALUES ("
            for index, keyv in enumerate(keys):
                if type(item[keyv]) is str or type(item[keyv]) is unicode:
                    sql += '"%s",' % item[keyv]
                else:
                    sql += '%s,' % item[keyv]
            sql = sql[:-1] + ")"
            print sql
            cursor = self.mysqlClients["target"][str(startpos)].cursor()
            cursor.execute(sql)
            self.mysqlClients["target"][str(startpos)].commit()
            cursor.close()


    def callback(self,startpos):
        oriStartPos = startpos
        tmpFile = self.logPath + "/" + str(self.occurs) + "_" + str(startpos) + ".txt"
        if os.path.exists(tmpFile):
            logFile = open(tmpFile, "r")
            startpos = int(logFile.read())
            logFile.close()

        cursor = self.getCursor(startpos)
        cursor.batch_size(1000)
        cnt = 0
        for document in cursor:
            retFlag = self.update(document,oriStartPos)
            cnt += 1
            if cnt > 10000:
                tmplog = open(tmpFile, "wb")
                startpos += cnt
                tmplog.write(str(startpos))
                tmplog.flush()
                tmplog.close()
                cnt = 0
            if startpos > oriStartPos + self.pageSize:
                break
        cursor.close()
        self.__close_connection(oriStartPos)