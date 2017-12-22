#!/usr/bin/env python
# coding=utf8
# author = MichelLiu

import os
import sys

import time

import datetime
from pymongo import MongoClient
import MySQLdb
from DBUtils.PooledDB import PooledDB
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
    def __init__(self, configContent, configPasor, jobName):
        self.configContent = configContent
        self.source = self.configContent["source"]
        self.target = self.configContent["target"]
        self.mongoClients = {"source": {}, "target": {}}
        self.mongoCli = {"source": {}, "target": {}}
        self.sqlserverClients = {"source": {}, "target": {}}
        self.mysqlClients = {"source": {}, "target": {}}
        self.opensearchClients = {"source": {}, "target": {}}
        if int(self.configContent["upsert"]) == 1:
            self.upsert = True
        else:
            self.upsert = False
        self.fieldMap = {}
        for k, v in enumerate(self.configContent["fieldMap"]):
            self.fieldMap[v["sourceField"]] = v["targetFields"]
        self.jobName = jobName
        ThreadPool.__init__(self, occurs=int(self.configContent["occurs"]))
        self.configPasor = configPasor
        self.docFormator = DataFormat(self.source["dbtype"], self.target["dbtype"], jobName)
        self.flagStart = False

    def customize_variable(self, startpos, endpos=None):
        threadid = self.jobName + "_" + str(self.occurs) + "_" + str(startpos)
        info = self.configPasor.findProgress(threadid)
        item = None
        if None == info:
            item = {
                "threadid": threadid,
                "status": 0,
                "position": str(startpos) + "," + str(endpos),
                "finished": 0
            }
            self.flagStart = True

        if self.source["dbtype"] == 'mongo':
            self.mongoCli["source"][str(threadid)] = MongoClient(self.source["connection"]["connectString"])
            self.mongoClients["source"][str(threadid)] = \
            MongoClient(self.source["connection"]["connectString"])[self.source["connection"]["db"]][
                self.source["connection"]["collection"]]
            if None != item:
                tmpQuery = json_util.loads(self.source["query"])
                tmpQuery["_id"] = {"$gte": ObjectId(str(startpos)), "$lt": ObjectId(str(endpos))}
                item["total"] = self.mongoClients["source"][str(threadid)].count(tmpQuery)
                print tmpQuery
        if self.target["dbtype"] == 'mongo':
            self.mongoClients["target"][str(threadid)] = MongoClient(self.target["connection"]["connectString"])[
                self.target["connection"]["db"]]
        if self.target["dbtype"] == 'opensearch':
            self.opensearchClients["target"][str(threadid)] = V3Api(self.target["connection"])

        self.configPasor.addProgress(item)

    def endrun(self):
        self.configPasor.updateRunInfo('', -1, 2)

    def __close_connection(self, startpos):
        for k, v in self.mongoClients.items():
            if self.mongoClients[k].has_key(str(startpos)) and type(self.mongoClients[k][str(startpos)]) is MongoClient:
                self.mongoClients[k][str(startpos)].close()
        for k, v in self.mongoCli.items():
            if self.mongoCli[k].has_key(str(startpos)) and type(self.mongoCli[k][str(startpos)]) is MongoClient:
                self.mongoCli[k][str(startpos)].close()
        for k, v in self.mysqlClients.items():
            if self.mysqlClients[k].has_key(str(startpos)) and type(
                    self.mysqlClients[k][str(startpos)]) is MySQLdb.Connection:
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

    def getCursor(self, threadId, startpos):
        if self.source["dbtype"] == 'mongo':
            projection = {}
            for k, v in self.fieldMap.items():
                projection[k] = 1
            print threadId, startpos
            tmpQuery = json_util.loads(self.source["query"])
            if self.flagStart:
                tmpQuery["_id"] = {"$gte": ObjectId(startpos)}
            else:
                tmpQuery["_id"] = {"$gt": ObjectId(startpos)}
            return self.mongoClients["source"][str(threadId)].find(tmpQuery, projection).sort("_id")
        elif self.source["dbtype"] == 'sqlserver':
            return 0
        elif self.source["dbtype"] == 'mysql':
            return 0
        else:
            return 0

    def docMapTabelRecord(self, offset):
        if self.source["dbtype"] == "mysql" or self.target["dbtype"] == "mysql":
            self.mysqlpool = PooledDB(MySQLdb, self.occurs, host=self.target["connection"]["host"],
                                      user=self.target["connection"]["user"],
                                      passwd=self.target["connection"]["passwd"], db=self.target["connection"]["db"],
                                      charset=self.target["connection"]["charset"])

        if self.source["dbtype"] == "mongo":
            progressList = self.configPasor.findAllProgress(self.occurs)
            if None != progressList and len(progressList) > 0:
                docMap = []
                for index, info in enumerate(progressList):
                    arr = info["threadid"].split("_")
                    tmp = info["position"].split(",")
                    docMap.append((None, {'startpos': arr[2], "endpos": tmp[1]}))
                    self.customize_variable(arr[2], tmp[1])
                print docMap
                return docMap

            tmpClient = MongoClient(self.source["connection"]["connectString"])
            firstDoc = tmpClient[self.source["connection"]["db"]][self.source["connection"]["collection"]].find_one()
            startTimeStamp = int(round(time.mktime(firstDoc["_id"].generation_time.timetuple())))
            tmp = datetime.datetime.now()
            endTimeStamp = int(
                round(time.mktime(datetime.datetime(year=tmp.year, month=tmp.month, day=28).timetuple())))

            total = tmpClient[self.source["connection"]["db"]][self.source["connection"]["collection"]].count()
            average = int(round(total / self.occurs))

            docMap = []
            skippos = hex(startTimeStamp)[2:] + "0000000000000000"
            for i in range(1, self.occurs):
                endpos = skippos
                if average < 200000:
                    curdoc = tmpClient[self.source["connection"]["db"]][self.source["connection"]["collection"]].find(
                        {"_id": {"$gte": ObjectId(endpos)}}).sort({"_id": 1}).skip(
                        average).limit(1)
                    for tmpdoc in curdoc:
                        endpos = str(tmpdoc["_id"])
                        docMap.append((None, {'startpos': skippos, "endpos": endpos}))
                        self.customize_variable(skippos, endpos)
                        skippos = endpos
                        break
                    curdoc.close()
                else:
                    autoskip = int(round(average / 200000))
                    for tmp in range(0, autoskip):
                        curdoc = tmpClient[self.source["connection"]["db"]][
                            self.source["connection"]["collection"]].find(
                            {"_id": {"$gte": ObjectId(endpos)}}).sort("_id").skip(
                            200000).limit(1)
                        for tmpdoc in curdoc:
                            endpos = str(tmpdoc["_id"])
                            break
                        curdoc.close()
                        print tmp, autoskip, endpos
                    docMap.append((None, {'startpos': skippos, "endpos": endpos}))
                    self.customize_variable(skippos, endpos)
                    skippos = endpos

            tmp = datetime.datetime.now()
            endTimeStamp = int(
                round(time.mktime(datetime.datetime(year=tmp.year, month=tmp.month, day=28).timetuple())))
            endpos = hex(endTimeStamp)[2:] + "0000000000000000"
            docMap.append((None, {'startpos': skippos, "endpos": endpos}))
            self.customize_variable(skippos, endpos)
            tmpClient.close()
            print docMap
            return docMap

        docCnt = self.getDocCnt()
        if docCnt <= self.occurs:
            self.pageSize = docCnt
        else:
            self.pageSize = int(round(docCnt / self.occurs))
        docMap = []
        for step in range(0, int(round(docCnt / self.pageSize))):
            skippos = step * self.pageSize + offset
            endpos = skippos + self.pageSize
            docMap.append((None, {'startpos': skippos, 'endpos': endpos}))
            self.customize_variable(skippos, endpos)

        print "docMap:", docMap
        return docMap

    def __cleanNone(self, fields):
        for key, value in fields.items():
            if value == None:
                del fields[key]
        return fields

    def update(self, document, startpos):
        threadid = self.jobName + "_" + str(self.occurs) + "_" + str(startpos)
        item, primary = self.docFormator.format(self.fieldMap, document, self.mongoCli["source"][str(threadid)])
        if item == None:
            return
        if self.target["dbtype"] == 'mongo':
            for collection, info in item.items():
                where = {}
                for key, value in primary.items():
                    where[key] = info[key]
                ret = self.mongoClients["target"][str(threadid)][collection].update(where, {"$set": info}, self.upsert,
                                                                                    True)
        elif self.target["dbtype"] == 'mysql':
            keys = []
            for index, keyv in enumerate(self.configContent["fieldMap"]):
                for m, n in enumerate(keyv["targetFields"]):
                    keys.append(n["field"])
            for tableName, info in item.items():
                sql = ''
                if self.upsert:
                    sql = "INSERT INTO " + self.target["connection"]["db"] + "." + tableName + \
                          " (`" + "`,`".join(keys) + "`) VALUES ("
                    for index, keyv in enumerate(keys):
                        if type(info[keyv]) is str or type(info[keyv]) is unicode:
                            sql += '"%s",' % info[keyv]
                        else:
                            sql += '%s,' % info[keyv]
                    sql = sql[:-1] + ")"
                else:
                    sql = "UPDATE " + self.target["connection"]["db"] + "." + tableName + \
                          " SET "
                    for index, keyv in enumerate(keys):
                        if type(info[keyv]) is str or type(info[keyv]) is unicode:
                            sql += keyv
                            sql += '="%s",' % info[keyv]
                        else:
                            sql += keyv
                            sql += '%s,' % info[keyv]
                    sql = sql[:-1] + " WHERE "
                    for keyv, v in primary.items():
                        if type(info[keyv]) is str or type(info[keyv]) is unicode:
                            sql += keyv
                            sql += '="%s",' % info[keyv]
                        else:
                            sql += keyv
                            sql += '%s,' % info[keyv]
                    sql = sql[:-1]
                try:
                    a = datetime.datetime.now()
                    conn = self.mysqlpool.connection()
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                    b = datetime.datetime.now()
                except:
                    print "error", sql
                finally:
                    cursor.close()
                    conn.close()
                    return
        elif self.target["dbtype"] == 'opensearch':
            for tabName, data in item.items():
                postData = []
                for k, fields in enumerate(data):
                    fields = self.__cleanNone(fields)
                    if len(fields) > 0:
                        tmp = {
                            "cmd": "update",
                            'timestamp': int(round(time.time() * 1000)),
                            'fields': fields
                        }
                        postData.append(tmp)
                flag = self.opensearchClients["target"][str(threadid)].runPost(table_name=tabName,
                                                                               body_json=json_util.dumps(postData,
                                                                                                         ensure_ascii=False))
                if flag == False:
                    print flag, startpos, tabName, json_util.dumps(postData, ensure_ascii=False)

    def callback(self, startpos, endpos):
        oriStartPos = startpos
        flagMongo = False
        if self.source["dbtype"] == "mongo":
            flagMongo = True
        threadid = self.jobName + "_" + str(self.occurs) + "_" + str(startpos)
        info = self.configPasor.findProgress(threadid)
        finished = int(info["finished"])
        position = []
        if type(info) is dict and info.has_key('position'):
            position = info["position"].split(',')
            if flagMongo:
                startpos = ObjectId(position[0])
            else:
                startpos = int(position[0])
        if len(position) <= 0:
            position.append(str(startpos))
            position.append(str(endpos))

        cursor = self.getCursor(threadid, startpos)
        cursor.batch_size(1000)
        cnt = 0
        bulk = None
        if self.target["dbtype"] == 'opensearch':
            bulk = []
        for document in cursor:
            finished += 1
            if bulk == None:
                retFlag = self.update(document, oriStartPos)
            else:
                bulk.append(document)
                if len(bulk) == 500:
                    self.update(bulk, oriStartPos)
                    bulk = []
            if flagMongo:
                cnt += 1
                position[0] = str(document["_id"])
                if cnt > 1000:
                    item = {
                        "threadid": threadid,
                        "status": 1,
                        "end_time": 0,
                        "position": ",".join(position),
                        "finished": finished
                    }
                    self.configPasor.updateProgress(item)
                    cnt = 0
                if document["_id"] > ObjectId(endpos):
                    item = {
                        "threadid": threadid,
                        "status": 2,
                        "end_time": int(round(time.time())),
                        "position": ",".join(position),
                        "finished": finished
                    }
                    self.configPasor.updateProgress(item)
                    cnt = 0
                    break
            else:
                cnt += 1
                position[0] = str(startpos)
                if cnt > 1000:
                    startpos += cnt
                    item = {
                        "threadid": threadid,
                        "status": 1,
                        "end_time": 0,
                        "position": ",".join(position),
                        "finished": finished
                    }
                    self.configPasor.updateProgress(item)
                    cnt = 0

                if startpos > oriStartPos + self.pageSize:
                    item = {
                        "threadid": threadid,
                        "status": 2,
                        "end_time": int(round(time.time())),
                        "position": ",".join(position),
                        "finished": finished
                    }
                    self.configPasor.updateProgress(item)
                    cnt = 0
                    break
        if type(bulk) is list and len(bulk) > 0:
            self.update(bulk, oriStartPos)

        item = {
            "threadid": threadid,
            "status": 2,
            "end_time": int(round(time.time())),
            "position": ",".join(position),
            "finished": finished
        }
        self.configPasor.updateProgress(item)
        cursor.close()
        self.__close_connection(oriStartPos)
