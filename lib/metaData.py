#!/usr/bin/env python
# coding=utf8
# MichelLiu
import getopt
import os
import sys

import time
from bson import json_util
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

class config_parser:
    def __init__(self,jobName,dbcfg):
        self.jobName = jobName
        self.dbcfg = dbcfg

    def getConnection(self):
        return MySQLdb.connect(host=self.dbcfg["host"],port=self.dbcfg["port"], user=self.dbcfg["user"],
                                   passwd=self.dbcfg["passwd"], db=self.dbcfg["db"],
                                   charset=self.dbcfg["charset"])

    def getConfigInfo(self):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            cursor.execute("select * from data_stream_config where name='%s'" % self.jobName)
            doc = cursor.fetchone()
            cursor.close()
            conn.close()
            if 1 != int(doc["status"]):
                return None
            return {
                "id": doc["id"],
                "jobname": doc["name"],
                "source": json_util.loads(doc["source"]),
                "target": json_util.loads(doc["target"]),
                "occurs": int(doc["occurs"]),
                "upsert": int(doc["upsert"]),
                "fieldMap": json_util.loads(doc["fieldMap"])
            }
        except:
            return None

    def addProgress(self,info):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            sql = '''
            insert into data_stream_progress(threadid,total,status,position,create_time) 
            values(\'%s\',%s,%s,\'%s\',%s)
            ''' % (info["threadid"],info["total"],info["status"],info["position"],int(round(time.time())))
            print sql
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except:
            return False

    def updateProgress(self,info):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            sql = '''
            update data_stream_progress set status=%s,position=\'%s\',end_time=%s,finished=%s
            where threadid=\'%s\'
            ''' % (info["status"], info["position"], info["end_time"], info["finished"], info["threadid"])
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except:
            return False

    def findProgress(self,threadid):
        conn = self.getConnection()
        cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql = '''
        select * from data_stream_progress
        where threadid=\'%s\'
        ''' % (threadid)
        cursor.execute(sql)
        doc = cursor.fetchone()
        cursor.close()
        conn.close()
        return doc

    def updateRunInfo(self,ip,pid,status):
        # try:
        conn = self.getConnection()
        cursor = conn.cursor()
        sql = '''
        update data_stream_config set ip=\'%s\',pid=%s,status=%s
        where name=\'%s\'
        ''' % (ip, pid,status,self.jobName)
        print sql
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return True
        # except:
        #     return False

    def isValidatedJob(self):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            cursor.execute("select count(*) as num from data_stream_config where name='%s' and status=1" % self.jobName)
            doc = cursor.fetchone()
            cursor.close()
            conn.close()
            if 1 == int(doc["num"]):
                return True
            else:
                return False
        except:
            return False

    def updatePyScript(self,pyscript):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            sql = '''
            update data_stream_config set pyscript=\'%s\'
            where name=\'%s\'
            ''' % (self.jobName, pyscript)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except:
            return False

    def loadPyScript(self):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            cursor.execute("select pyscript from data_stream_config where name='%s'" % self.jobName)
            doc = cursor.fetchone()
            cursor.close()
            conn.close()
            if None == doc["pyscript"]:
                return True
            if len(doc["pyscript"]) <= 0:
                return True
            lpath = os.path.dirname(os.path.abspath(__file__)) + "/.."
            fp = open(lpath + "/dataformat/userformat/" + self.jobName + '.py', "wb")
            fp.write(doc["pyscript"])
            fp.flush()
            fp.close()
            fpInit = open(lpath + "/dataformat/userformat/__init__.py", "rb")
            data = fpInit.read()
            fpInit.close()
            if -1 == data.find(self.jobName):
                data += "\r\nimport " + self.jobName
                fpInitRes = open(lpath + "/dataformat/userformat/__init__.py", "wb")
                fpInitRes.write(data)
                fpInitRes.flush()
                fpInitRes.close()
            return True
        except:
            return False

    def getPyScript(self):
        try:
            conn = self.getConnection()
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
            cursor.execute("select pyscript from data_stream_config where name='%s'" % self.jobName)
            doc = cursor.fetchone()
            cursor.close()
            conn.close()
            return doc["pyscript"]
        except:
            return None