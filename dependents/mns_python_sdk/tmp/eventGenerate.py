#!/usr/bin/env python
# coding=utf8
__author__ = 'Liu Wei (MichelLiu)'

import os
import sys
import time

import bson

reload(sys)
sys.setdefaultencoding('utf8')

import collections
import types
from dateutil.parser import parser
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from mns.account import Account
from mns.queue import *
from mns.topic import *
from mns.subscription import *
import ConfigParser

class eventGenerate:
    def __init__(self,eventEnvCfg, cioEnv):
        confkey = cioEnv + "-Base"
        Opkey = cioEnv + "-Optional"
        required_ops = [("Sandbox-Base", "AccessKeyId"), ("Sandbox-Base", "AccessKeySecret"), ("Sandbox-Base", "Endpoint"),("Sandbox-Base", "MongoDB"),("Sandbox-Base", "mnsQueue"),("Online-Base", "AccessKeyId"), ("Online-Base", "AccessKeySecret"), ("Online-Base", "Endpoint"),("Online-Base", "MongoDB"),("Online-Base", "mnsQueue")]
        optional_ops = [("Sandbox-Optional", "SecurityToken"),("Online-Optional", "SecurityToken")]

        parser = ConfigParser.ConfigParser()
        parser.read(eventEnvCfg)
        for sec, op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec, op, eventEnvCfg))
                sys.stderr.write("Read README to get help inforamtion.\n")
                sys.exit(1)

        # 获取配置信息
        ## AccessKeyId      阿里云官网获取
        ## AccessKeySecret  阿里云官网获取
        ## Endpoint         阿里云消息和通知服务官网获取, Example: http://$AccountId.mns.cn-hangzhou.aliyuncs.com
        ## WARNING： Please do not hard code your accessId and accesskey in next line.(more information: https://yq.aliyun.com/articles/55947)
        accessKeyId = parser.get(confkey, "AccessKeyId")
        accessKeySecret = parser.get(confkey, "AccessKeySecret")
        endpoint = parser.get(confkey, "Endpoint")
        mongoConnection = parser.get(confkey, "MongoDB")
        mnsQueue = parser.get(confkey, "mnsQueue")
        securityToken = ""
        if parser.has_option(Opkey, "SecurityToken") and parser.get(Opkey,
                                                                         "SecurityToken") != "$SecurityToken":
            securityToken = parser.get(Opkey, "SecurityToken")

        # 初始化my_account
        my_account = Account(endpoint, accessKeyId, accessKeySecret, securityToken)
        print mnsQueue
        self.my_queue = my_account.get_queue(mnsQueue)

    def sendMsg(self, event_name, company, pid, ttype, happen_date, content):
        try:
            content = self._isEventContentValidate(content)
            if None != content:
                if happen_date is None:
                    happen_date = int(round(time.time() * 1000))
                TEMP = {
                    "event_name": event_name,
                    "event_content": content,
                    "pid": pid,
                    "entname": company,
                    "happen_date": self._dateToTimeStamp(happen_date),
                    "type": ttype
                }
                msg = Message(json.dumps(TEMP, ensure_ascii=False))
                re_msg = self.my_queue.send_message(msg)
                print json.dumps(TEMP, ensure_ascii=False)
                print "Send Message Succeed! MessageBody:%s MessageID:%s" % (msg, re_msg.message_id)
                return True
            else:
                print "Event Content is empty!"
                return False
        except MNSExceptionBase, e:
            print "Send Message Fail!\nException:%s\n\n" % e
            return False


    def _responseReturn(self ,is_sendmsg):
        if False == is_sendmsg:
            return "Generate Event Failed\r\n"
        else:
            return "Generate Event Success:\r\n"

    def _handle_dict(self,obj):
        def handle():
            return None
        if type(obj) is dict:
            obj = collections.defaultdict(handle,obj)
            return obj

    def _dateToTimeStamp(self,datestr):
        try:
            if type(datestr) is types.StringType or type(datestr) is unicode:
                if datestr.isdigit():
                    return datestr
                dt = parser(datestr)
                out = re.findall('[0-9]+',datestr)

                t = (int(out[0]),int(out[1]),int(out[2]),0,0,0,0,0,0)
                return int(round(1000 * time.mktime(t)))
            return datestr
        except:
            return None

    def _isEventContentValidate(self,jsonObj):
        ret = False
        if type(jsonObj) is dict:
            for key,val in jsonObj.items():
                if -1 < key.upper().find('DATE'):
                    val = self._dateToTimeStamp(val)
                    jsonObj[key] = val
                if val is None:
                    jsonObj[key] = ''
                    ret = True
                elif type(val) is long or type(val) is int or type(val) is bson.int64.Int64:
                    ret = True
                elif type(val) is types.StringType and len(val) > 0:
                    ret = True
                elif type(val) is dict:
                    ret = True
                    for k,v in val.items():
                        jsonObj[key] = v

        elif type(jsonObj) is list:
            tmpArr = []
            for i, val in enumerate(jsonObj):
                for k,v in val.items():
                    if len(v) > 0:
                        tmpArr.append(val)
            if len(tmpArr) > 0:
                return tmpArr

        if ret:
            return jsonObj
        return None

    def pushEvent(self,company,pid, eventType,eventBody):
        func_name = "pushEvent" + str(eventType)
        ret = getattr(self,func_name)(company,pid,eventBody)
        return ret

    # 1:  股东信息[变更]
    def pushEvent1(self,company, pid, eventBody):
        pass


    # 2:  股东出资[变更]
    def pushEvent2(self,company, pid, eventBody):
        pass


    # 3:  主要人员[变更]
    def pushEvent3(self,company, pid, eventBody):
        pass


    # 4:  对外投资[新增]
    def pushEvent4(self,company, pid, eventBody):
        pass


    # 5:  行政处罚 PUNISHINFO
    def pushEvent5(self,company, pid, eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'行政处罚', company, pid, 5,event_content['penDecIssDate'],{
                        'penDecNo': event_content['penDecNo'],
                        'illegActType': event_content['illegActType'],
                        'penContent': event_content['penContent'],
                        'penAuth_CN': event_content['penAuth_CN'],
                        'penDecIssDate': event_content['penDecIssDate'],
                        'publicDate': event_content['publicDate']
                    })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 6:  经营异常[列入]
    def pushEvent6(self,company, pid, eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'经营异常[列入]', company, pid, 6, event_content["abntime"],{
                    'speCause_CN': event_content['speCause_CN'],
                    'abntime': event_content['abntime'],
                    'decOrg_CN': event_content['decOrg_CN']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 7:  清算信息
    def pushEvent7(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            tmpArr = []
            for k,event_content in enumerate(eventBody):
                event_content = self._handle_dict(event_content)
                tmp = {"ligpriSign": event_content["ligpriSign"], "liqMem": event_content["liqMem"]}
                tmpArr.append(tmp)
            ret = self.sendMsg(u'清算信息', company, pid, 7, int(round(time.time() * 1000)), tmpArr)
            if ret:
                is_sendmsg = True

        return is_sendmsg


    # 8:  抽查信息
    def pushEvent8(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'抽查信息', company, pid, 8, event_content["insDate"], {
                    'insAuth_CN': event_content['insAuth_CN'],
                    'insType_CN': event_content['insType_CN'],
                    'insDate': event_content['insDate'],
                    'insRes_CN': event_content['insRes_CN']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 9:  司法协助
    def pushEvent9(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'司法协助', company, pid, 9, int(round(time.time() * 1000)), {
                    'inv': event_content['inv'],
                    'froAm': event_content['froAm'],
                    'froAuth': event_content['froAuth'],
                    'executeNo': event_content['executeNo'],
                    'type': event_content['type']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 10: 欠税信息 TaxArrears
    def pushEvent10(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'欠税信息', company, pid, 10, event_content['EVENTDATE'], {
                    'CREATEDATE': event_content['CREATEDATE'],
                    'TAXID': event_content['TAXID'],
                    'TAXTYPE': event_content['TAXTYPE'],
                    'TAXAMOUNT': event_content['TAXAMOUNT'],
                    'LAWERNAME': event_content['LAWERNAME'],
                    'LAWERCREDTYPE': event_content['LAWERCREDTYPE'],
                    'LAWERCREDID': event_content['LAWERCREDID'],
                    'COMPANYNAME': event_content['COMPANYNAME']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 11: 税务非正常户 TaxIllegal
    def pushEvent11(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'税务非正常户', company, pid, 11, event_content['EVENTDATE'], {
                    'CREATEDATE': event_content['CREATEDATE'],
                    'TAXID': event_content['TAXID'],
                    'LAWERNAME': event_content['LAWERNAME']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 12: 税务重大违法 TaxUnusual
    def pushEvent12(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'税务重大违法', company, pid, 12, event_content['EVENTDATE'], {
                    'CREATEDATE': event_content['CREATEDATE'],
                    'TAXID': event_content['TAXID'],
                    'CASEKIND': event_content['CASEKIND'],
                    'LAWERINFO': event_content['LAWERINFO'],
                    'MAINCASE': event_content['MAINCASE'],
                    'PUNISHMENT': event_content['PUNISHMENT']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 13: 被执行人 Enforcement
    def pushEvent13(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'被执行人', company, pid, 13, event_content['FILINGDATE'], {
                    'FILINGDATE': event_content['FILINGDATE'],
                    'COURT': event_content['COURT'],
                    'REFERENCENO': event_content['REFERENCENO'],
                    'ENFOBJECT': event_content['ENFOBJECT'],
                    'ENFNAME': event_content['ENFNAME'],
                    'CARDNO': event_content['CARDNO']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 14: 失信信息 Dishonest
    def pushEvent14(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'失信信息', company, pid, 14, event_content['PUBDATE'], {
                    'REFERENCENO': event_content['REFERENCENO'],
                    'ENFDUTY': event_content['ENFDUTY'],
                    'ENFSTATUS': event_content['ENFSTATUS'],
                    'ENFNO': event_content['ENFNO'],
                    'CARDNO': event_content['CARDNO'],
                    'PROVINCE': event_content['PROVINCE'],
                    'PERSON': event_content['PERSON'],
                    'ENFNAME': event_content['ENFNAME'],
                    'PUBDATE': event_content['PUBDATE'],
                    'ENFSITUATION': event_content['ENFSITUATION'],
                    'COURT': event_content['COURT'],
                    'FILINGDATE': event_content['FILINGDATE'],
                    'ENFUNIT': event_content['ENFUNIT']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 15: 开庭公告
    def pushEvent15(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'开庭公告', company, pid, 15, None, {
                    'FILINGDATE': event_content['FILINGDATE'],
                    'COURT': event_content['COURT'],
                    'TRIBUNAL': event_content['TRIBUNAL'],
                    'COURTDATE': event_content['COURTDATE'],
                    'SCHEDULEDATE': event_content['SCHEDULEDATE'],
                    'REFERENCENO': event_content['REFERENCENO'],
                    'DEPARTMENT': event_content['DEPARTMENT'],
                    'JUDGE': event_content['JUDGE'],
                    'ACCUSER': event_content['ACCUSER'],
                    'DEFENDANT': event_content['DEFENDANT'],
                    'PARTY': event_content['PARTY'],
                    'BRIEF': event_content['BRIEF']
                })
            if ret:
                is_sendmsg = True

        return is_sendmsg


    # 16: 法院公告 CourtAnno
    def pushEvent16(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'法院公告', company, pid, 16, event_content['ANNODATE'], {
                    'ANNOTPYE': event_content['ANNOTPYE'],
                    'ANNOER': event_content['ANNOER'],
                    'LAWERNAME': event_content['LAWERNAME'],
                    'ANNODATE': event_content['ANNODATE'],
                    'CONTENT': event_content['CONTENT'],
                    'PUBLISHED': event_content['PUBLISHED'],
                    'PUBLISHDATE': event_content['PUBLISHDATE'],
                    'UPLOADDATE': event_content['UPLOADDATE']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 17: 法院判决
    def pushEvent17(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'法院判决', company, pid, 17, event_content['RELEASEDATE'], {
                    'PARTY': event_content['PARTY'],
                    'REFEREEDATE': event_content['REFEREEDATE'],
                    'CASETYPE': event_content['CASETYPE'],
                    'COMPANYTYPE': event_content['COMPANYTYPE'],
                    'RELEASEDATE': event_content['RELEASEDATE'],
                    'COMPANYNAME': event_content['COMPANYNAME'],
                    'WRITID': event_content['WRITID'],
                    'CASENAME': event_content['CASENAME']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 18: 司法违法失信企业[列入]
    def pushEvent18(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'司法违法失信企业[列入]', company, pid, 18, event_content['abntime'], {
                    'type': event_content['type'],
                    'serILLRea_CN': event_content['serILLRea_CN'],
                    'abntime': event_content['abntime'],
                    'decOrg_CN': event_content['decOrg_CN']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 19: 营业执照信息[变更]
    def pushEvent19(self, company, pid,eventBody):
        pass


    # 20: 工商变更[变更]
    def pushEvent20(self, company, pid,eventBody):
        pass


    # 21: 子公司与分支机构[新增]
    def pushEvent21(self, company, pid,eventBody):
        return self.sendMsg(u'行政处罚', company, pid, 21, None, eventBody)


    # 22: 企业年报[变更]
    def pushEvent22(self, company, pid,eventBody):
        pass


    # 23: 企业年报[新增]
    def pushEvent23(self, company, pid,eventBody):
        pass


    # 24: 经营异常[移出]
    def pushEvent24(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg('经营异常[移出]', company, pid, 24, event_content["remDate"], {
                    "speCause_CN": event_content['speCause_CN'],
                    "abntime": event_content['abntime'],
                    "decOrg_CN": event_content['decOrg_CN'],
                    "remExcpRes_CN": event_content['remExcpRes_CN'],
                    "remDate": event_content['remDate'],
                    "reDecOrg_CN": event_content['reDecOrg_CN']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


    # 25: 司法违法失信企业[移出]
    def pushEvent25(self, company, pid,eventBody):
        is_sendmsg = False
        if eventBody is None or 0 == len(eventBody):
            is_sendmsg = False
        else:
            for event_content in eventBody:
                event_content = self._handle_dict(event_content)
                ret = self.sendMsg(u'司法违法失信企业[移出]', company, pid, 25, event_content['remDate'], {
                    'type': event_content['type'],
                    'serILLRea_CN': event_content['serILLRea_CN'],
                    'abntime': event_content['abntime'],
                    'decOrg_CN': event_content['decOrg_CN'],
                    'remExcpRes_CN': event_content['remExcpRes_CN'],
                    'remDate': event_content['remDate'],
                    'reDecOrg_CN': event_content['reDecOrg_CN'],
                    'reDecOrg_CN': event_content['reDecOrg_CN']
                })
                if ret:
                    is_sendmsg = True

        return is_sendmsg


def main():
    pass


if __name__ == "__main__":
    main()
