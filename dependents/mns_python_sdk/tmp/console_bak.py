#!/usr/bin/env python
# coding=utf8

import os
import sys
import time
import eventdata
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

import sys
import time
from mns.account import Account
from mns.queue import *
from mns.topic import *
from mns.subscription import *
import ConfigParser

class MNSPushEvent:
    def __init__(self):
        cfgFN = "sample.cfg"
        required_ops = [("Base", "AccessKeyId"), ("Base", "AccessKeySecret"), ("Base", "Endpoint")]
        optional_ops = [("Optional", "SecurityToken")]

        parser = ConfigParser.ConfigParser()
        parser.read(cfgFN)
        for sec, op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec, op, cfgFN))
                sys.stderr.write("Read README to get help inforamtion.\n")
                sys.exit(1)

        # 获取配置信息
        ## AccessKeyId      阿里云官网获取
        ## AccessKeySecret  阿里云官网获取
        ## Endpoint         阿里云消息和通知服务官网获取, Example: http://$AccountId.mns.cn-hangzhou.aliyuncs.com
        ## WARNING： Please do not hard code your accessId and accesskey in next line.(more information: https://yq.aliyun.com/articles/55947)
        accessKeyId = parser.get("Base", "AccessKeyId")
        accessKeySecret = parser.get("Base", "AccessKeySecret")
        endpoint = parser.get("Base", "Endpoint")
        securityToken = ""
        if parser.has_option("Optional", "SecurityToken") and parser.get("Optional",
                                                                         "SecurityToken") != "$SecurityToken":
            securityToken = parser.get("Optional", "SecurityToken")

        # 初始化my_account
        my_account = Account(endpoint, accessKeyId, accessKeySecret, securityToken)

        ##############Queue 相关操作#####################
        self.my_queue = my_account.get_queue("CIO-ALERTINFO")
        #self.my_queue = my_account.get_queue("TEST-FOR-MNSPUSH")

    def sendMsgContent(self, event_name, company, pid, ttype, happen_date, content):
        try:
            TEMP = {
                "event_name": event_name,
                "event_content": content,
                "pid": pid,
                "entname":company,
                "happen_date": happen_date,
                "type": ttype
            }
            # print(TEMP)
            msg = Message(json.dumps(TEMP, ensure_ascii=False))
            re_msg = self.my_queue.send_message(msg)
            print "Send Message Succeed! MessageBody:%s MessageID:%s" % (msg, re_msg.message_id)
            return True
        except MNSExceptionBase, e:
            print "Send Message Fail!\nException:%s\n\n" % e
            return False

    def sendmsg(self,company,ttype):
        try:
            msg = Message(eventdata.generateData(company,ttype))
            re_msg = self.my_queue.send_message(msg)
            print "Send Message Succeed! MessageBody:%s MessageID:%s" % (msg, re_msg.message_id)
            return True
        except MNSExceptionBase, e:
            print "Send Message Fail!\nException:%s\n\n" % e
            return False


def console_out():
    title = '''
        aliyun mns console
        event type list:
        (1) “股东信息[变更]”
        (2) “出资信息[变更]”
        (3) “主要人员[变更]”
        (4) “对外投资[新增]”
        (5) “行政处罚”
        (6) “经营异常[列入]”
        (7) “清算信息”
        (8) “抽查信息”
        (9) “司法协助”
        (10) “欠税信息”
        (11) “税务非正常户”
        (12) “税务重大违法”
        (13) “被执行人”
        (14) “失信信息”
        (15) “开庭公告”
        (16) “法院公告”
        (17) “法院判决”
        (18) “司法违法失信企业[列入]”
        (19) “营业执照信息[变更]”
        (20) “工商变更[变更]”
        (21) “子公司与分支机构[新增]”
        (22) “企业年报[变更]”
        (23) “企业年报[新增]”
        (24) “经营异常[移出]”
        (25) “司法违法失信企业[移出]”
        '''
    print title

def use_input(mnsClient):
    company = raw_input("Please type a companyName or quit>");
    if "quit" == company:
        return 0
    eventType = raw_input("Choose an eventType between [1 , 25]>")
    try:
        if int(eventType) >= 0 and int(eventType) < 26 and len(company) > 0:
            mnsClient.sendmsg(company,int(eventType))
        else:
            print "companyName or eventType not acceptable"
    except TypeError, e:
        print e

    return 1



def main():
    console_out()
    mnsClient = MNSPushEvent()
    while(1==use_input(mnsClient)):
        pass


if __name__ == "__main__":
    main()
