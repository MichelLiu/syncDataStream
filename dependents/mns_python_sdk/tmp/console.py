#!/usr/bin/env python
# coding=utf8
__author__ = 'Liu Wei (MichelLiu)'

import json
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import datetime,time

import pytz

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from flask import Flask,request,make_response
app = Flask(__name__)

from bson import ObjectId,json_util
import ConfigParser

import eventGenerate
from event_data_Center import dataCenter
import event_data_Center

tz = pytz.timezone('Asia/Shanghai')


def console_out():
    title = '''
        event type list:
        1:  股东信息[变更]
        2:  股东出资[变更]
        3:  主要人员[变更]
        4:  对外投资[新增]
        5:  行政处罚
        6:  经营异常[列入]
        7:  清算信息
        8:  抽查信息
        9:  司法协助
        10: 欠税信息
        11: 税务非正常户
        12: 税务重大违法
        13: 被执行人
        14: 失信信息
        15: 开庭公告
        16: 法院公告
        17: 法院判决
        18: 司法违法失信企业[列入]
        19: 营业执照信息[变更]
        20: 工商变更[变更]
        21: 子公司与分支机构[新增]
        22: 企业年报[变更]
        23: 企业年报[新增]
        24: 经营异常[移出]
        25: 司法违法失信企业[移出]
        '''
    return title

def use_input(eventGen):
    company = raw_input("Please type a companyName or quit>");
    if "quit" == company:
        return 0
    eventType = raw_input("Choose an eventType between [1 , 25]>")
    try:
        if int(eventType) > 0 and int(eventType) < 26 and len(company) > 0:
            eventGen.sendmsg(company,int(eventType))
        else:
            print "companyName or eventType not acceptable"
    except TypeError, e:
        print e

    return 1


cfg = "sample.cfg"
eventEnv = "Sandbox"
entpriseData = dataCenter(cfg, eventEnv)
eventGenerator = eventGenerate.eventGenerate(cfg,eventEnv)

@app.route("/generatorEvent",methods= ["GET"])
def generatorEvent():
    company = request.args.get('company')
    eventType = request.args.get('type')
    if company is None or len(company) <= 0 or eventType is None or len(eventType) <= 0:
        return make_response("Paramter not Acceptable")

    pid = entpriseData.queryCompanyId(company)
    if pid is None:
        return make_response("This company not exists")

    print pid
    eventInfo = entpriseData.queryEventInfo(pid,int(eventType))

    if eventInfo is None:
        return make_response("This company doesn't have this event")

    if type(eventInfo) is list and len(eventInfo) == 0:
        return make_response("This company doesn't have this event")


    ret = eventGenerator.pushEvent(company,pid, eventType,eventInfo)
    if ret:
        return make_response("Event Msg send success")
    return make_response("Event Msg send failed")


@app.route("/generatorChangeEvent",methods= ["POST"])
def generatorChangeEvent():
    company = request.json["company"]
    pid = request.json["pid"]
    eventType = int(request.json["eventType"])
    eventInfo = request.json["eventInfo"]
    ret = eventGenerator.pushEvent(company, pid, eventType, eventInfo)
    if ret:
        return make_response("Event Msg send success")
    return make_response("Event Msg send failed")


@app.route("/getEventTypeDefine",methods= ["GET"])
def getEventTypeDefine():
    return make_response(console_out())


def main():
    # console_out()
    app.run(host="0.0.0.0",port=9000,threaded=True)
    # obj = getattr(event_data_Center,"EnterpriseBaseInfo")(entpriseData.mongoDataBase,eventGenerator)
    # obj.run()


if __name__ == "__main__":
    main()