#!/usr/bin/env python
# coding=utf8
import getopt

__author__ = 'Liu Wei (wei.liu@gemdata.net)'

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

import ConfigParser

tz = pytz.timezone('Asia/Shanghai')
from bson import json_util

lpath = os.path.dirname(os.path.abspath(__file__))

@app.route("/getConfig",methods= ["GET"])
def getConfig():
    jobname = request.args.get('jobname')
    envType = request.args.get('envType')
    if jobname is None or len(jobname) <= 0 or envType is None or len(envType) <= 0:
        return make_response("Paramter not Acceptable")

    fpath = lpath + "/config/" + jobname + "/" + envType + ".json"
    if os.path.exists(fpath) == False:
        return make_response("config not exists")

    fdata = open(fpath,"rb")
    try:
        jsonStr = fdata.read()
        configContent = json_util.loads(jsonStr)
    except:
        fdata.close()
        return make_response("config not exists or config format error")

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


def main(argv):
    # console_out()
    global entpriseData,eventGenerator
    cfg = None
    eventEnv = None
    port = 10241

    try:
        opts, args = getopt.getopt(argv,"h:c:e:p:", ["help=","config=","eventEnv=","port="])
    except getopt.GetoptError:
        print 'console.py -c <config file> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-h","--help"]:
            print '''
            usage: console.py -c <config file> -e <enviroment (Sandbox or Online)>
            Options and arguments:
                -h | --help       帮助命令
                -c | --config     配置文件的路径
                -e | --eventEnv   事件生成的环境 Sandbox 表示测试环境 Online 表示线上环境
                -p | --port       端口
            '''
            sys.exit()
        elif opt in ["-c","--config"]:
            cfg = arg
        elif opt in ["-e","--eventEnv"]:
            eventEnv = arg
        elif opt in ["-p","--port"]:
            port = int(arg.strip(' '))

    if None == cfg or None == eventEnv:
        print 'console.py -c <config file> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    entpriseData = dataCenter(cfg, eventEnv)
    eventGenerator = eventGenerate.eventGenerate(cfg, eventEnv)
    if None == entpriseData or None == eventGenerator:
        sys.stderr.write("Connect MongoDB Failed.\n")
        sys.exit(1)

    app.run(host="0.0.0.0",port=port,threaded=True)

if __name__ == "__main__":
    main(sys.argv[1:])
