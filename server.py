#!/usr/bin/env python
# coding=utf8
import getopt
import socket

__author__ = 'Liu Wei (MichelLiu)'

import json
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import datetime,time

import pytz
import subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from flask import Flask,request,make_response
app = Flask(__name__)

import ConfigParser
from lib.metaData import config_parser
from configdb.configdb import initDB

tz = pytz.timezone('Asia/Shanghai')
from bson import json_util

lpath = os.path.dirname(os.path.abspath(__file__))

envType = None
port = None
dbConfig = None

@app.route("/getFormat",methods=["GET"])
def getFormat():
    try:
        jobname = request.args.get('jobname')
        configPar = config_parser(jobname, dbConfig)
        pyscript = configPar.getPyScript()
        if None == pyscript or len(pyscript) <= 0:
            data = '''
#!/usr/bin/env python
# coding=utf8
# MichelLiu
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
           
           '''
            return make_response(json_util.dumps({"error_code": 0, "success": True, "message": "empty script", "data":data},
                                                 ensure_ascii=False))
        return make_response(json_util.dumps({"data": pyscript, "error_code": 0, "success": True, "message":""}, ensure_ascii=False))
    except:
        return make_response(json_util.dumps({"error_code": 400, "success": False, "message":"openfile failed"}, ensure_ascii=False))

@app.route("/uploadFormat",methods= ["POST"])
def uploadFormat():
    jobname = request.json["jobname"]
    try:
        pyscript = request.json["pyscript"]
        configPar = config_parser(jobname, dbConfig)
        configPar.updatePyScript(pyscript)
        return make_response(json_util.dumps({"error_code": 0, "success": True, "message": ""}, ensure_ascii=False))
    except:
        return make_response(json_util.dumps({"error_code": 400, "success": False, "message": "upload file failed"}, ensure_ascii=False))


@app.route("/startDataStream",methods=["GET"])
def startDataStream():
    jobname = request.args.get('jobname')
    try:
        configPar = config_parser(jobname,dbConfig)
        if configPar.loadPyScript():
            cmd = "python " + lpath + '/main.py -j ' + jobname + " -e " + envType
            if configPar.isValidatedJob():
                progress = subprocess.Popen(cmd,shell=True)
                configPar.updateRunInfo(socket.gethostbyname(socket.gethostname()) + ":" + str(port), progress.pid, 1)
                return make_response(json_util.dumps(
                    {"error_code": 0, "success": True, "message": cmd + " starting"},
                    ensure_ascii=False))
            return make_response(json_util.dumps({"error_code": 400, "success": False, "message": cmd + " still running or finished already"}, ensure_ascii=False))
        else:
            return make_response(json_util.dumps(
                {"error_code": 400, "success": False, "message": "load script failed"},
                ensure_ascii=False))
    except Exception, e:
        ms = e.message
        return make_response(json_util.dumps(
            {"error_code": 400, "success": False, "message": "start " + jobname + " failed"},
            ensure_ascii=False))


@app.route("/stopDataStream",methods=["GET"])
def stopDataStream():
    jobname = request.args.get('jobname')
    try:
        pid = int(request.args.get('pid'))
        os.kill(pid,9)
        configPar = config_parser(jobname, dbConfig)
        configPar.updateRunInfo('', -1,3)
        return make_response(json_util.dumps(
            {"error_code": 0, "success": True, "message": "kill job " + jobname + " success"},
            ensure_ascii=False))
    except Exception, e:
        ms = e.message
        return make_response(json_util.dumps(
            {"error_code": 400, "success": False, "message": "kill job " + jobname + " failed"},
            ensure_ascii=False))

def main(argv):
    global envType,port,dbConfig
    port = 10222
    try:
        opts, args = getopt.getopt(argv,"h:e:p:", ["help=","envType=","port="])
    except getopt.GetoptError:
        print 'server.py -p <port>'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-h","--help"]:
            print '''
            usage: print 'server.py -p <port>'
            Options and arguments:
                -h | --help       帮助命令
                -e | --envType    生成的环境 local 表示本地环境 sandbox 表示测试环境 online 表示线上环境
                -p | --port       端口
            '''
            sys.exit()
        elif opt in ["-e","--envType"]:
            envType = arg
        elif opt in ["-p","--port"]:
            port = int(arg.strip(' '))

    if None == port or envType == None or envType not in ["local","sandbox","online"]:
        print 'server.py -p <port> -e <local or sandbox or online>'
        sys.exit(2)

    dbConfig = initDB(envType)

    app.run(host="0.0.0.0",port=port,threaded=True)

if __name__ == "__main__":
    main(sys.argv[1:])
