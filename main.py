#!/usr/bin/env python
# coding=utf8
# author = wei.liu@gemdata.net
import getopt
import os
import sys
from bson import json_util

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from datatransfer.dataMaker import dataTransfer

def main(argv):
    jobName = None
    envType = None

    try:
        opts, args = getopt.getopt(argv, "h:j:e:", ["help=", "config=", "envType="])
    except getopt.GetoptError:
        print 'main.py -j <job name> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            print '''
                    usage: main.py -j <job name> -e <enviroment (Sandbox or Online)>
                    Options and arguments:
                        -h | --help       帮助命令
                        -j | --config     配置文件的路径
                        -e | --envType   环境 Sandbox 表示测试环境 Online 表示线上环境
                    '''
            sys.exit()
        elif opt in ["-j", "--job"]:
            jobName = arg
        elif opt in ["-e", "--envType"]:
            envType = arg

    if None == jobName or None == envType:
        print 'main.py -j <job name> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    configPath = os.path.dirname(os.path.abspath(__file__)) + "/config/" + jobName + "/" + envType + ".json"
    configContent = None
    if os.path.exists(configPath):
        fdata = open(configPath, 'rb')
        try:
            jsonStr = fdata.read()
            configContent = json_util.loads(jsonStr)
        except:
            print "config not exists or config format error"
            fdata.close()

    if None == configContent:
        print "config not exists or config format error"
        sys.exit(2)

    logPath = os.path.dirname(os.path.abspath(__file__)) + "/log/" + jobName + "/" + envType
    for index,configInfo in enumerate(configContent):
        obj = dataTransfer(configInfo,logPath,jobName)
        obj.setOccurs(10)
        obj.run()

if __name__ == '__main__':
    main(sys.argv[1:])