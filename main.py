#!/usr/bin/env python
# coding=utf8
# MichelLiu
import getopt
import os
import sys
from bson import json_util
from lib.metaData import config_parser
import ConfigParser
from configdb.configdb import initDB

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

from datatransfer.dataMaker import dataTransfer

def main(argv):
    jobName = None
    envType = None

    try:
        opts, args = getopt.getopt(argv, "h:j:e:", ["help=", "jobname=", "envType="])
    except getopt.GetoptError:
        print 'main.py -j <job name> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            print '''
                    usage: main.py -j <job name> -e <enviroment (Sandbox or Online)>
                    Options and arguments:
                        -h | --help       帮助命令
                        -j | --jobname     配置文件的路径
                        -e | --envType   环境 Sandbox 表示测试环境 Online 表示线上环境
                    '''
            sys.exit()
        elif opt in ["-j", "--jobname"]:
            jobName = arg
        elif opt in ["-e", "--envType"]:
            envType = arg

    if None == jobName or None == envType:
        print 'main.py -j <job name> -e <enviroment (Sandbox or Online)>'
        sys.exit(2)

    dbcfg = initDB(envType)

    configPasor = config_parser(jobName,dbcfg)
    configInfo = configPasor.getConfigInfo()

    if None == configInfo:
        print "config not exists or config format error"
        sys.exit(2)

    obj = dataTransfer(configInfo,configPasor,jobName)
    obj.run()

if __name__ == '__main__':
    main(sys.argv[1:])