#!/usr/bin/env python
# coding=utf8
# MichelLiu
import os
import sys

import ConfigParser
reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

def initDB(envType):
    required_ops = [("local", "db"), ("local", "user"),("local","passwd"),("local","charset"),("local","host"),("local","port"),
                    ("sandbox", "db"), ("sandbox", "user"),("sandbox","passwd"),("sandbox","charset"),("sandbox","host"),("sandbox","port"),
                    ("online", "db"), ("online", "user"),("online","passwd"),("online","charset"),("online","host"),("online","port"),]

    parser = ConfigParser.ConfigParser()
    parser.read(os.path.dirname(os.path.abspath(__file__)) + '/configdb.cfg')
    for sec, op in required_ops:
        if not parser.has_option(sec, op):
            sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec, op, cfg))
            sys.stderr.write("Read README to get help inforamtion.\n")
            sys.exit(1)
    return {
        "db": parser.get(envType,"db"),
        "user": parser.get(envType,"user"),
        "passwd": parser.get(envType,"passwd"),
        "host": parser.get(envType,"host"),
        "port": int(parser.get(envType,"port")),
        "charset": parser.get(envType,"db"),
    }