#!/usr/bin/env python
# coding=utf8
# author = wei.liu@gemdata.net

import os
import sys
from bson import json_util
from bson.objectid import ObjectId
import pinyin

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

def ID(value):
    return int(value)