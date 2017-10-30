#!/usr/bin/env python
# coding=utf8
# author = MichelLiu

import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

def SAMPLEFIELD(value):
    return int(value)