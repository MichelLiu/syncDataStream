#!/usr/bin/env python
# coding=utf8
# author = wei.liu@gemdata.net

import os
import sys

import threadpool

reload(sys)
sys.setdefaultencoding('utf8')

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")

class ThreadPool:
    def __init__(self,occurs=1,offset=0):
        self.occurs = occurs
        self.offset = offset

    def setOffset(self,offset):
        self.offset = int(offset)

    def setOccurs(self,occurs):
        self.occurs = int(occurs)

    def customize_variable(self,startpos):
        pass

    def getDocCnt(self):
        pass

    def callback(self,startpos):
        pass

    def docMapTabelRecord(self,offset):
        docCnt = self.getDocCnt()
        if docCnt <= self.occurs:
            self.pageSize = docCnt
        else:
            self.pageSize = int(round(docCnt / self.occurs))
        docMap = []
        for step in range(0, int(round(docCnt / self.pageSize))):
            skippos = step * self.pageSize + offset
            docMap.append((None, {'startpos': skippos}))
            self.customize_variable(skippos)

        print "docMap:", docMap
        return docMap

    def run(self):
        self.docMap = self.docMapTabelRecord(self.offset)

        pool = threadpool.ThreadPool(self.occurs)
        requests = threadpool.makeRequests(self.callback, self.docMap)
        [pool.putRequest(req) for req in requests]
        pool.wait()
