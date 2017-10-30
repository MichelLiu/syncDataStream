#!/usr/bin/env python
# coding=utf-8

__author__ = 'Edward Chen (edward_chen@koios.cn)'

from pymongo import MongoClient
import hashlib, sys, datetime, json, time
from  console import MNSPushEvent


CONNECTION_STRING_SANDBOX = 'mongodb://root:8tgEuNF2rRhiDY@127.0.0.1:37172/admin'
# CONNECTION_STRING_API = 'mongodb://rest_enterprise_backend:o6qrFXmKBpNEeQMui82@127.0.0.1:37171/admin'

def generate_event(company_file):
    client = MongoClient(CONNECTION_STRING_SANDBOX)

    mnsClient = MNSPushEvent()

    with open(company_file, 'rb') as fp:
        company = fp.readline()
        while company:
            company = company.strip(' \r\n')
            COM = client.enterprise['EnterpriseBaseInfo'].find_one({'ENTNAME': company})
            if COM:
                PID = COM['PID']
                try:
                    # 工商变更[变更]
                    if COM.has_key('ALTERINFO'):
                        for alterInfo in COM['ALTERINFO']:
                            print('Push one ALTERINFO info event for ' + company)
                            mnsClient.sendMsgContent(u'工商变更[变更]', company.decode('utf-8'), PID, 20, alterInfo['altDate'], {
                                "before":{
                                    alterInfo['altItem_CN']: alterInfo['altBe']
                                },
                                "after":{
                                    alterInfo['altItem_CN']: alterInfo['altAf']
                                }
                            })

                    # 行政处罚
                    if COM.has_key('PUNISHINFO'):
                        for punish in COM['PUNISHINFO']:
                            print('Push one punish info event for ' + company)
                            mnsClient.sendMsgContent(u'行政处罚', company.decode('utf-8'), PID, 5, int(round(time.time() * 1000)), punish)

                    # 经营异常
                    if COM.has_key('INDBUSINFO'):
                        for indbusinfo in COM['INDBUSINFO']:
                            print('Push one INDBUSINFO event for ' + company)
                            if indbusinfo.has_key('speCause_CN') and indbusinfo.has_key('abntime') and indbusinfo.has_key('decOrg_CN'):
                                mnsClient.sendMsgContent(u'经营异常[列入]', company.decode('utf-8'), PID, 6, indbusinfo['abntime'], {
                                        'speCause_CN': indbusinfo['speCause_CN'],
                                        'abntime': indbusinfo['abntime'],
                                        'decOrg_CN': indbusinfo['decOrg_CN']
                                    })

                            if indbusinfo.has_key('remExcpRes_CN') and len(indbusinfo['remExcpRes_CN']) != 0:
                                mnsClient.sendMsgContent(u'经营异常[移出]', company.decode('utf-8'), PID, 24, indbusinfo['remDate'], {
                                    "speCause_CN": indbusinfo['speCause_CN'],
                                    "abntime": indbusinfo['abntime'],
                                    "decOrg_CN": indbusinfo['decOrg_CN'],
                                    "remExcpRes_CN": indbusinfo['remExcpRes_CN'],
                                    "remDate": indbusinfo['remDate'],
                                    "reDecOrg_CN": indbusinfo['reDecOrg_CN']
                                })
                except Exception as e:
                    print(e)
            company = fp.readline()

    client.close()


if __name__ == '__main__':
    try:
        generate_event('./beitongseed.txt')
    except Exception as e:
        print('Exception: %s' % e)

