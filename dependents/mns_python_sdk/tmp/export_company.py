#!/usr/bin/env python
# coding=utf-8

__author__ = 'Edward Chen (edward_chen@koios.cn)'

from pymongo import MongoClient
import hashlib, sys, datetime, json, time



CONNECTION_STRING_SANDBOX = 'mongodb://rest_enterprise_backend:o6qrFXmKBpNEeQMui82@127.0.0.1:37172/admin'
CONNECTION_STRING_API = 'mongodb://rest_enterprise_backend:o6qrFXmKBpNEeQMui82@127.0.0.1:37171/admin'

# 更新企业信息
def export_by_company():
    client_sandbox = MongoClient(CONNECTION_STRING_SANDBOX)
    client_api = MongoClient(CONNECTION_STRING_API)
    db_sandbox = client_sandbox.enterprise
    db_api = client_api.enterprise

    ENT_COLLECTION_API = db_api['EnterpriseBaseInfo']
    ENT_COLLECTION_SANDBOX = db_sandbox['EnterpriseBaseInfo']

    with open('company.txt', 'rb') as fp:
        company = fp.readline()
        while company:
            company = company.strip(' \r\n')

            if len(company) < 4:
                continue

            COM_API = ENT_COLLECTION_API.find_one({'ENTNAME': company})
            COM_SANDBOX = ENT_COLLECTION_SANDBOX.find_one({'ENTNAME': company})

            if COM_SANDBOX is not None:
                print(company)

            if COM_API is not None and COM_SANDBOX is None:
                ENT_COLLECTION_SANDBOX.insert(COM_API)
                print("Insert one company to sandbox " + company)

            if COM_API is not None:
                ANN_API = db_api['AnnualReport'].find_one({'PID': COM_API['PID']})
                ANN_SANDBOX = db_sandbox['AnnualReport'].find_one({'PID': COM_API['PID']})
                if ANN_API is not None and ANN_SANDBOX is None:
                    db_sandbox['AnnualReport'].insert(ANN_API)
                    print("Insert one annual report to sandbox " + company)

            company = fp.readline()

    client_api.close()
    client_sandbox.close()


if __name__ == '__main__':
    try:
        export_by_company()
    except Exception as e:
        print(e)

