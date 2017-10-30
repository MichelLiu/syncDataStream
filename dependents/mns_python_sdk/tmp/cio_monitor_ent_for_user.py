#!/usr/bin/env python
# coding=utf-8

__author__ = 'Edward Chen (edward_chen@koios.cn)'

from pymongo import MongoClient
import hashlib, sys, datetime, json, time



CONNECTION_STRING_SANDBOX = 'mongodb://root:8tgEuNF2rRhiDY@127.0.0.1:37172/admin'
# CONNECTION_STRING_API = 'mongodb://rest_enterprise_backend:o6qrFXmKBpNEeQMui82@127.0.0.1:37171/admin'

# 更新企业信息
def monitor_ent_to_user(mobile, group_name, company_file):
    client = MongoClient(CONNECTION_STRING_SANDBOX)

    # find user
    USER = client.CIO_User['users'].find_one({'mobile':mobile})
    if USER is None:
        print("Can't find user for mobile [" + mobile + ']')
        return
    GROUP = client.CIO_User['groups'].find_one({'user_id': USER['_id'], 'title': group_name})
    if GROUP is None:
        print("Can't find group for group [" + group_name +']')
        return

    print(USER['username'], GROUP['title'])

    with open(company_file, 'rb') as fp:
        company = fp.readline()
        while company:
            company = company.strip(' \r\n')

            record = client.CIO_User['collection_monitors'].find_one({'entname': company, 'user_id': USER['_id']})
            if record:
                company = fp.readline()
                continue

            TEMP = {
                "type" : 1,
                "status" : 1,
                "begin_expiry_date" : datetime.datetime.now(),
                "end_expiry_date" : datetime.datetime(2020, 1, 1, 0, 0, 0),
                "created_date" : datetime.datetime.now(),
                "modified_date" : datetime.datetime.now(),
                "timestamp" : datetime.datetime.now()
            }
            TEMP['user_id'] = USER['_id']
            TEMP['group_id'] = GROUP['_id']
            TEMP['group_name'] = GROUP['title']

            COM = client.enterprise['EnterpriseBaseInfo'].find_one({'ENTNAME': company})

            TEMP['pid'] = COM['PID']
            TEMP['entname'] = COM['ENTNAME']
            TEMP['created_by'] = '%s' % USER['_id']
            TEMP['modified_by'] = '%s' % USER['_id']

            client.CIO_User['collection_monitors'].insert(TEMP)

            TEMP2 = TEMP.copy()
            TEMP2.pop('_id')
            TEMP2['type'] = 2
            TEMP2['status'] = 4
            client.CIO_User['collection_monitors'].insert(TEMP2)

            company = fp.readline()

    client.close()


if __name__ == '__main__':
    try:
        monitor_ent_to_user('13917819007', '默认分组', './beitongseed.txt')
        # monitor_ent_to_user('18918029397', '默认分组', './beitongseed.txt')
        monitor_ent_to_user('18221543526', '默认分组', './beitongseed.txt')
    except Exception as e:
        print(e)

