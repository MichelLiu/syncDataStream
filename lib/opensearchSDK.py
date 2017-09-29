# -- coding: utf-8 --
import ConfigParser
import md5,time,random,hmac,base64, copy
import urllib
from hashlib import sha1
import httplib
import hashlib

import sys
from bson import json_util
from pymongo import MongoClient

class V3Api:
    def __init__(self,cfg, envType):
        self.VERB = "POST"
        self.URI_PREFIX = '/v3/openapi/apps/'
        self.OS_PREFIX = 'OPENSEARCH'
        confkey = envType + "-Base"
        Opkey = envType + "-Optional"
        required_ops = [("Sandbox-Base", "AccessKeyId"), ("Sandbox-Base", "AccessKeySecret"),("Sandbox-Base","AppName"),
                        ("Sandbox-Base", "Endpoint"), ("Sandbox-Base", "MongoDB"),
                        ("Online-Base", "AccessKeyId"),("Online-Base", "AccessKeySecret"), ("Online-Base","AppName"),
                        ("Online-Base", "Endpoint"), ("Online-Base", "MongoDB")]

        parser = ConfigParser.ConfigParser()
        parser.read(cfg)
        for sec, op in required_ops:
            if not parser.has_option(sec, op):
                sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec, op, cfg))
                sys.stderr.write("Read README to get help inforamtion.\n")
                sys.exit(1)

        self.app_name = parser.get(confkey, "AppName")
        self.mongoConnection = parser.get(confkey, "MongoDB")
        self.mongoClient = MongoClient(self.mongoConnection)
        self.address = parser.get(confkey, "Endpoint")
        self.port = 80
        self.AccessKeyID = parser.get(confkey,"AccessKeyId")
        self.AccessKeySecret = parser.get(confkey, "AccessKeySecret")

    def runPost(self,
                 table_name = None,
                 body_json = None,
                 http_header = {},
                 http_params = {}):
        query, header = self.buildQuery(app_name = self.app_name,
                                        table_name=table_name,
                                        access_key = self.AccessKeyID,
                                        secret = self.AccessKeySecret,
                                        body_json=body_json,
                                        http_header = http_header,
                                        http_params = http_params)

        conn = httplib.HTTPConnection(self.address, self.port)
        conn.request(self.VERB, url = query, body = body_json.encode('utf-8'), headers = header)
        response = conn.getresponse()

        return response.status, response.getheaders(), response.read()

    def buildQuery(self,
                   app_name = None,
                   table_name = None,
                   access_key = None,
                   secret = None,
                   body_json=None,
                   http_header = {},
                   http_params = {}):
        uri = self.URI_PREFIX
        if app_name is not None:
            uri += app_name
        uri += '/{TABLE_NAME}/actions/bulk'.format(TABLE_NAME=table_name)

        request_header = self.buildRequestHeader(uri = uri,
                                                 body_json = body_json,
                                                 access_key = access_key,
                                                 secret = secret,
                                                 http_params = http_params,
                                                 http_header = http_header)

        return uri , request_header

    def buildAuthorization(self, uri, body_json,access_key, secret, http_params, request_header):
        canonicalized = self.VERB + '\n'\
                      + self.__getHeader(request_header, 'Content-MD5', hashlib.md5(body_json.encode('utf-8')).hexdigest()) + '\n' \
                      + self.__getHeader(request_header, 'Content-Type', '') + '\n' \
                      + self.__getHeader(request_header, 'Date', '') + '\n' \
                      + self.__canonicalizedHeaders(request_header) \
                      + self.__canonicalizedResource(uri, http_params)

        h = hmac.new(secret, canonicalized, sha1)
        signature = base64.encodestring(h.digest()).strip()
        return '%s %s%s%s' %(self.OS_PREFIX, access_key, ':', signature)

    def __getHeader(self, header, key, default_value = None):
        if key in header and header[key] is not None:
            return header[key]
        return default_value

    def __canonicalizedHeaders(self, request_header):
        header = {}
        for key, value in request_header.iteritems():
            if key is None or value is None:
                continue
            k = key.strip(' \t')
            v = value.strip(' \t')
            if k.startswith('X-Opensearch-') and len(v) > 0:
                header[k] = v

        if len(header) == 0:
            return ''

        sorted_header = sorted(header.items(), key=lambda header: header[0])
        canonicalized = ''
        for (key, value) in sorted_header:
            canonicalized += (key.lower() + ':' + value + '\n')

        return canonicalized

    def __canonicalizedResource(self, uri, http_params):
        canonicalized = urllib.quote(uri).replace('%2F', '/')

        sorted_params = sorted(http_params.items(), key = lambda http_params : http_params[0])
        params = []
        for (key, value) in sorted_params:
            if value is None or len(value) == 0:
                continue

            params.append(urllib.quote(key) + '=' + urllib.quote(value))

        return canonicalized  + '&'.join(params)

    def generateDate(self, format = "%Y-%m-%dT%H:%M:%SZ", timestamp = None):
        if timestamp is None:
            return time.strftime(format, time.gmtime())
        else:
            return time.strftime(format, timestamp)

    def generateNonce(self):
        return str(int(time.time()*100)) + str(random.randint(1000, 9999))

    def buildRequestHeader(self, uri, body_json,access_key, secret, http_params, http_header):
        request_header = copy.deepcopy(http_header)
        if 'Content-MD5' not in request_header:
            request_header['Content-MD5'] = hashlib.md5(body_json.encode('utf-8')).hexdigest()
        if 'Content-Type' not in request_header:
            request_header['Content-Type'] = 'application/json'
        if 'Date' not in request_header:
            request_header['Date'] = self.generateDate()
        if 'X-Opensearch-Nonce' not in request_header:
            request_header['X-Opensearch-Nonce'] = self.generateNonce()

        if 'Authorization' not in request_header:
            request_header['Authorization'] = self.buildAuthorization(uri,
                                                                      body_json,
                                                                      access_key,
                                                                      secret,
                                                                      http_params,
                                                                      request_header)
        key_del = []
        for key, value in request_header.iteritems():
            if value is None:
                key_del.append(key)

        for key in key_del:
            del request_header[key]

        return request_header

if __name__ == '__main__':
    accesskey_id = '鏇挎崲涓篈ccessKeyId'
    accesskey_secret = '鏇挎崲涓簊ecret'
    # 涓嬮潰host鍦板潃锛屾浛鎹负璁块棶瀵瑰簲搴旂敤api鍦板潃锛屼緥濡傚崕涓�1鍖�
    internet_host = 'opensearch-cn-hangzhou.aliyuncs.com'
    appname = '鏇挎崲涓哄簲鐢ㄥ悕'

    api = V3Api(address = internet_host, port = '80')

    print api.runPost(app_name = appname, access_key=accesskey_id, secret=accesskey_secret, http_params={}, http_header={})