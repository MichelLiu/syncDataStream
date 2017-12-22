import sys
from mns.account import Account
from mns.queue import *
from mns.topic import *
from mns.subscription import *
from bson import json_util
from lib.opensearchSDK import V3Api

class mnsManage:
    def __init__(self,envType,queueName):
        my_account = Account(self.endpoint, self.accessKeyId, self.accessKeySecret, '')
        self.queue = my_account.get_queue(queueName)

    def handle_func(self,msg):
        pass

    def consumeMsg(self,batch_size):
        # try:
        #     msgList = self.queue.batch_receive_message(batch_size)
        #     for index,msg in enumerate(msgList):
        #         msg = json_util.loads(msg.message_body)
        #         if self.handle_func(msg):
        #             self.queue.delete_message(msg.receipt_handle)
        # except:
        #     return False
        msgList = self.queue.batch_receive_message(batch_size)
        print msgList
        for index, msg in enumerate(msgList):
            msg = json_util.loads(msg.message_body)
            if self.handle_func(msg):
                self.queue.delete_message(msg.receipt_handle)

