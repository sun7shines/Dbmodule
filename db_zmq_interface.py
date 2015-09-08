# -*- coding: utf-8 -*-

import support.message.message_queue

class DbMessageQueueServer(object):
    
    def __init__(self, ip, port):

        self.server = support.message.message_queue.ZmqMessageQueueServer(ip, port)

    def get(self):
        
        return self.server.get()
    
    def put(self, msg):
        
        return self.server.put(msg)

class DbMessageQueueClient(object):
    
    def __init__(self, ip, port):

        self.socket_client = support.message.message_queue.ZmqMessageQueueClient(ip, port)
        
    def get(self):
        
        return self.socket_client.get(False, 1000)
    
    def put(self, msg):
        
        return self.socket_client.put(msg)
        
    def close(self):
        
        return self.socket_client.close()
        
       
        
