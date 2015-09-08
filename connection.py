#-*-coding:utf-8-*-

import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
# sys.path.append("/usr/vmd")

import MySQLdb
import threading
import global_params
import time
import syslog

class Connection(object):
    def __init__(self,ip='127.0.0.1',user='fronware',passwd='j3e3z5r9a0',db='fronware'):
        self.connection = MySQLdb.connect(db='fronware',host=ip,user=user,passwd=passwd)
        self.status = 'avaliable'
        self.lock = threading.Lock()
        self.last_use = time.time()
    def __del__(self):
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.connection.close()
        except:
            pass
    def close(self):
        if 'cursor' in self.__dict__:
            self.cursor.close()
            del self.cursor
        self.status = 'avaliable'
        self.last_use = time.time()
        global_params.locklist['connection_lock'].acquire()
        
        if time.time() - global_params.last_check>120:
            for key,value in global_params.connections.items():
                connectiondel = []
                for x in value:
                    if x.status=='unavaliable' or x==self:
                        continue
                    elif time.time() - x.last_use  > 60:
                        connectiondel.append(x)
                delc = len(connectiondel)
                for x in connectiondel:
                    global_params.connections[key].remove(x)
                    del x
                syslog.syslog(syslog.LOG_ERR,'CLEAN CONNECTIONS: %s connections to %s has been cleaned' % (str(delc),key))
                syslog.syslog(syslog.LOG_ERR,'CLEAN CONNECTIONS: %s connections was left in connection pool:%s' % (str(len(global_params.connections[key])),key))
                if not len(global_params.connections[key]):
                    del global_params.connections[key]
                    syslog.syslog(syslog.LOG_ERR,'CLEAN CONNECTIONS: connection pool:%s was deleted,because has not been used for a long time' % key)
        global_params.last_check = time.time()
        global_params.locklist["connection_lock"].release()
        
    def Cursor(self):
        if 'cursor' in self.__dict__:
            return self.cursor
        else:
            self.cursor = self.connection.cursor()
            return self.cursor
        
    def check(self):
        try:
            self.connection.ping()
        except:
            self.connection.ping(True)
            #return False
        return True
    def connect(self):
        self.status = 'unavaliable'
    def select(self,sqlcmd,attrs={}):
        pass
#         self.lock.acquire()
#         
#         self.lock.release()

    def insert(self,table,attrs,values):
        
        names = ','.join(['`' + x + '`' for x in attrs])
        vl = []
        for l in values:
            vs ='(' + ','.join(["'" + str(x) + "'" for x in l]) + ')'
            vl.append(vs)
        vlstr = ','.join(vl)
        
        sql = 'insert into `%s` (%s) values %s' % (table,names,vlstr)
        
        self.lock.acquire()
        try:
            if 'cursor' not in self.__dict__:
                self.cursor = self.connection.cursor()
            self.cursor.execute(sql)
            self.connection.commit()
            flag = True
        except:
            flag = False
        self.lock.release()
        return flag

    def update(self):
        pass
        

def get_connection(ip='127.0.0.1'):
    global_params.locklist['connection_lock'].acquire()
    conn = None
    
    if not global_params.connections.get(ip):
        global_params.connections[ip] = []
    
    for x in global_params.connections[ip]:
        if x.status=='avaliable':
            conn = x
            x.status = 'unavaliable'
            break
    if len(global_params.connections[ip])<global_params.MAX_LEN and not conn:
        conn = Connection(ip)
        conn.status = 'unavaliable'
        global_params.connections[ip].append(conn)
    elif not conn:
        for _ in range(120):
            for x in global_params.connections[ip]:
                if x.status=='avaliable':
                    x.check()        # 恢复断开的连接
                    x.status = 'unavaliable'
                    #x.last_use = time.time()
                    conn = x
                    
                    break
            time.sleep(0.5)
    
    global_params.locklist["connection_lock"].release()
    return conn

# def get_table_attrs(conn,table):
#     pass
# 
# 
# def test():
#     conn = get_connection(ip='127.0.0.1')
#     cursor = conn.Cursor()
#     cursor.execute('select * from hosts')
#     res = cursor.fetchall()
#     print str(res)
#     time.sleep(10)
#     conn.close()
# 
# def test2():
#     conn = get_connection(ip='127.0.0.1')
#     cursor = conn.Cursor()
#     cursor.execute('select * from hosts')
#     res = cursor.fetchall()
#     print str(res)
#     time.sleep(40)
#     conn.close()
# 
# if __name__=='__main__':
#     global_params.init_threadlock()
#     conn = get_connection(ip='127.0.0.1')
#     cursor = conn.Cursor()
#     cursor.execute('select * from hosts')
#     res = cursor.fetchall()
#     conn.close()
#     print str(res)
#     threads = []
#     for x in range(15):
#         threadx = threading.Thread(target=test)
#         threads.append(threadx)
#     threadx = threading.Thread(target=test2)
#     for x in threads:
#         x.start()
#         time.sleep(1)
#     for x in threads:
#         x.run()
#     

#         
