# -*- coding: utf-8 -*-

import sys
import traceback
import types
import time
import syslog
from Queue import Queue
import datetime

import dbmodule.db_zmq_interface
import new_subthread
import global_params
import support.message.message
import support.lock_option
from db_object.models import *
import support.uuid_op
import system.network.dns_service_op
try:
    import json
except ImportError:
    import simplejson as json


DB_SERVER_PORT = "9164"
DJANGO_MODE = 1
SQLITE = 2

DB_MODE = {}
DB_MODE["Django"] = DJANGO_MODE
DB_MODE["sqlite"] = SQLITE

# MESSAGE_QUEUE = Queue()
DB_MODULE_SERVER = None


def convert_time_to_seconds(msg):

    vm_createtimedes = msg.split(".")[0]
    if '+' in vm_createtimedes:
        vm_createtimedes = vm_createtimedes.split('+')[0]
    ISOTIMEFORMAT='%Y-%m-%d %X'
    vm_createtimesec = time.mktime(time.strptime(vm_createtimedes, ISOTIMEFORMAT))
    return vm_createtimesec

def convert_seconds_to_time(msg):
    timeStamp = int(msg)
    return datetime.datetime.utcfromtimestamp(timeStamp)
    
def update_dict_contain_seconds(info):

    for key, value in info.iteritems():
        if type(value) is types.DictType:
            for k, v in value.iteritems():
                if k.endswith("_*stmap"):
                    timevlaue = convert_seconds_to_time(value[k])
                    value.pop(k)
                    new_key = k.split("_*stmap")[0]
                    value[new_key] = timevlaue
            info.pop(key)
            info[key] = value
                    
        elif key.endswith("_*stmap") :
            if info[key]:
                timevlaue =  convert_seconds_to_time(info[key])
                info.pop(key)
                new_key = key.split("_*stmap")[0]
                info[new_key] = timevlaue
            else:
                info.pop(key)
                info[new_key] = ""

    return info
    

def update_dict_contain_time(info):

    for key, value in info.iteritems():
        if type(value) is types.DictType:
            for k, v in value.iteritems():
                if  k.endswith("_at") or type(v)==type(datetime.datetime(1999,12,12)):
                    value[k] = convert_time_to_seconds(str(value[k]))
        elif key.endswith("_at") or type(value)==type(datetime.datetime(1999,12,12)):
            if info[key]:
                info[key] =  convert_time_to_seconds(str(info[key]))
            else:
                info[key] = ""

    return info

def update_dict_contain_time_flag(info):

    for key, value in info.iteritems():
        if type(value) is types.DictType:
            for k, v in value.iteritems():
                if  k.endswith("_*stmap") and type(v)==type(datetime.datetime(1999,12,12)):
                    value[k] = convert_time_to_seconds(str(value[k]))
        elif key.endswith("_*stmap") and type(value)==type(datetime.datetime(1999,12,12)):
            if info[key]:
                info[key] =  convert_time_to_seconds(str(info[key]))
            else:
                info[key] = ""

    return info

def update_dict_contain_time_tostr(info):

    for key, value in info.iteritems():
        if type(value) is types.DictType:
            for k, v in value.iteritems():
                if  k.endswith("_at"):
                    value[k] = time.strftime("%Y-%m-%d %H:%M:%S+0800", time.localtime(value[k]))
        elif key.endswith("_at"):
            if info[key]:
                info[key] =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info[key]))
            else:
                info[key] = ""

    return info


class HandlerDbFunctionBase(object):

    def __init__(self):
        pass

    def insert(self,db_name,field,value):
        pass

    def delete(self,db_name,field,value):
        pass

    def modify(self,db_name,field_old,value_old,field_new,value_new):
        pass

    def select(self,db_name,field,value):
        pass


def make_filed_group(dic_message):

    value_tmp1 = {}
    for k,v in dic_message.iteritems():
        if k == "foreignkey":
            for node in v:
                name = node["field_name"]
                try:
                    module_name =eval(node["db_name"])
                except:
                    return(False,"")
                field = node["field"]
                try:
                    if len(field) == 0:
                        value_tmp1[name] = None
                    else:
                        ob = module_name.objects.get(**field)
                        value_tmp1[name]=ob
                except:
                    return False,""
        else:
            value_tmp1[k] = v

    return True,value_tmp1    


class HandlerDbFunction_Django(HandlerDbFunctionBase):
    
    def __init__(self):
        pass
        
    def insert(self,db_name,**value):
        
        flag,value_tmp1 = make_filed_group(value)
        if not flag:
            return(flag, "InsertDB:make field group failed")
        try:
            node = eval(db_name)
            m = node(**value_tmp1)
            m.save()
        except:
            syslog.syslog(syslog.LOG_ERR, "InertDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "InertDBError:"+traceback.format_exc())
            return(False, "InsertDB failed")
        return (True, {"id":m.id})

    def update_all(self,db_name,srcMsg):

        changeList = srcMsg["changeList"]
        _N_ID = srcMsg["_N_ID"]
        for fif, masterformid, addFlag in changeList:
            # 可能为update或者insert操作，
            # update操作传递created_at/updated_at时间为秒数时会触发异常。而insert则无此问题。
            # 增加特殊处理
            for timeKey in ["created_at", "updated_at"]:
                if fif.has_key(timeKey):
                    del fif[timeKey]
            try:
                flag,value_tmp1 = make_filed_group(fif)
                node = eval(db_name)
                m = node(**value_tmp1)
                m.save()
                if addFlag:
                    if not _N_ID.get(str(db_name)):
                        _N_ID[str(db_name)] = []
                    _N_ID[str(db_name)].append({str(masterformid):int(m.id)})
            except:
                syslog.syslog(syslog.LOG_ERR, "InertDBError:"+str(fif)+":"+str(sys.exc_info()))
                syslog.syslog(syslog.LOG_ERR, "InertDBError:"+traceback.format_exc())
        return (True, {"_N_ID":_N_ID})
    
    def insert_all(self,db_name,srcMsg):
        
        srcTables = srcMsg["srcTables"]
        srcDict = srcMsg["srcDict"]
        srcArgs = srcMsg["srcArgs"]
        
        for obj in srcTables:
            older_id = obj["id"]
            obj["id"] = None
            for ref_model, attrname in srcArgs:
                attrname = "".join([attrname, "_id"])
                if obj[attrname]:
                    obj[attrname] = int(srcDict[str(ref_model)][str(obj[attrname])])
                else:
                    obj[attrname] = None
            flag,msg = self.insert(db_name,**obj)
            if not flag:
                syslog.syslog(syslog.LOG_ERR, "SYNC VS TO VC ERROR: insert into table %s error" % str(db_name))
                continue
    
            new_id = msg["id"]
            if not srcDict.has_key(str(db_name)):
                srcDict[str(db_name)] = {}
            srcDict[str(db_name)][str(older_id)] = int(new_id)
        
        return (True, {"dstDict":srcDict})
    
    def delete(self,db_name,**value_flag):

        flag,value_tmp1 = make_filed_group(value_flag)
        if not flag:
            return(flag, "DeleteDB:make field group failed")
        try:
            node = eval(db_name)
            if len(value_tmp1) == 0:
                node.objects.all().delete()
            else:
                node.objects.filter(**value_tmp1).delete()
        except:
            syslog.syslog(syslog.LOG_ERR, "DeleteDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "DeleteDBError:"+traceback.format_exc())
            return (False, "DeleteDB failed")

        return (True, "")
    
    def modify(self,db_name,value_flag,**value):

        flag,value_tmp1 = make_filed_group(value_flag)
        if not flag:
            return(flag, "ModifyDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1)
            if not m:
                return False,'no values'
        except:
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
            return(False, "ModifyDB failed")

        flag,value_tmp2 = make_filed_group(value)

        if not flag:
            return(flag, "ModifyDB:make field group failed")

        for nd in m:
            for key in value.keys():
                try:
                    nd.__setattr__(key, value_tmp2[key])
                except:
                    syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
                    syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
                    return(False, "ModifyDB:no attribute")
            try:
                nd.save()
            except:
                syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
                syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
                return (False, "ModifyDB:write db failed")
        return (True, "")
    
    def select(self,db_name,**value_flag):

        flag,value_tmp1 = make_filed_group(value_flag)
        if not flag:
            return(flag, "SelectDB:make field group failed")
        
        msg = []
        try:
            node = eval(db_name)
            if len(value_tmp1) == 0:
                m = node.objects.values()
            else:
                m = node.objects.filter(**value_tmp1).values()
            msg = list(m)
            if not msg:
                # 找某一个数据，没找着，则返回失败。
                return (False, msg)
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectDBError:"+traceback.format_exc())
            return(False, "SelectDBError:please log message")

        return (True, msg)
    
    def select_all(self,db_name,value_flag,foreignkey_list = None):

        flag,value_tmp1 = make_filed_group(value_flag)
        if not flag:
            return(flag, "SelectAllDB:make field group failed")
        
        msg = []
        try:
            node = eval(db_name)
            if len(value_tmp1) == 0:
                m = node.objects.values()
            else:
                m = node.objects.filter(**value_tmp1).values()
            msg = list(m)
            if not msg:
                # 找某一系列数据，无则返回成功，以及空数组。
                return (True, [])
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+traceback.format_exc())
            return(False, "SelectAllDBError:please log message")
        
        for node in msg:
            for node2 in foreignkey_list:
                if node[node2["field_id"]]:
                    #node2["field_id"]的值有可能为None或NULL
                    obj = eval(node2["db_name"])
                    value = obj.objects.values().get(id = node[node2["field_id"]])
                    node[node2["field"]] = value
                else:
                    node[node2["field"]] = None
        return (True, msg)
    
    def select_by_fkey(self,db_name,**keys):
        
        """
        (table_name,{key_name:xxx,fkey_name:{table_name:{keyname1:xxx,keyname2:xxx,fkey_name1:{table_name:{...}}}}})
        eg ("host_partition",{"partition_name"":"xx","hdisk_id":{"host_harddisk":{"harddisk_name":"xxx","host_id":{"hosts":{"uuid":"xxx"}}}}})
        
        
        """
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    if keys[x].get("attrname"):
                        if fkey_id and keys[x].get("attrname"):
                            field[x] = fkey_id.__getattribute__(keys[x].get("attrname"))
                        else:
                            field[x] = fkey_id
                    else:
                        field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
            
        field = {}
        for x in keys:
            if type(keys[x])==type({}): 
                fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                field[x] = fkey_id
            else:
                field[x] = keys[x]
                
        flag,value_tmp1 = make_filed_group(field)
        if not flag:
            return(flag, "SelectAllDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1).values()
            res = list(m)
            return (True,res)
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+traceback.format_exc())
            return(False, "SelectAllDB:make field group failed")
        
    def insert_f(self,db_name,**keys):
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    if fkey_id and keys[x].get("attrname"):
                        field[x] = fkey_id.__getattribute__(keys[x].get("attrname"))
                    else:
                        field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
        field = {}
        for x in keys:
            if type(keys[x])==type({}): 
                fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                if fkey_id and keys[x].get("attrname"):
                    field[x] = fkey_id.values()[keys[x].get("attrname")]
                else:
                    field[x] = fkey_id
            else:
                field[x] = keys[x]
                
        flag,value_tmp1 = make_filed_group(field)
        if not flag:
            return(flag, "SelectAllDB:make field group failed")
        try:
            node = eval(db_name)
            m = node(**value_tmp1)
            m.save()
            return (True,m.id)
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+traceback.format_exc())
            return(False, "SelectAllDB:make field group failed")

    def modify_f(self,db_name,value_flag,**keys):
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
            
        field = {}
        for x in value_flag:
            if type(value_flag[x])==type({}): 
                fkey_id = get_id(value_flag[x]["db_name"],value_flag[x]["field"])
                field[x] = fkey_id
            else:
                field[x] = value_flag[x]
        flag,value_tmp1 = make_filed_group(field)
        
        if not flag:
            return(flag, "ModifyDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1)
        except:
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
            return (False, "ModifyDB failed")
        if not m:
            return (False, "ModifyDB failed")
            
        field2 = {}
        for x in keys:
            if type(keys[x])==type({}): 
                fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                field2[x] = fkey_id
            else:
                field2[x] = keys[x]
                
        flag,value_tmp2 = make_filed_group(field2)
        if not flag:
            return(flag, "SelectAllDB:make field group failed")
        
        for nd in m:
            for key in field2.keys():
                try:
                    nd.__setattr__(key, value_tmp2[key])
                except:
                    syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
                    syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
                    return(False, "ModifyDB:no attribute")
            try:
                nd.save()
                
            except:
                syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
                syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
                return (False, "ModifyDB:write db failed")
        return (True,nd.id)
        """
        try:
            node = eval(db_name)
            m = node(**value_tmp1)
            m.save()
            return (True,m.id)
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+traceback.format_exc())
            return(False, "SelectAllDB:make field group failed")
        """
        
    def multi_insert(self,db_name,**keys):
        """ param:{"name":"switchX","port_num":"n","qos":{"db_name":"qos","field":{"qosid":"2","host":{"db_name":"hosts","field":{"uuid":"XXXXX"}}}}} """
        
        def _insert(db_name,**keys):
            node = eval(db_name)
            field = {}
            for x in keys:
                if type(keys[x])!=type({}):
                    field[x] = keys[x]
                else:
                    flag,msg = _insert(keys[x]["db_name"],keys[x]["field"])
                    if not flag:
                        return (flag,msg)
                    field[x] = msg
            try:
                m=node(**field)
                m.save()
                return (True,m)
            except:
                return (False,"")
        
        node = eval(db_name)
        field = {}
        for x in keys:
            if type(keys[x])!=type({}):
                field[x] = keys[x]
            else:
                flag,msg = _insert(keys[x]["db_name"],keys[x]["field"])
                if not flag:
                    return (flag,msg)
                field[x] = msg
        try:            
            m=node(**field)
            m.save()
            return (True,m.id)
        except:
            return (False,"INSERT FAILD")
        
        
        
    def delete_f(self,db_name,**value_flag):
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
            
        field = {}
        for x in value_flag:
            if type(value_flag[x])==type({}): 
                fkey_id = get_id(value_flag[x]["db_name"],value_flag[x]["field"])
                field[x] = fkey_id
            else:
                field[x] = value_flag[x]
        flag,value_tmp1 = make_filed_group(field)
        
        if not flag:
            return(flag, "ModifyDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1).delete()
        except:
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
            return(False, "ModifyDB failed")
            
        return (True,"")
    
    def select_before_delete(self,db_name,attrname,**value_flag):
        
        
        #can not delete more than one object
        
        
        
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
            
        def get_attr(obj,attrname):
            if '__' not in attrname:
                return eval("obj.%s" % attrname)
            else:
                obj = eval("obj.%s" % attrname[:attrname.index('__')])
                attrname = attrname[attrname.index('__')+2:]
                return get_attr(obj,attrname)
        
        field = {}
        for x in value_flag:
            if type(value_flag[x])==type({}): 
                fkey_id = get_id(value_flag[x]["db_name"],value_flag[x]["field"])
                field[x] = fkey_id
            else:
                field[x] = value_flag[x]
        flag,value_tmp1 = make_filed_group(field)
        
        if not flag:
            return(flag, "ModifyDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1)[0]
            attr = get_attr(m,attrname)
            node.objects.filter(**value_tmp1).delete()
        except:
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "ModifyDBError:"+traceback.format_exc())
            return(False, "ModifyDB failed")
            
        return (True,str(attr))
    
    def get_with_mtinfo(self,db_name,mt_attrname,**keys):
                
        """
        适用于只返回一条记录的查询
        (table_name,{key_name:xxx,fkey_name:{table_name:{keyname1:xxx,keyname2:xxx,fkey_name1:{table_name:{...}}}}})
        eg ("host_partition",{"partition_name"":"xx","hdisk_id":{"host_harddisk":{"harddisk_name":"xxx","host_id":{"hosts":{"uuid":"xxx"}}}}})
        
        返回值：(flag,{"res":{XXXXXX},"mt_attr_value":XXXX})
        
        """
        mt_attr_value = "ERROR"
        res = ""
        def get_id(db_name,keys):
            field = {}
            for x in keys:
                if type(keys[x])==type({}):
                    fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                    if keys[x].get("attrname"):
                        if fkey_id and keys[x].get("attrname"):
                            field[x] = fkey_id.__getattribute__(keys[x].get("attrname"))
                        else:
                            field[x] = fkey_id
                    else:
                        field[x] = fkey_id
                else:
                    field[x] = keys[x]
            tm = {"db_name":db_name,"field":field}
            flag,value_tmp1 = make_filed_group(field)
            if not flag:
                return(flag, "SelectAllDB:make field group failed")
            try:
                node = eval(db_name)
                m = node.objects.filter(**value_tmp1)[0]
                #res = list(m)[0]
                return m
            except:
                pass
            
        def get_attr(obj,attrname):
            if '__' not in attrname:
                return eval("obj.%s" % attrname)
            else:
                obj = eval("obj.%s" % attrname[:attrname.index('__')])
                attrname = attrname[attrname.index('__')+2:]
                return get_attr(obj,attrname)
            
        field = {}
        for x in keys:
            if type(keys[x])==type({}): 
                fkey_id = get_id(keys[x]["db_name"],keys[x]["field"])
                field[x] = fkey_id
            else:
                field[x] = keys[x]
                
            
        flag,value_tmp1 = make_filed_group(field)
        if not flag:
            return(flag, "SelectAllDB:make field group failed")
        try:
            node = eval(db_name)
            m = node.objects.filter(**value_tmp1)
            obj = m[0]
            mt_attr_value = get_attr(obj,mt_attrname)
            r = m.values()
            res = list(r)[0]
            return (True,{"res":res,"mt_attr_value":mt_attr_value})
        except IndexError,e:
            syslog.syslog(syslog.LOG_ERR, "SelectDBError:"+str(e))
            syslog.syslog(syslog.LOG_ERR, "SelectDBError:"+traceback.format_exc())
            return(False, "SelectDB error")
        except:
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+str(sys.exc_info()))
            syslog.syslog(syslog.LOG_ERR, "SelectAllDBError:"+traceback.format_exc())
            return(False, "SelectAllDB:make field group failed")
    
    def select_manytomany(self,table1,table2,intertable=None):
        """table1: 需要返回查询结果的表，table2:查询条件所在表(只返回一行记录)，intertable中间表"""
        try:
            node2 = eval(table2['db_name'])
            obj2 = node2.objects.filter(**table2['field'])
        except:
            pass
        

        
        
        
        


class HandlerDbFunction_Sqlite(HandlerDbFunctionBase):
    
    def __init__(self):
        pass
    
    def insert(self,db_name,field,value):
        pass
    
    def delete(self,db_name,field,value):
        pass
    
    def modify(self,db_name,field_old,value_old,field_new,value_new):
        pass
    
    def select(self,db_name,field,value):
        pass

class DB_INTERFACE(object):
    
    def __init__(self,mode):
        
        if mode == DB_MODE["Django"]:
            self.handler = HandlerDbFunction_Django()
        elif mode == DB_MODE["sqlite"]:
            self.handler = HandlerDbFunction_Sqlite()




class HandlerDbFunction(object):
    
    def __init__(self,mode):
        
        self.db_interface = DB_INTERFACE(mode)
        
def init_db_server(ip,port):

    global DB_MODULE_SERVER
    DB_MODULE_SERVER = dbmodule.db_zmq_interface.DbMessageQueueServer(ip,port)

def create_db_client(ip,port):
    
    return dbmodule.db_zmq_interface.DbMessageQueueClient(ip,port)


def get_info_from_message(message):

    info = message.split("##")
    msg = {}
    msg["ip"] = info[0]
    msg["port"] = info[1]
    msg["operation"] = info[2]
    msg["db_name"] = info[3]
    msg["field1"] = eval(info[4])
    if len(info) > 5:
        msg["field2"] = eval(info[5])
    else:
        msg["field2"] = ""
    return msg


def put_re_message_in_queue(message):

    message = '''"'''.join(message.split("'"))
    msg = json.JSONDecoder().decode(message)
    if "type" not in msg.keys():
        return False
    if msg["type"] == "re":
        support.lock_option.lock_acquire("db_result_lock")
        global_params.DB_RESULT_QUEUE.append(msg)
        support.lock_option.lock_release("db_result_lock")
        return True
    return False
    
def wait_message():
    
    global DB_MODULE_SERVER
    while True:
        try:
            flag, message =  DB_MODULE_SERVER.get()
            DB_MODULE_SERVER.put("recv successful")
            
            message = '''"'''.join(message.split("'"))            
            msg = json.JSONDecoder().decode(message)
            
            msg = update_dict_contain_seconds(msg)
            new_subthread.addtosubthread_Fast("run_require_ruturn_result", run_require_ruturn_result, msg)
            
#             MESSAGE_QUEUE.put(msg)            
        except:
            syslog.syslog(syslog.LOG_ERR, "DB_MODULE_SERVER get failed")
        
def db_module_handler(msg):

    global db_module

    msg_return = {}
    flag = True
    info = None
    
    for x in msg:
        if type(msg[x])==type({}):
            msg[x] = update_dict_contain_time_tostr(msg[x])
    
    
    if msg["operation"] == "insert":
        flag,info = db_module.db_interface.handler.insert(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "select":
        flag,info = db_module.db_interface.handler.select(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "delete":
        flag,info = db_module.db_interface.handler.delete(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "modify":
        flag,info = db_module.db_interface.handler.modify(msg["db_name"],msg["field1"],**msg["field2"])
    elif msg["operation"] == "select_all":
        flag,info = db_module.db_interface.handler.select_all(msg["db_name"],msg["field1"],msg["foreignkey_list"])
    elif msg["operation"] == "select_by_fkey":
        flag,info = db_module.db_interface.handler.select_by_fkey(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "insert_f":
        flag,info = db_module.db_interface.handler.insert_f(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "modify_f":
        flag,info = db_module.db_interface.handler.modify_f(msg["db_name"],msg["field1"],**msg["field2"])
    elif msg["operation"] == "delete_f":
        flag,info = db_module.db_interface.handler.delete_f(msg["db_name"],**msg["field1"])
    elif msg["operation"] == "select_before_delete":
        flag,info = db_module.db_interface.handler.select_before_delete(msg["db_name"],msg["attrname"],**msg["field1"])
    elif msg["operation"] == "get_with_mtinfo":
        flag,info = db_module.db_interface.handler.get_with_mtinfo(msg["db_name"],msg["mt_attrname"],**msg["field1"])
    elif msg["operation"] == "multi_insert":
        flag,info = db_module.db_interface.handler.multi_insert(msg["db_name"],**msg["field1"])
        
    elif msg["operation"] == "insert_all":
        flag,info = db_module.db_interface.handler.insert_all(msg["db_name"],msg)
    
    elif msg["operation"] == "update_all":
        flag,info = db_module.db_interface.handler.update_all(msg["db_name"],msg)
   
    if not flag:
        msg_return["msg_uuid"] = msg["uuid"]
        msg_return["result"] = flag
        msg_return["db_content"] = info
        return(flag,msg_return)
    if type(info) is types.DictType:
        info = update_dict_contain_time(info)
    elif type(info) is types.ListType:
        for node in info:
            node = update_dict_contain_time(node)

    msg_return["db_content"] = info

    msg_return["msg_uuid"] = msg["uuid"]
    msg_return["result"] = flag
    return (flag,msg_return)


def run_require_ruturn_result(msg):

    try:
        (flag, message) = db_module_handler(msg)
        send_db_re_message(msg,message)
    except:
        syslog.syslog(syslog.LOG_ERR, "DB HANDEL ERROR:" + str(msg))
        syslog.syslog(syslog.LOG_ERR, "DB HANDEL ERROR:"+str(sys.exc_info()))
        syslog.syslog(syslog.LOG_ERR, "DB HANDEL ERROR:"+traceback.format_exc())

# def handler_message():
#     
#     # 实际测试100次查询访问时间，使用如下串行方式，反而比使用线程的并行方式速度还要块，
#     # 所以，这里不做线程化并行优化。
#     
#     # 使用快速开启线程的方式，运行数据库请求，测试性能提升？是否会造成进程崩溃？
#     while True:
#         node = MESSAGE_QUEUE.get()
#         new_subthread.addtosubthread_Fast("run_require_ruturn_result", run_require_ruturn_result, node)

def send_db_re_message(node,message):

    zmq_client = create_db_client(node["ip"],node["port"])
    msg = {}
    msg["type"] = "re"
    msg["content"] = message
    info = support.message.message._dict_to_message(msg)    
    zmq_client.put(info)
    zmq_client.get()
    zmq_client.close()

def send_db_commend_message(ip,port,msg):
    
    zmq_client = create_db_client(ip,port)
    msg = update_dict_contain_time_flag(msg) ##检查特定的时间标记转化为时间戳
    message = support.message.message._dict_to_message(msg)
    try:
        zmq_client.put(message)
        zmq_client.get()
    except:
        return -1
    
def get_db_commend_response(uuid,timeout):

    while timeout > 0:
        support.lock_option.lock_acquire("db_result_lock")
        for node in global_params.DB_RESULT_QUEUE:
            if node["content"]["msg_uuid"] == uuid:
                flag = node["content"]["result"]
                content = node["content"]["db_content"]
                global_params.DB_RESULT_QUEUE.remove(node)
                support.lock_option.lock_release("db_result_lock")
                return (flag, content)
        support.lock_option.lock_release("db_result_lock")
        timeout = timeout - 0.05
        time.sleep(0.05)
    return False, "Get DB Response Timeout"

def handler_db(ip,port,msg,timeout):
    
    if ip == "127.0.0.1":
        # 请求对象为127.0.0.1，则请求回复的IP也为"127.0.0.1"
        msg["ip"] = "127.0.0.1"
    else:
        # 请求对象为远端主机，则请求回复的IP为主机的对外IP
        # 实时读取，可以避免问题的出现，若用缓存可能存在更新异常或时间差等问题
        host_uuid = support.uuid_op.get_vs_uuid()[1]
        msg["ip"] = system.network.dns_service_op.get_host_uuid_ip(host_uuid)
        global_params.LOCAL_IP = msg["ip"]

    ret = send_db_commend_message(ip,port,msg)
    if -1 == ret:
        syslog.syslog(syslog.LOG_ERR, "Send DB mesage failed:TargetIP:" + ip + ":" + str(msg))
        return (False, "Send DB message failed.")
    
    flag,message = get_db_commend_response(msg["uuid"],timeout)
    if not flag and msg.get("operation") not in ["select",]:
        syslog.syslog(syslog.LOG_ERR, "Get DB message result failed:TargetIP:" + ip + ":" + str(msg))
    return (flag, message)

def create_foreignkey_field(db_name,field,field_name):
    
    foreignkey = []
    length = len(db_name)
    for i in range(0,length):
        msg = {}
        msg["db_name"] = db_name[i]
        msg["field"] = field[i]
        msg["field_name"] = field_name[i]
        foreignkey.append(msg)
    return foreignkey

class DbMessageObject(object):
    
    def __init__(self,db_name,operation = "select",ip_d="127.0.0.1",port_d= DB_SERVER_PORT,ip_s=global_params.LOCAL_IP,port_s=global_params.DB_PORT):
        
        self.message = {}
        self.message["uuid"] = support.uuid_op.get_uuid()
        self.message["operation"] = operation
        if not ip_s:
            self.message["ip"] = global_params.LOCAL_IP
        else:
            self.message["ip"] = ip_s
        if not port_s:
            self.message["port"] = global_params.DB_PORT
        else:
            self.message["port"] = port_s
        self.message["db_name"] = db_name
        self.ip_d = ip_d
        self.port_d = port_d
        
    def insert_flag_field(self,field):
        
        self.message["field1"] = field
        
    def insert_change_field(self,field):
        
        self.message["field2"] = field

    def change_operation(self,operation):
        
        self.message["operation"] = operation
        
    def change_des_net(self,ip_d, port_d = DB_SERVER_PORT):
        
        self.ip_d = ip_d
        self.port_d = port_d
        
    def change_db_name(self,db_name):
        
        self.message["db_name"] = db_name
        
    def send_msg_to_db_server(self):
        
        flag,msg = handler_db(self.ip_d,self.port_d,self.message,60)
        return flag,msg
    
    def select(self):
        
        self.message["operation"] = "select"
        return self.send_msg_to_db_server()
    
    def delete(self):
        
        self.message["operation"] = "delete"
        return self.send_msg_to_db_server()
    
    def modify(self):
        
        self.message["operation"] = "modify"
        return self.send_msg_to_db_server()
    
    def insert(self):
        
        self.message["operation"] = "insert"
        return self.send_msg_to_db_server()
    
    def insert_all(self):
        
        self.message["operation"] = "insert_all"
        return self.send_msg_to_db_server()
    
    def update_all(self):
        
        self.message["operation"] = "update_all"
        return self.send_msg_to_db_server()

    def select_all(self):
        
        self.message["operation"] = "select_all"
        return self.send_msg_to_db_server()
    def select_by_fkey(self):
        self.message["operation"] = "select_by_fkey"
        return self.send_msg_to_db_server()
    
    def insert_f(self):
        self.message["operation"] = "insert_f"
        return self.send_msg_to_db_server()
    
    def modify_f(self):
        
        self.message["operation"] = "modify_f"
        return self.send_msg_to_db_server()
    
    def delete_f(self):
        
        self.message["operation"] = "delete_f"
        return self.send_msg_to_db_server()
    
    def select_before_delete(self):
        
        self.message["operation"] = "select_before_delete"
        return self.send_msg_to_db_server()
    
    def get_with_mtinfo(self):
        
        self.message["operation"] = "get_with_mtinfo"
        return self.send_msg_to_db_server()
    def multi_insert(self):
        
        self.message["operation"] = "multi_insert"
        return self.send_msg_to_db_server()
    
    
class DbObject(object):
    
    def __init__(self,db_name="",operation = "select",ip_d="127.0.0.1",port_d= DB_SERVER_PORT,ip_s=global_params.LOCAL_IP,port_s=global_params.DB_PORT):
        
        self.message = {}
        self.message["uuid"] = support.uuid_op.get_uuid()
        self.message["operation"] = operation
        if not ip_s:
            self.message["ip"] = global_params.LOCAL_IP
        else:
            self.message["ip"] = ip_s
        if not port_s:
            self.message["port"] = global_params.DB_PORT
        else:
            self.message["port"] = port_s
        self.message["db_name"] = db_name
        self.ip_d = ip_d
        self.port_d = port_d
        
    def insert_change_field(self,**field):
        
        self.message["field2"] = field
        
    def add_foreignkey_info(self,info):
        
        self.message["foreignkey_list"] = info
        
    def send_msg_to_db_server(self):
        
        flag,msg = handler_db(self.ip_d,self.port_d,self.message,60)
        return flag,msg
    
    def select(self,hostip="127.0.0.1",**field):
        
        self.ip_d = hostip
        self.message["field1"] = field
        self.message["operation"] = "select"
        return self.send_msg_to_db_server()
    
    def delete(self,hostip="127.0.0.1",**field):
        
        self.message["operation"] = "delete"
        self.ip_d = hostip
        self.message["field1"] = field
        return self.send_msg_to_db_server()
    
    def modify(self,hostip="127.0.0.1",**field):
        
        self.message["operation"] = "modify"
        self.ip_d = hostip
        self.message["field1"] = field
        return self.send_msg_to_db_server()
    
    def insert(self,hostip="127.0.0.1",**field):
        
        self.message["operation"] = "insert"
        self.ip_d = hostip
        self.message["field1"] = field
        return self.send_msg_to_db_server()
    
    def select_all(self,hostip="127.0.0.1",**field):
        
        self.message["operation"] = "select_all"
        self.ip_d = hostip
        self.message["field1"] = field
        return self.send_msg_to_db_server()
    
class VmsCfg(DbObject):
    
    def __init__(self):
        
        DbObject.__init__(self,db_name="vms")
        
class ClustersCfg(DbObject):
    
    def __init__(self):
        
        DbObject.__init__(self,db_name="clusters")
    
def operation_db_interface(ip_s,port_s,ip_d,port_d,db_name,operation,field1,field2,timeout):
    
    message = {}
    message["ip"] = ip_s
    message["port"] = port_s
    message["uuid"] = "1234"
    message["type"] = "co"
    message["operation"] = operation
    message["field1"] = field1
    message["field2"] = field2
    flag,message = handler_db(ip_d,port_d,message,60)
    return flag,message

def init_db():
    
    global db_module
    db_module = HandlerDbFunction(DB_MODE["Django"])

    init_db_server("*", DB_SERVER_PORT)
    
    wait_message()
#     
#     new_subthread.addtosubthread("db_wait_message", wait_message)
  
    
