# -*- coding: utf-8 -*-

import syslog
import sys

import dbmodule.db_module_interface


def sys_vmd_log(msg):
    syslog.syslog(syslog.LOG_ERR, "OP ON DB FAILED:") # traceback.extract_stack()
    syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
    
    return True,''

def db_get(table,param):
    
    if not (table ):
        return {}
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg[0]
    
    return {}

def get_id(dst,src_dst,src,src_id,src_field,src_value):
    pass

# ('vm_portgroup',{'pg':name,'vswitch_id':('vswitch','host_vswitch','hosts','host_id','uuid',hostuuid)})

def db_join_get(table,param,dst_id,extra):
    
    (dst,srcdst,src,src_id,src_field,src_value) = extra
    
    srcparam = {}
    srcparam[src_field] = src_value
    srcobj = db_get(src,srcparam)
    
    if not srcobj:
        return {}
    
    jobjs = db_values(srcdst,{src_id:srcobj['id']})
    for jobj in jobjs:
        dstobj = db_get(dst,{'id':jobj[dst_id]})
        if not dstobj:
            continue
        param[dst_id] = dstobj['id'] 
        pgobj = db_get(table,param)
        if pgobj:
            return pgobj
    
    if not pgobj:
        return {}
    

def db_join_get_vc(table,param,dst_id,extra,vcip):
    
    (dst,srcdst,src,src_id,src_field,src_value) = extra
    
    srcparam = {}
    srcparam[src_field] = src_value
    srcobj = db_get_vc(src,srcparam,vcip)
    
    if not srcobj:
        return {}
    
    jobjs = db_values_vc(srcdst,{src_id:srcobj['id']},vcip)
    for jobj in jobjs:
        dstobj = db_get_vc(dst,{'id':jobj[dst_id]},vcip)
        if not dstobj:
            continue
        param[dst_id] = dstobj['id'] 
        pgobj = db_get_vc(table,param,vcip)
        if pgobj:
            return pgobj
    
    if not pgobj:
        return {}
    
    
def db_values(table,param):
    
    if not (table ):
        return []
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg
    
    return []

def db_get_vc(table,param,vcip):
    
    if not (table and vcip):
        return {}
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg[0]
    
    return {}

def db_values_vc(table,param,vcip):
    
    if not (table and  vcip):
        return []
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg
    
    return []

def xattrc(table,obj,fkey,vcip):
    
    if not (obj and table and fkey and vcip):
        return {}
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    field1 = {'id':obj[fkey]}
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg[0]
    
    return {}

def xattr(table,obj,fkey):
    
    if not (obj and table and fkey):
        return {}
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    field1 = {'id':obj[fkey]}
    module_object.message['field1'] = field1
    flag,msg = module_object.select()
    if flag and msg:
        return msg[0]
    
    return {}

def db_delete(table,param):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.delete()
    if not msg:
        msg = ''
    if not flag:
        return (False, "Delete %s failed: %s %s" % (table,str(param),msg) )

    return True,''

def db_delete_vc(table,param,vcip):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    field1 = param
    module_object.message['field1'] = field1
    flag,msg = module_object.delete()
    if not msg:
        msg = ''
    if not flag:
        return (False, "Delete %s failed: %s %s" % (table,str(param),msg) )

    return True,''

def db_save_vc(table,param,vcip):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    field1 = param
    module_object.message["field1"] = field1
    flag,msg=module_object.insert()
    if not msg:
        msg = ''
    if not flag:
        sys_vmd_log(msg)
        return False,'insert db in vc db failed'
    return True,str(msg["id"])

def db_save(table,param):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    field1 = param
    module_object.message["field1"] = field1
    flag,msg=module_object.insert()
    if not msg:
        msg = ''
    if not flag:
        sys_vmd_log(msg)
        return False,'insert db failed' 
    return True,str(msg["id"])

def db_modify(table,param,updateparam):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table)
    module_object.message["field1"] = param
    module_object.message["field2"] = updateparam
    flag,msg = module_object.modify()
    if not msg:
        msg = ''
    if not flag:
        sys_vmd_log(msg)
        return (False, "Update hostname db failed")
    return True,''

def db_modify_vc(table,param,updateparam,vcip):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name=table,ip_d=vcip)
    module_object.message["field1"] = param
    module_object.message["field2"] = updateparam
    flag,msg = module_object.modify()
    if not msg:
        msg = ''
    if not flag:
        sys_vmd_log(msg)
        return (False, "Update hostname db failed")
    return True,''
 
