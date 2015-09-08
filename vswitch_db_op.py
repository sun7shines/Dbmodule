# -*- coding: utf-8 -*-
"""
# Copyright (c) 2011, www.fronware.com
# All rights reserved.
#
# Filename: vswitch_db_op.py
# Note: 虚拟交换机相关的数据库操作
# Author: chenjianfei
# Modify time: 2011-11-29
#
#
""" 
import syslog                                       
import support.uuid_op
import operation.vhost.sync_db_op
import global_params

import dbmodule.db_op

def get_default_service_console_ip(dfvsw):
    
    try:
        vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':dfvsw})
        if not vswitch_obj or not vswitch_obj.get('id'):
            return []
        svinfo = dbmodule.db_op.db_get('service_console',{'vswitch_id':vswitch_obj.get("id")})
        return (True, svinfo)
    except:
        pass
    return (False, "")
    
            

def insert_dvswitch_bymission(vsname,datacenter_uuid):
    
    vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':vsname})
    if vswitch_obj and vswitch_obj.get('id'):
        datacenter_obj = dbmodule.db_op.db_get('datacenters',{'uuid':datacenter_uuid})
        if datacenter_obj and datacenter_obj.get('id'):
            insertparam = {'datacenter_id':datacenter_obj.get("id"),"vswitch_id":vswitch_obj.get("id")}
            dbmodule.db_op.db_save('datacenter_vswitch',insertparam)      
    
        
    
def delete_vswitch(name, datacenter_uuid = None, vs_type = "vswitch",vsid=None):
    
    """
      删除虚拟交换机    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    if vsid:
        swobjs = dbmodule.db_op.db_values('vswitch',{'id':vsid})
    elif datacenter_uuid:
        if global_params.vserverflag:
            swobjs = dbmodule.db_op.db_values('vswitch',{'name':name})
        else:
            swobjs = dbmodule.db_op.db_values('vswitch',{'name':name,"datacenters__uuid":datacenter_uuid}) 
            
    else:
        swobjs = dbmodule.db_op.db_values('vswitch',{'name':name})
         
    if not swobjs or not swobjs[0].get('id'):
        syslog.syslog(syslog.LOG_ERR,'get vswitch info failed')
        return
    
    for vswitch_obj in swobjs:
        
        for netcard_obj in dbmodule.db_op.db_values('netcard',{'vswitch_id':vswitch_obj.get("id")}):
            update_param = {"vswitch":None,"bond_seq":None}
            dbmodule.db_op.db_modify('netcard',{'id':netcard_obj.get("id")},update_param)
            netcard_obj = dbmodule.db_op.db_get('netcard',{'id':netcard_obj.get("id")})
            if netcard_obj and netcard_obj.get('id'):
                operation.vhost.sync_db_op.update_to_vcenter_ref_host("netcard", netcard_obj, 
                                                                      vsuuid,{"name":netcard_obj.get("name")})
        for portgroup_obj in dbmodule.db_op.db_values('vm_portgroup',{'vswitch_id':vswitch_obj.get("id")}):
            if portgroup_obj.get("qos_id"):
                try:
                    netcard_vmsobjs = dbmodule.db_op.db_values('netcard_vms',{'vm_portgroup_id':portgroup_obj.get("id")})
                    if netcard_vmsobjs:
                        for netcard_vmsobj in netcard_vmsobjs:
                            dbmodule.db_op.db_modify('netcard_vms',{'id':netcard_vmsobj.get("id")},{"vm_portgroup":None})
                except:
                    pass
                if portgroup_obj and portgroup_obj.get('qos_id'):
                    dbmodule.db_op.db_delete('qos',{'id':portgroup_obj.get("qos_id")})
                dbmodule.db_op.db_delete('vm_portgroup',{'id':portgroup_obj.get("id")})
            if vs_type == "vswitch":
                operation.vhost.sync_db_op.delete_from_vcenter("qos", {"host__uuid":vsuuid,
                                                             "vm_portgroup__name":name})
        dbmodule.db_op.db_delete('vswitch',{'id':vswitch_obj.get("id")})
        
        ##调整顺序，根据表关联删除qos会删除关联vswitch
        if vswitch_obj and vswitch_obj.get('qos_id'):
            dbmodule.db_op.db_delete('qos',{'id':vswitch_obj.get("qos_id")})
   
    if vs_type == "vswitch":
        operation.vhost.sync_db_op.delete_from_vcenter("qos", {"host__uuid":vsuuid,
                                                         "vswitch__name":name})
    else:
        operation.vhost.sync_db_op.delete_from_vcenter_ref_host("service_console", vsuuid, 
                                                                    {"vswitch__name":name})
        operation.vhost.sync_db_op.delete_from_vcenter_ref_host("host_vswitch", vsuuid, 
                                                                    {"vswitch__name":name})

def delete_vswitch_vc(name):
    
    """
      删除虚拟交换机    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    is_vcuuid,vcuuid,vcip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid != "127.0.0.1":
        vswitch_obj = dbmodule.db_op.db_get_vc('vswitch',{'hosts__uuid':vsuuid,"name":name},vcip)
        if not vswitch_obj:
            return
        if vswitch_obj and vswitch_obj.get('qos_id'):
            dbmodule.db_op.db_delete('qos',{'id':vswitch_obj.get("qos_id")})
            
        for portgroup_obj in dbmodule.db_op.db_values_vc('vm_portgroup',{'vswitch_id':vswitch_obj.get("id")},vcip):
            if portgroup_obj.get("qos_id"):
                try:
                    netcard_vmsobjs = dbmodule.db_op.db_values_vc('netcard_vms',{'vm_portgroup_id':portgroup_obj.get("id")},vcip)
                    if netcard_vmsobjs:
                        for netcard_vmsobj in netcard_vmsobjs:
                            dbmodule.db_op.db_modify_vc('netcard_vms',{'id':netcard_vmsobj.get("id")},{"vm_portgroup":None},vcip)
                except:
                    pass
                
                if portgroup_obj and portgroup_obj.get('qos_id'):
                    dbmodule.db_op.db_delete('qos',{'id':portgroup_obj.get("qos_id")})
                dbmodule.db_op.db_delete_vc('vm_portgroup',{'id':portgroup_obj.get("id")},vcip)
            
        dbmodule.db_op.db_delete_vc('vswitch',{'id':vswitch_obj.get("id")},vcip)
       
        
    
def update_vswitch(name, bond_mode = None, qos_state = None, 
                   min_bandwidth = None, max_bandwidth = None, burst = None, 
                   vs_type = "vswitch", datacenter_uuid = None):
    
    """
      修改虚拟交换机
    """       
    _, vsuuid = support.uuid_op.get_vs_uuid()
    if global_params.vserverflag:
        getparam = {'name':name}

    else:
        getparam = {'name':name,"datacenters__uuid":datacenter_uuid}

    updateparam = {}
    vswitch_obj = dbmodule.db_op.db_get('vswitch',getparam)
    if not vswitch_obj or not vswitch_obj.get('id'):
        syslog.syslog(syslog.LOG_ERR,'update vswitch failed: get vswitch info failed')
        return
    if qos_state:
        updateparam["qos_state"] = qos_state

    if bond_mode:
        updateparam["bond_mode"] = bond_mode

    if qos_state == "enable":
        qos_updateparam = {"min_bandwidth": min_bandwidth/1000,"max_bandwidth": max_bandwidth/1000,"burst": burst/1000}
        dbmodule.db_op.db_modify('qos',{'id':vswitch_obj.get('qos_id')},qos_updateparam)

        qos_obj = dbmodule.db_op.db_get('qos',{'id':vswitch_obj.get('qos_id')})
        if vs_type == "vswitch" and qos_obj and qos_obj.get('id'):
            operation.vhost.sync_db_op.update_to_vcenter_ref("qos", qos_obj, {"host__uuid":vsuuid, "vswitch__name":name}, 
                                                             ("host", "hosts", {"uuid": vsuuid}))
    dbmodule.db_op.db_modify('vswitch',getparam,updateparam)
    vswitch_obj = dbmodule.db_op.db_get('vswitch',getparam)
    if vs_type == "vswitch" and vswitch_obj and vswitch_obj.get('id'):
        operation.vhost.sync_db_op.update_to_vcenter_ref("vswitch", vswitch_obj, {"hosts__uuid":vsuuid, "name":name},
                                                         ("qos", "qos", {"host__uuid":vsuuid, "vswitch__name":name}))
     
def get_vswitch_hosts(vsname, datacenter_uuid = None):
    
    """
      获取交换机下的所有主机
    """  
    if global_params.vserverflag:
        vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':vsname})
        
    else:
        vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':vsname,"datacenters__uuid":datacenter_uuid})
    
    if not vswitch_obj or not vswitch_obj.get('id'):
        syslog.syslog(syslog.LOG_ERR,'get vswitch_host failed: get vswitch info failed')
        return []
    
    host_list = dbmodule.db_op.db_values('host_vswitch',{'vswitch_id':vswitch_obj.get("id")})
    return_list = []
    for item in host_list:
        hostobj = dbmodule.db_op.db_get('hosts',{'id':item.get("host_id")})
        if hostobj:
            return_list.append(hostobj.get("uuid"))
    return return_list
    

def get_portgroup_hosts(name, datacenter_uuid):
    
    """
      获取端口组下的所有主机
    """  
    if global_params.vserverflag:
        portgroup_obj = dbmodule.db_op.db_get('vm_portgroup',{'name':name})
    else:
        portgroup_obj = dbmodule.db_op.db_get('vm_portgroup',{'name':name,"vswitch__datacenters__uuid":datacenter_uuid})
        
    if not portgroup_obj or not portgroup_obj.get('id'):
        syslog.syslog(syslog.LOG_ERR,'get portgroup_hosts failed: get portgroup info failed')
        return []
    vswitch_obj = dbmodule.db_op.db_get('vswitch',{'id':portgroup_obj.get("vswitch_id")})
    if not vswitch_obj or not vswitch_obj.get('id'):
        syslog.syslog(syslog.LOG_ERR,'get portgroup_hosts failed: get vswitch info failed')
        return []
    host_list = dbmodule.db_op.db_values('host_vswitch',{'vswitch_id':vswitch_obj.get("id")})
    return_list = []
    for item in host_list:
        hostobj = dbmodule.db_op.db_get('hosts',{'id':item.get("host_id")})
        if hostobj:
            return_list.append(hostobj.get("uuid"))
    return return_list     
    
    
def get_vswitch(name, datacenter_uuid = None,vsid = None):
    
    """
      获取虚拟交换机,及其下的qos配置.  
    """
    
    if vsid:
        vswitch_list = dbmodule.db_op.db_values('vswitch',{'id':vsid})
        
    if datacenter_uuid:
        if global_params.vserverflag:
            vswitch_list = dbmodule.db_op.db_values('vswitch',{'name':name}) 
            
        else:
            vswitch_list = dbmodule.db_op.db_values('vswitch',{'name':name,"datacenters__uuid":datacenter_uuid})  
    else:
        vswitch_list = dbmodule.db_op.db_values('vswitch',{'name':name}) 
        
        
    if vswitch_list:
        vswitch_dict = vswitch_list[0]
        qos_dict = dbmodule.db_op.db_get('qos',{'id':vswitch_dict.get("qos_id")})
        vswitch_dict["qos"] = qos_dict
        return vswitch_dict

def get_all_vswitch(vsuuid = None):
    
    """
     获取所有虚拟交换机(包括分布式交换机)，及serviceconsole    
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()
    vswitch_list = dbmodule.db_op.db_values('vswitch',{'vs_type__endswith':'vswitch',"hosts__uuid":vsuuid}) 
    
    for vswitch_dict in vswitch_list:
        sc_list = dbmodule.db_op.db_values('service_console',{'vswitch__id':vswitch_dict["id"]}) 
        
        if sc_list:
            vswitch_dict["service_console"] = sc_list[0]     
        vswitch_dict["netcards"]  = dbmodule.db_op.db_values('netcard',{'vswitch__id':vswitch_dict["id"]}) 
        
    return vswitch_list

def get_all_dvswitch(vsuuid = None):
    
    """
     获取主机上的所有分布式交换机  
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()
    vswitch_list = dbmodule.db_op.db_values('vswitch',{'vs_type':'dvswitch',"hosts__uuid":vsuuid}) 
    

    return vswitch_list

    
def update_portgroup(name, datacenter_uuid = None, vlanid = None, qos_state = None,
                     min_bandwidth = None, max_bandwidth = None, burst = None, vs_type = "vswitch"):
    
    """
      修改端口组，vcenter上只能修改分布式端口组    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    if global_params.vserverflag:
        getparam = {'name':name}
        
    else:
        getparam = {'name':name,"vswitch__datacenters__uuid":datacenter_uuid}
        
    vm_portgroup_obj = dbmodule.db_op.db_get('vm_portgroup',getparam)
    if not vm_portgroup_obj or not vm_portgroup_obj.get('id'):
        syslog.syslog(syslog.LOG_ERR,'update portgroup failed: get portgroup info failed')
        return 
    updateparam = {}
    if vlanid:
        updateparam["vlanid"] = int(vlanid)
        
    if qos_state:
        updateparam["qos_state"] = qos_state
        
    if qos_state == "enable":
        qos_updateparam = {"min_bandwidth": min_bandwidth/1000,"max_bandwidth": max_bandwidth/1000,"burst": burst/1000}
        dbmodule.db_op.db_modify('qos',{'id':vm_portgroup_obj.get('qos_id')},qos_updateparam)
        qos_obj = dbmodule.db_op.db_get('qos',{'id':vm_portgroup_obj.get('qos_id')})
        if vs_type == "vswitch" and qos_obj and qos_obj.get('id'):
            operation.vhost.sync_db_op.update_to_vcenter_ref("qos", qos_obj, {"host__uuid":vsuuid, "vm_portgroup__name":name}, 
                                                             ("host", "hosts", {"uuid": vsuuid}))
    dbmodule.db_op.db_modify('vm_portgroup',getparam,updateparam)
    
    vm_portgroup_obj = dbmodule.db_op.db_get('vm_portgroup',getparam)
    if vm_portgroup_obj and vm_portgroup_obj.get('vswitch_id') :
        vm_portgroup_obj["vswitch_name"] = dbmodule.db_op.db_get('vswitch',{"id":vm_portgroup_obj.get("vswitch_id")}).get("name")
    
        
        if vs_type == "vswitch" and vm_portgroup_obj and vm_portgroup_obj.get('vswitch_name'):
            operation.vhost.sync_db_op.update_to_vcenter_ref("vm_portgroup", vm_portgroup_obj, 
                                                             {"name":name, "vswitch__hosts__uuid":vsuuid}, 
                             ("vswitch", "vswitch", {"name":vm_portgroup_obj.get("vswitch_name"),"hosts__uuid":vsuuid}), 
                                        ("qos", "qos", {"host__uuid":vsuuid, "vm_portgroup__name":name}))
    

def get_portgroup(pgname, datacenter_uuid = None):
    
    """
     获取端口组,及其下面的qos,虚拟网卡,vcenter上只能查询分布式端口组  
    """
    if datacenter_uuid:
        if global_params.vserverflag:
            vm_portgroup_list = dbmodule.db_op.db_values('vm_portgroup',{'name':pgname})
            
        else:
            vm_portgroup_list = dbmodule.db_op.db_values('vm_portgroup',{'name':pgname,"vswitch__datacenters__uuid":datacenter_uuid})
            
    else:
        vm_portgroup_list = dbmodule.db_op.db_values('vm_portgroup',{'name':pgname}) 
        
    if vm_portgroup_list:
        vm_portgroup_dict = vm_portgroup_list[0]
        if vm_portgroup_dict["qos_id"]:
            qos_dict = dbmodule.db_op.db_get('qos',{'id':vm_portgroup_dict["qos_id"]})
            
        else:
            qos_dict = {}
        vswitch_name = dbmodule.db_op.db_get('vswitch',{'id':vm_portgroup_dict["vswitch_id"]}).get("name")
        ports = ["vnet%s" % netcard_vm_obj.get("description") for netcard_vm_obj in dbmodule.db_op.db_values('netcard_vms',{'vm_portgroup__id':vm_portgroup_dict["id"]})]
        vm_portgroup_dict["vswitch"] = vswitch_name
        vm_portgroup_dict["qos"] = qos_dict
        vm_portgroup_dict["ports"] = ports
        return vm_portgroup_dict
    
def get_vs_portgroup_qos(vsname, datacenter_uuid = None):
    
    """
     获取交换机下的端口组,及其下面的qos,虚拟网卡,vcenter上只能查询分布式端口组  
    """
    if global_params.vserverflag:
        vm_portgroup_list = dbmodule.db_op.db_values('vm_portgroup',{'vswitch__name':vsname}) 
    else:
        vm_portgroup_list = dbmodule.db_op.db_values('vm_portgroup',{'vswitch__name':vsname,"vswitch__datacenters__uuid":datacenter_uuid}) 
    for vm_portgroup_dict in vm_portgroup_list:
    
        if vm_portgroup_dict["qos_id"]:
            qos_dict = dbmodule.db_op.db_get('qos',{'id':vm_portgroup_dict["qos_id"]}) 
        else:
            qos_dict = {}

        vm_portgroup_dict["vswitch"] = vsname
        vm_portgroup_dict["qos"] = qos_dict
        
    return vm_portgroup_list
    

    
def get_unused_netcards():
    
    """
       获取未使用的物理网卡  
    """
    netcard_list = dbmodule.db_op.db_values('netcard',{'vswitch':None})
    netcard_list.sort(key=lambda x:x["name"])
    return netcard_list

  
def delete_portgroup(name, datacenter_uuid = None, vs_type = "vswitch"):
    
    """
      删除端口组,vcenter上只能删除分布式端口组     
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    if datacenter_uuid:
        if global_params.vserverflag:
            pgobjs =  dbmodule.db_op.db_values('vm_portgroup',{'name':name})
            
        else:
            pgobjs = dbmodule.db_op.db_values('vm_portgroup',{'name':name,"vswitch__datacenters__uuid":datacenter_uuid})
            
    else:
        pgobjs = dbmodule.db_op.db_values('vm_portgroup',{'name':name})
    
    if not pgobjs:
        return
    for portgroup_obj in pgobjs:
        for netcard_vm_obj in  dbmodule.db_op.db_values('netcard_vms',{'vm_portgroup_id':portgroup_obj.get("id")}):
    
            dbmodule.db_op.db_modify('netcard_vms',{'id':netcard_vm_obj.get("id")},{"vm_portgroup_id":None})
            #删除相关mirror
            dbmodule.db_op.db_delete('netcard_mirror',{'src_netcard_vm':netcard_vm_obj.get("description")})
            dbmodule.db_op.db_delete('netcard_mirror',{'dst_netcard_vm':netcard_vm_obj.get("description")})
    
        if portgroup_obj and portgroup_obj.get("qos_id"):
            dbmodule.db_op.db_delete('qos',{'id':portgroup_obj.get("qos_id")})
        dbmodule.db_op.db_delete('vm_portgroup',{'id':portgroup_obj.get("id")})

   
    is_vcuuid, vcuuid, vcip = support.uuid_op.get_vc_uuid() 
    if is_vcuuid and vcuuid != "127.0.0.1":
        portgroup_list = dbmodule.db_op.db_values_vc('vm_portgroup',{'name':name,"vswitch__hosts__uuid":vsuuid},vcip)
        if portgroup_list:
            if vs_type == "vswitch":   
                #删除端口组,vcenter上只能删除分布式端口组。分布式交换机的虚拟机上，则vserver无法删除。
                portgroup_obj = portgroup_list[0]
                if portgroup_obj and portgroup_obj.get("qos"):
                    dbmodule.db_op.db_delete_vc('qos',{'id':portgroup_obj.get("qos_id")}, vcip)
                for netcard_vm_obj in dbmodule.db_op.db_values_vc('netcard_vms',{'vm_portgroup_id':portgroup_obj.get("id")},vcip):
                    dbmodule.db_op.db_modify_vc('netcard_vms',{'id':netcard_vm_obj.get("id")},{"vm_portgroup_id":None},vcip)
                    
                    #删除相关mirror
                    dbmodule.db_op.db_delete_vc('netcard_mirror',{'src_netcard_vm':netcard_vm_obj.get("description")},vcip)
                    dbmodule.db_op.db_delete_vc('netcard_mirror',{'dst_netcard_vm':netcard_vm_obj.get("description")},vcip)                                         
                
                if portgroup_obj.get("qos_id"):
                    dbmodule.db_op.db_delete_vc('qos',{'id':portgroup_obj.get("qos_id")},vcip)                
                dbmodule.db_op.db_delete_vc('vm_portgroup',{'id':portgroup_obj.get("id")},vcip)
def get_vs_netcards(vsname, vsuuid = None):
    
    """
     获取虚拟交换机的所有物理网卡    
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()
    netcard_objects = dbmodule.db_op.db_values('netcard',{'vswitch__name':vsname,"host__uuid":vsuuid})
    netcard_objects.sort(key=lambda x:x["bond_seq"])
    return netcard_objects

def get_vs_qos_vnetports(vsname, datacenter_uuid = None):
    
    """
     获取所有使用虚拟交换机默认qos配置的虚拟网卡端口    
    """
    if global_params.vserverflag:
        portgroup_objs = dbmodule.db_op.db_values('vm_portgroup',{"vswitch__name":vsname}) 

    else:
        portgroup_objs = dbmodule.db_op.db_values('vm_portgroup',{"vswitch__name":vsname,"vswitch__datacenters__uuid":datacenter_uuid}) 

    netcard_vm_objs = []
    for portgroup_obj in portgroup_objs:
        netcard_vm_objs.extend( dbmodule.db_op.db_values('netcard_vms',{"vm_portgroup_id":portgroup_obj.get("id")}) )
    ports = ["vnet%s" % netcard_vm_obj.get("description") for netcard_vm_obj in netcard_vm_objs]
    return ports

def get_vs_portgroups(vsname, datacenter_uuid = None,hostuuid=None):
    
    """
      获取虚拟交换机的所有端口组   
    """
    if global_params.vserverflag:
        return dbmodule.db_op.db_values('vm_portgroup',{'vswitch__name':vsname})
    else:
        if not hostuuid:
            return []
        return dbmodule.db_op.db_values('vm_portgroup',{'vswitch__name':vsname,"vswitch__hosts__uuid":hostuuid})

def update_netcard(ncname, vsname, bond_seq, ip = None, netmask = None, bootproto = None):
    
    """
      修改网卡配置    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    if vsname:
        vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':vsname}).get("id")  
    else:
        vswitch_obj = None

    netcard_obj = dbmodule.db_op.db_get('netcard',{'name':ncname,"host__uuid":vsuuid})
    if not netcard_obj:
        syslog.syslog(syslog.LOG_ERR,'update netcard failed: get netcard info failed')
        return
    updateparam = {"vswitch_id":vswitch_obj,"bond_seq":bond_seq,"ip":ip,"netmask":netmask,"bootproto":bootproto}
    dbmodule.db_op.db_modify('netcard',{'name':ncname,"host__uuid":vsuuid},updateparam)
    
    netcard_obj["vswitch_id"] = vswitch_obj
    netcard_obj["bond_seq"] = bond_seq
    netcard_obj["ip"] = ip
    netcard_obj["netmask"] = netmask
    netcard_obj["bootproto"] = bootproto   
    
    if vsname:
        operation.vhost.sync_db_op.update_to_vcenter_ref("netcard", netcard_obj, {"name":ncname, "host__uuid":vsuuid}, 
                                                         ("host", "hosts", {"uuid":vsuuid}),
                                                         ("vswitch", "vswitch", {"name":vsname, "hosts__uuid":vsuuid}))
    else:
        operation.vhost.sync_db_op.update_to_vcenter_ref("netcard", netcard_obj, {"name":ncname, "host__uuid":vsuuid}, 
                                                         ("host", "hosts", {"uuid":vsuuid}))
        
def update_netcard_seq(ncname, bond_seq):
    
    """
      修改网卡绑定顺序  
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    netcard_obj = dbmodule.db_op.db_get('netcard',{'name':ncname,"host__uuid":vsuuid})
    if not netcard_obj:
        syslog.syslog('update netcard seq failed : get netcard info failed')
        return
    netcard_obj["bond_seq"] = bond_seq
    netcard_obj["vswitch__name"] = dbmodule.db_op.db_get('vswitch',{'id':netcard_obj.get("vswitch_id")}).get("name")

    updateparam = {"bond_seq":bond_seq}
    dbmodule.db_op.db_modify('netcard',{'name':ncname,"host__uuid":vsuuid},updateparam)
    
    
    if netcard_obj.get("vswitch__name"):
        operation.vhost.sync_db_op.update_to_vcenter_ref("netcard", netcard_obj, {"name":ncname, "host__uuid":vsuuid}, 
                                                         ("host", "hosts", {"uuid":vsuuid}),
                                                         ("vswitch", "vswitch", {"name":netcard_obj.get("vswitch__name"), "hosts__uuid":vsuuid}))
    else:
        operation.vhost.sync_db_op.update_to_vcenter_ref("netcard", netcard_obj, {"name":ncname, "host__uuid":vsuuid}, 
                                                             ("host", "hosts", {"uuid":vsuuid}))

    
    
def get_netcard(netcard_name, vsuuid = None):
    
    """
     获取物理网卡
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()      
    netcard_dict = dbmodule.db_op.db_get('netcard',{'name':netcard_name,"host__uuid":vsuuid})
    if netcard_dict and netcard_dict.get("vswitch_id"):
        netcard_dict["vswitch_name"] = dbmodule.db_op.db_get('vswitch',{'id':netcard_dict["vswitch_id"]}).get("name")
    return netcard_dict


def add_mirrors(vsname, src_ports, dst_ports, output_port):
    
    """
     批量添加mirror表    
    """
    for src_vnetcard_name in src_ports:        
        add_mirror(vsname, src_vnetcard_name, output_port, "outlet")
    for src_vnetcard_name in dst_ports:       
        add_mirror(vsname, src_vnetcard_name, output_port, "inlet")
        
    
def add_mirror(vsname, src_vnetcard, dst_vnetcard, nm_type, vlan = None):
    
    """
     添加mirror表    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    src_vnetcard = _del_vnet_head(src_vnetcard)
    dst_vnetcard = _del_vnet_head(dst_vnetcard)
    vswitch_obj = dbmodule.db_op.db_get('vswitch',{'name':vsname})
    if vswitch_obj:
        mirrot_getparam = {"vswitch_id" : vswitch_obj.get("id"), "src_netcard_vm" : src_vnetcard, "dst_netcard_vm" : dst_vnetcard, "nm_type" : nm_type}
        mirror_list = dbmodule.db_op.db_values('netcard_mirror',mirrot_getparam)
        if not mirror_list:
            insert_param = {"vswitch_id" : vswitch_obj.get("id"), "src_netcard_vm" : src_vnetcard, 
                            "dst_netcard_vm" : dst_vnetcard, "nm_type" : nm_type,"vlan" : vlan}
            dbmodule.db_op.db_save('netcard_mirror',insert_param)
            mirror_obj = insert_param
    
            operation.vhost.sync_db_op.save_to_vcenter_ref("netcard_mirror",mirror_obj, ("vswitch", "vswitch", 
                                                                        {"name":vsname, "hosts__uuid":vsuuid}))
 
def mirror_on_netcard(netcard):
    
    mirror_list = dbmodule.db_op.db_values('netcard_mirror',{'src_netcard_vm':netcard})
    if mirror_list:
        return True
    mirror_list = dbmodule.db_op.db_values('netcard_mirror',{'dst_netcard_vm':netcard}) 
    if mirror_list:
        return True
    
def del_mirrors(vsname, src_ports, dst_ports, output_port):
    
    """
      批量从mirror表中删除   
    """
    for src_vnetcard_name in src_ports:
        del_mirror(vsname, src_vnetcard_name, output_port, "outlet")
    for src_vnetcard_name in dst_ports:
        del_mirror(vsname, src_vnetcard_name, output_port, "inlet")
    
       
def del_mirror(vsname, src_vnetcard, dst_vnetcard, nm_type, vlan = None):
    
    """
     从mirror表中删除
    """
    src_vnetcard = _del_vnet_head(src_vnetcard)
    dst_vnetcard = _del_vnet_head(dst_vnetcard)
    filter_param = {"vswitch__name":vsname, "src_netcard_vm":src_vnetcard,
                    "dst_netcard_vm":dst_vnetcard,"nm_type":nm_type, "vlan":vlan}
    dbmodule.db_op.db_delete('netcard_mirror',filter_param)
  
    operation.vhost.sync_db_op.delete_from_vcenter("netcard_mirror", filter_param)

def get_mirrors_about_vnets(vnetnames, re_types):
    
    """
      获取虚拟网卡有关的mirror    
    """
    vnetnames = map(_del_vnet_head, vnetnames)
    mirrors = []
    if "listen" in re_types:
        mirrors.extend(dbmodule.db_op.db_values('netcard_mirror',{'dst_netcard_vm__in':vnetnames}))
    if "outlet" in re_types:
        mirrors.extend(dbmodule.db_op.db_values('netcard_mirror',{'src_netcard_vm__in':vnetnames,"nm_type":"outlet"}))
        
    if "inlet" in re_types:
        mirrors.extend(dbmodule.db_op.db_values('netcard_mirror',{'src_netcard_vm__in':vnetnames,"nm_type":"inlet"}))

    return mirrors

def del_mirrors_about_vnets(vnetnames):
    
    vnetnames = map(_del_vnet_head, vnetnames)
    
    dbmodule.db_op.db_delete('netcard_mirror',{"dst_netcard_vm__in":vnetnames})
    dbmodule.db_op.db_delete('netcard_mirror',{"src_netcard_vm__in":vnetnames})
    operation.vhost.sync_db_op.delete_from_vcenter("netcard_mirror", 
                                                   {"dst_netcard_vm__in":vnetnames})
    operation.vhost.sync_db_op.delete_from_vcenter("netcard_mirror", 
                                                   {"src_netcard_vm__in":vnetnames})

def get_mirrors(src_netcard, dst_netcard, m_type):
    
    """
     获取mirror    
    """
    src_netcard = _del_vnet_head(src_netcard)
    dst_netcard = _del_vnet_head(dst_netcard)
    filter_key = {"src_netcard_vm":src_netcard,
                  "dst_netcard_vm":dst_netcard}
    if m_type != "all":
        filter_key["nm_type"] = m_type
    
    return dbmodule.db_op.db_values('netcard_mirror',filter_key) 

def get_serviceconsole(scname, vsuuid = None):
    
    """
     获取服务控制台    
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()
    sc_list = dbmodule.db_op.db_values('service_console',{"name":scname,"host__uuid":vsuuid}) 
    
    if sc_list:
        sc_dict = sc_list[0]
        vswitch_name = dbmodule.db_op.db_get('vswitch',{"id":sc_dict["vswitch_id"]}).get("name")  
        sc_dict["vswitch"] = vswitch_name
        return sc_dict
    
def delete_serviceconsole(scname, vsuuid = None):
    
    """
     删除服务控制台    
    """
    if not vsuuid:
        _, vsuuid = support.uuid_op.get_vs_uuid()
    #删除vs关联的qos数据表
    serviceconsole_obj = dbmodule.db_op.db_get('service_console',{"name" : scname, "host__uuid" : vsuuid})
    if serviceconsole_obj and serviceconsole_obj.get("qos_id"):
        dbmodule.db_op.db_delete('qos',{"id" : serviceconsole_obj.get("qos_id")})
    
    dbmodule.db_op.db_delete('service_console',{"name" : scname, "host__uuid" : vsuuid})
    
    ##删除vc数据库中关联的qos
    is_vcuuid,vcuuid,vcip = support.uuid_op.get_vc_uuid() 
    if is_vcuuid and vcuuid != "127.0.0.1":
        serviceconsole_obj = dbmodule.db_op.db_get_vc('netcard',{'host__uuid':vsuuid},vcip)
        if serviceconsole_obj and serviceconsole_obj.get("qos_id"):
            dbmodule.db_op.db_delete_vc('qos',{"id" : serviceconsole_obj.get("qos_id")}, vcip)
    
    operation.vhost.sync_db_op.delete_from_vcenter("service_console", {"name":scname, 
                                                    "host__uuid":vsuuid})
    
def update_serviceconsole(scname, new_scname, boot_proto, ip, netmask):
    
    """
     修改服务控制台    
    """
    _, vsuuid = support.uuid_op.get_vs_uuid()
    get_param = {"name":scname, "host__uuid":vsuuid}
    service_console_obj = dbmodule.db_op.db_get('service_console',get_param)
    if not service_console_obj:
        syslog.syslog(syslog.LOG_ERR,'update serviceconsole failed: get service_console info failed')
        return
    switch_obj = dbmodule.db_op.db_get('vswitch',{"id" : service_console_obj.get("vswitch_id")})
    service_console_obj["vswitch_name"] = switch_obj.get("name")
    sc_obj = service_console_obj 
    sc_obj["name"] = new_scname
    sc_obj["ip"] = ip
    sc_obj["netmask"] = netmask
    sc_obj["bootproto"] = boot_proto
    updateparam = {"name":new_scname,"ip":ip,"netmask":netmask,"bootproto":boot_proto}
    dbmodule.db_op.db_modify('service_console',get_param,updateparam)
   
    operation.vhost.sync_db_op.update_to_vcenter_ref("service_console", sc_obj, get_param,
                                                     ("vswitch", "vswitch", {"hosts__uuid":vsuuid,"name":sc_obj.get("vswitch_name")}),
                                                     ("host", "hosts", {"uuid":vsuuid}))
    
def _del_vnet_head(vnetcardname):
    
    """
      为虚拟网卡除去vnet头    
    """
    if vnetcardname.startswith("vnet"):
        return vnetcardname[4:]
    else:
        return vnetcardname

