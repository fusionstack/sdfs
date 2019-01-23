#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import socket
import json

from config import Config
from utils import exec_shell, dmsg, dwarn, Exp, exec_remote, json_load, json_store, get_dev_by_addr,\
                  kill_tcp_connections, mutil_exec, derror, lock_file, check_ip_valid

from argparse import RawTextHelpFormatter

def _parse(master_vip):
    #master_vip "192.168.120.32:eno16777984,192.168.120.33:eno16777984,192.168.1.100/24";
    #return {'host1': {'eth': eth, 'vip': vip}, ...} 
    print master_vip
    vips = {}
    v1 = master_vip.split(",")
    vip = v1[-1]
    del v1[-1]
    for h in v1:
        host = h.split(":")[0]
        eth = h.split(":")[1]
        vips.update({host: {'eth': eth, "vip": vip}})

    return vips

def vip_get(args):
    config = Config()
    vips = _parse(config.master_vip)
    dmsg(vips)
    return vips

def _vip_get_local(vips, local):
    local = socket.gethostname()
    for h in vips.keys():
        if h == local:
            eth = vips[h]['eth']
            vip = vips[h]['vip']
            return (eth, vip)

    dwarn("no vip")
    return (None, None)

def _vip_set(eth, vip):
    cmd = "ip addr add %s dev %s" % (vip, eth)
    exec_shell(cmd, p=True)

def _vip_unset(eth, vip):
    cmd = "ip addr del %s dev %s" % (vip, eth)
    exec_shell(cmd, p=True)

def _vip_unset_remote(host, eth, vip):
    cmd = "ip addr del %s dev %s" % (vip, eth)
    exec_remote(host, cmd)

def _vip_exist(eth, vip):
    cmd = "ip addr|grep %s|grep %s" % (vip, eth)
    try:
        exec_shell(cmd, p=True)
    except Exp, e:
        dwarn('_vip exist %s' % e)
        return False

    return True

#尝试去掉其他节点上的vip
def _vip_unset_other(vips, local):
    for h in vips.keys():
        if h == local:
            continue

        eth = vips[h]['eth']
        vip = vips[h]['vip']
        try:
            _vip_unset_remote(h, eth, vip)
        except Exp, e:
            dwarn(e)

def vip_set(args):
    #暂时关闭
    return None
    vips = vip_get()
    local = socket.gethostname()

    _vip_unset_other(vips, local)

    eth, vip = _vip_get_local(vips, local)
    if not _vip_exist(eth, vip):
        _vip_set(eth, vip)

def vip_unset(args):
    #暂时关闭
    return None
    vips = vip_get()
    local = socket.gethostname()

    eth, vip = _vip_get_local(vips, local)
    if _vip_exist(eth, vip):
        _vip_unset(eth, vip)

def vip_loadconf(vip_conf):
    return json_load(vip_conf)

def get_local_vips(vip_conf, group):
    data = vip_loadconf(vip_conf)
    _vips = data[group]["vips"]
    vips = ""

    for vip in _vips.split(','):
        if vip == "":
            continue

        try:
            cmd = "ip addr show | grep '%s/' >/dev/null" % (vip)
            exec_shell(cmd, p=False)

            if vips == "":
                vips = vip
            else:
                vips = vips + ',' + vip
        except Exp, e:
            pass

    return vips

def del_local_vips(vip_conf):
    data = vip_loadconf(vip_conf)

    for group in data.keys():
        vips = get_local_vips(vip_conf, group)
        if vips.strip() == "":
            return

        for vip in vips.split(','):
            vip_del(vip, data[group]["mask"])

    return

def get_host_viplist(host, node_script, group):
    cmd = "%s getvip --group %s" % (node_script, group)
    viplist, err = exec_remote(host, cmd)
    return viplist.strip()

def get_vipinfo_by_group(config, hosts, group):
    vip_num_dic = {"total":0, "max":0, "max_host":"", "min":99999, "min_host":""}
    vip_list_dic = {}
    valid_hosts = []
    used_vips = []

    for host in hosts:
        host_isvalid = False
        vip_list = []
        try:
            vips = get_host_viplist(host, config.uss_node, group)
            if len(vips) > 0:
                vip_list += vips.split(',')

            valid_hosts.append(host)
            host_isvalid = True
        except Exp, e:
            vip_list = []

        vip_list_dic[host] = vip_list

        if not host_isvalid:
            continue

        used_vips += vip_list
        vip_num = len(vip_list)
        vip_num_dic["total"] += vip_num

        if vip_num > vip_num_dic["max"]:
            vip_num_dic["max"] = vip_num
            vip_num_dic["max_host"] = host

        if vip_num < vip_num_dic["min"]:
            vip_num_dic["min"] = vip_num
            vip_num_dic["min_host"] = host

    return vip_num_dic, vip_list_dic, valid_hosts, used_vips

def set_vip_by_average(config, hosts, mask, vip_list):
    node_num = len(hosts)
    vip_num = len(vip_list)
    dic = {}

    for i in range(vip_num):
        if dic.has_key(hosts[i%node_num]):
            dic[str(hosts[i%node_num])] = dic[str(hosts[i%node_num])] + ',' + vip_list[i]
        else:
            dic[str(hosts[i%node_num])] = vip_list[i]

    def _warp(h):
        try:
            dmsg("try set vip:%s on host:%s..." % (dic[h], h))
            cmd = "%s setvip --vips %s --mask %s" % (config.uss_node, dic[h], mask)
            exec_remote(h, cmd)
            dmsg("set vip:%s on host:%s ok..." % (dic[h], h))
        except Exp, e:
            derror("%s : %s" % (h, e))

    args = [[h] for (h) in dic.keys()]
    mutil_exec(_warp, args)

def _get_host_vipnum_min(host2vip_dic, valid_hosts):
    _num = 0
    _min = 999999
    _min_host = ""

    for host in valid_hosts:
        _num = len(host2vip_dic[host])
        if _num < _min:
            _min = _num
            _min_host = host

    return _min_host

def set_vip_by_priority(config, hosts, mask, vip_list, host2vip_dic):
    node_num = len(hosts)
    dic = {}
    min_host = {}
    tmp_host2vip_dic = host2vip_dic

    for i in range(len(vip_list)):
        min_host = _get_host_vipnum_min(tmp_host2vip_dic, hosts)
        if dic.has_key(min_host):
            dic[str(min_host)] = dic[str(min_host)] + ',' + vip_list[i]
        else:
            dic[str(min_host)] = vip_list[i]
        tmp_host2vip_dic[min_host].append(vip_list[i])

    def _warp(h):
        try:
            dmsg("try set vip:%s on host:%s..." % (dic[h], h))
            cmd = "%s setvip --vips %s --mask %s" % (config.uss_node, dic[h], mask)
            exec_remote(h, cmd)
            dmsg("set vip:%s on host:%s ok..." % (dic[h], h))
        except Exp, e:
            derror("%s : %s" % (h, e))

    args = [[h] for (h) in dic.keys()]
    mutil_exec(_warp, args)

def set_vips(vips, mask):
    vip_list = vips.split(',')

    if len(vip_list) == 0:
        return

    addr = vip_list[0]
    dev = get_dev_by_addr(addr)
    for vip in vip_list:
        try:
            cmd = "ip addr add %s/%s brd + dev %s && arping -c 1 -U -I %s %s" % (vip, mask, dev, dev, vip)
            exec_shell(cmd, p=False)
        except Exp, e:
                derror("cmd:%s fail, errno:%d, errmsg:%s" % (cmd, e.errno, e.err))

def _delete_local_vip(vip, mask):
    try:
        dev = get_dev_by_addr(vip)
        cmd = "ip addr del %s/%s dev %s" % (vip, mask, dev)
        exec_shell(cmd)
    except Exp, e:
        raise Exp(e.errno, "delete vip:%s dev:%s fail, %s", vip, dev, e.err)

def vip_del(vip, mask):
    #断开vip的tcp连接， 删除vip
    kill_tcp_connections(vip)
    _delete_local_vip(vip, mask)

def vip_isexist(vip):
    cmd = "ip addr | grep '%s/' >/dev/null" % (vip)
    try:
        exec_shell(cmd, p=False)
        return True
    except Exp, e:
        return False

def _check_vip_isexist(config, host, vip):
    try:
        cmd = "%s vipisexist --vip %s" % (config.uss_node, vip)
        exec_remote(host, cmd)
        return True
    except Exp, e:
        return False

def _del_vip_from_host(config, host, vip, mask):
    try:
        cmd = "%s vipdel --vip %s --mask %s" % (config.uss_node, vip, mask)
        exec_remote(host, cmd)
    except Exp, e:
        raise Exp(e.errno, "del vip %s from %s fail, %s" % (vip, host, e.err))

def _add_vip_to_host(config, host, vip, mask):
    try:
        cmd = "%s setvip --vips %s --mask %s" % (config.uss_node, vip, mask)
        exec_remote(host, cmd)
    except Exp, e:
        raise Exp(e.errno, "add vip %s to %s fail, %s" % (vip, host, e.err))

def move_vip(config, from_host, to_host, vip, mask):
    if not _check_vip_isexist(config, from_host, vip):
        dwarn("vip:%s not exist on host:%s" % (vip, from_host))
        return

    _del_vip_from_host(config, from_host, vip, mask)
    dmsg("del vip %s from %s ok" % (vip, from_host))
    _add_vip_to_host(config, to_host, vip, mask)
    dmsg("add vip %s to %s ok" % (vip, to_host))

def _get_vip2host_dic(host2vip_dic):
    vip_dic = {}
    _hosts = host2vip_dic.keys()
    _len = len(_hosts)

    for host in _hosts:
        for vip in host2vip_dic[host]:
           if vip_dic.has_key(vip):
               vip_dic[vip].append(host)
           else:
               vip_dic[vip] = [str(host)]

    return vip_dic

def _deal_repeat_vip(config, mask, vip2host_dic):
    for vip in vip2host_dic.keys():
        hosts = vip2host_dic[vip]
        if len(hosts) > 1:
            for i in range(len(hosts)):
                if i == 0:
                    continue

                _del_vip_from_host(config, hosts[i], vip, mask)

    return

def _all_vip_isused(total_vips, used_vips):
    for vip in total_vips:
        if vip not in used_vips:
            return False

    return True

def vip_balance(args):
    lfile = "/var/run/uss.vipbalance.lock"
    lock = lock_file(lfile)
    config = Config()

    #1：加载vip配置文件
    data = vip_loadconf(config.vip_conf)

    #2：以组为单位进行balance
    for group in data.keys():
        total_vip_nums = len(data[group]["vips"].split(","))
        total_vip_list = data[group]["vips"].split(',')
        nodes_list = data[group]["nodes"].split(',')
        mask = data[group]["mask"]

        while True:
            #3：检查每个节点上的vip数量，并记录节点和vip的对应关系
            vipnum_dic = {}
            host2vip_dic = {}
            used_vips = []
            valid_hosts = []

            vipnum_dic, host2vip_dic, valid_hosts, used_vips = get_vipinfo_by_group(config, nodes_list, group)

            #4：检查是否需要进行vip balancealance：1，节点总vip数不等于vips数量；2，节点vip数量max-min>1
            #    4.1：vip总数量是否为0：
                 #4.1.1  总数为0：说明集群刚启动，没有分配vip， 把vip list平均分配到每个能ping通的节点上即可
                 #4.1.2  总数大于0,小于vips数量：说明集群运行过程中，有节点故障。这种情况需要把vip list中去除掉已有的vip后
                 #       按优先级分配到集群其他可以ping通的节点上， 其中节点上已有的vip数量越少，优先级越高
                 #4.1.3  总数大于vips数量，说明有两个或多个节点都存在同一个vip，需要只保留一个节点的vip，其他删掉
            #    4.2：从max节点上删除一个vip，添加到min节点上，回到步骤3，再次检查
            if  vipnum_dic["total"] != total_vip_nums:
                if vipnum_dic["total"] == 0:
                    dmsg("vip num is %d, set vip by average!" % (vipnum_dic["total"]))
                    set_vip_by_average(config, valid_hosts, mask, total_vip_list)
                elif vipnum_dic["total"] < total_vip_nums:
                    need_set_vip_list = total_vip_list
                    for vip in used_vips:
                        idx = need_set_vip_list.index(vip)
                        del need_set_vip_list[idx]

                    dmsg("vip num is %d, set vip by priority!" % (vipnum_dic["total"]))
                    set_vip_by_priority(config, valid_hosts, mask, need_set_vip_list, host2vip_dic)
                elif vipnum_dic["total"] > total_vip_nums:
                    vip2host_dic = _get_vip2host_dic(host2vip_dic)
                    _deal_repeat_vip(config, mask, vip2host_dic)
            elif  vipnum_dic["max"] - vipnum_dic["min"] > 1:
                max_host = vipnum_dic["max_host"]
                min_host = vipnum_dic["min_host"]
                vip = host2vip_dic[max_host][0]

                dmsg("host:%s vip num:%d, host:%s vip num:%d, move vip:%s" % (max_host, vipnum_dic["max"], min_host, vipnum_dic["min"], vip))
                move_vip(config, max_host, min_host, vip, mask)
                dmsg("move ip:%s from %s to %s success" % (vip, max_host, min_host))
            else:
                if not _all_vip_isused(total_vip_list, used_vips):
                    vip2host_dic = _get_vip2host_dic(host2vip_dic)
                    _deal_repeat_vip(config, mask, vip2host_dic)
                    continue

                dmsg("%s vip balanced!" % (group))
                break

    #5：结束
    return

def is_valid_ip(args):
    hosts = []
    vips = []

    if args.host != None:
        hosts = args.host.split(",")
    if args.vip != None:
        vips = args.vip.split(",")

    if len(hosts):
        for h in hosts:
            check_ip_valid(h)

    if len(vips):
        for v in vips:
            check_ip_valid(v)

    return True

def is_valid_args(args):
    if not is_valid_ip(args):
        return False

    if args.type and args.type not in ['user', 'dns']:
        return False

    return True

def list_distinct(hosts):
    host = [json.dumps(h.strip('"')) for h in hosts.split(',')]
    new_host = sorted(set(host),key=host.index)
    return _list2str(new_host)

def vip_add(args):
    #check args
    if not is_valid_args(args):
        derror('please check args')
        sys.exit(1)

    update = 1
    count = 0
    data = {}
    group_list = []

    config = Config()
    json_file = config.vip_conf

    if os.path.exists(json_file):
        data = json_load(json_file)

    args.host = list_distinct(args.host)
    args.vip = list_distinct(args.vip)

    if len(data) != 0:
        group_list = [(k, data[k]) for k in sorted(data.keys())]
        for k,v in group_list:
            if v['nodes'] == args.host and v['vips'] == args.vip \
                and v['type'] == args.type and v['mask'] == args.mask:
                update = 0
                break

        gname, v = group_list[len(group_list) - 1]
        count = filter(str.isdigit, json.dumps(gname))

    if args:
        gname = "group" + str(int(count)+1)
        data[gname] = {}
        group = data[gname]

        #generate vipconf json
        group['nodes'] = args.host
        group['type'] = args.type
        group['vips'] = args.vip
        group['mask'] = args.mask
    else:
        update = 0

    #data = sorted(data.items(), key=lambda d: d[0])
    #modify vipconf about vip or nodes

    if update:
        json_store(data, json_file)
        dmsg('add vipconf sucessfully')
    else:
        dwarn('vipconf was updated.')

def _list2str(lst):
    host = ""
    for h in lst:
        h = h.strip('"')
        if h != "":
            host = host + h + ','
    return host.rstrip(',')

#add host or vip
def vip_confadd(args):

    #check args
    if not is_valid_ip(args):
        derror('please check args')
        sys.exit(1)

    data = {}
    new_host = []
    new_vip = []

    #load vipconf
    config = Config()
    json_file = config.vip_conf
    if os.path.exists(json_file):
        data = json_load(json_file)

    if len(data) == 0:
        print 'vipconf was null'
        return

    #reload new host and vip
    old_host = json.dumps(data[args.group]['nodes']).strip('"').split(',')
    old_vip = json.dumps(data[args.group]['vips']).strip('"').split(',')

    if args.host:
        new_host = [json.dumps(h.strip('"')) for h in args.host.split(',') if h.strip('"') not in old_host]
    if args.vip:
        new_vip = [json.dumps(v.strip('"')) for v in args.vip.split(',') if v.strip('"') not in old_vip]

    #print old_host
    #print new_host

    if len(new_host) != 0:
        new_host.extend(old_host)
        data[args.group]['nodes'] = _list2str(new_host)

    if len(new_vip) != 0:
        new_vip.extend(old_vip)
        data[args.group]['vips'] = _list2str(new_vip)

    if len(new_host) != 0 or len(new_vip) != 0:
        json_store(data, json_file)
        dmsg('update vipconf sucessfully')
    else:
        dwarn('vipconf is already updated.')

#del host or vip
def vip_confdel(args):

    #check args
    if not is_valid_ip(args):
        derror('please check args')
        sys.exit(1)

    data = {}
    new_host = []
    new_vip = []

    #load vipconf
    config = Config()
    json_file = config.vip_conf
    if os.path.exists(json_file):
        data = json_load(json_file)

    if len(data) == 0:
        print 'vipconf was null'
        return

    if not data.has_key(args.group):
        derror("vipconfdel: %s not exist." % (args.group))
        sys.exit(2)

    #reload new host and vip
    old_host = json.dumps(data[args.group]['nodes']).strip('"').split(',')
    old_vip = json.dumps(data[args.group]['vips']).strip('"').split(',')

    if args.host:
        new_host = [json.dumps(h.strip('"')) for h in old_host if h.strip('"') not in args.host.split(',') and h != '']
    if args.vip:
        new_vip = [json.dumps(v.strip('"')) for v in old_vip if v.strip('"') not in args.vip.split(',') and v != '']

    #print old_host
    #print new_host

    if len(new_host) != 0:
        data[args.group]['nodes'] = _list2str(new_host)

    if len(new_vip) != 0:
        data[args.group]['vips'] = _list2str(new_vip)

    if len(new_host) == 0 and len(new_vip) == 0:
        del data[args.group]

    json_store(data, json_file)
    #dmsg('vipconf del host or vip sucessfully')

#vipconf by group
def vip_confdel_group(args):
    data = {}

    config = Config()
    json_file = config.vip_conf

    if os.path.exists(json_file):
        data = json_load(json_file)

    if len(data) == 0:
        dmsg('vipconf was null')
        return

    if not data.has_key(args.group):
        derror("vipconfdel: %s not exist." % (args.group))
        sys.exit(2)
    else:
        del_vips = data[args.group]["vips"]
        mask = data[args.group]["mask"]
        hosts = data[args.group]["nodes"].split(",")
        del data[args.group]
        json_store(data, json_file)

        for host in hosts:
            try:
                _del_vip_from_host(config, host, del_vips, mask)
            except Exp, e:
                print e

    #dmsg('del vipconf sucessfully')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    subparsers = parser.add_subparsers()

    parser_set = subparsers.add_parser('set', help='set vip')
    parser_set.set_defaults(func=vip_set)

    parser_unset = subparsers.add_parser('unset', help='unset vip')
    parser_unset.set_defaults(func=vip_unset)

    parser_balance = subparsers.add_parser('balance', help='balance vip')
    parser_balance.set_defaults(func=vip_balance)

    parser_add = subparsers.add_parser('addconf', help='add vipconf')
    parser_add.add_argument('-t', '--type', required=True,help='user or dns')
    parser_add.add_argument('-m', '--mask', required=True,help='ip mask')
    parser_add.add_argument('-H', '--host', required=True,help='cluser nodes ip eg:192.168.100.1')
    parser_add.add_argument('-v', '--vip', required=True,help='vip ip eg:192.168.120.1')
    parser_add.set_defaults(func=vip_add)

    parser_addnodes = subparsers.add_parser('add', help='add host or vip of vipconf')
    parser_addnodes.add_argument('-g', '--group', required=True, help='group name eg:group1')
    parser_addnodes.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.100.1')
    parser_addnodes.add_argument('-v', '--vip', help='vip ip eg:192.168.120.1')
    parser_addnodes.set_defaults(func=vip_confadd)

    parser_delnodes = subparsers.add_parser('del', help='del host or vip of vipconf')
    parser_delnodes.add_argument('-g', '--group', required=True, help='group name eg:group1')
    parser_delnodes.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.100.1')
    parser_delnodes.add_argument('-v', '--vip', help='vip ip eg:192.168.120.1')
    parser_delnodes.set_defaults(func=vip_confdel)

    parser_del = subparsers.add_parser('delconf', help='delete vipconf')
    parser_del.add_argument('-g', '--group', required=True, help='group name eg:group1')
    parser_del.set_defaults(func=vip_confdel_group)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
