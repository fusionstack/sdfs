#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import socket
import errno

from argparse import RawTextHelpFormatter

from utils import exec_shell, exec_remote, ip_addrs, dmsg, dwarn, derror, Exp, ping
from config import Config
from node import Node

#不要在drbd_primary, drbd_secondary, drbd_primary_check 中初始化Config,
#防止由于配置文件的临时错误，导致服务异常

#The local ip is always displayed first, the remote ip state last.
def drbd_hosts():
    cmd = "cat /etc/drbd.d/mds.res|grep address|awk '{print $NF}'|awk -F':' '{print $1}'"
    hosts, _ = exec_shell(cmd, need_return=True)
    hosts = [x.strip() for x in hosts.split()]
    assert(len(hosts) == 2)

    local_ips = [x.split('/')[0] for x in ip_addrs()]
    local_ip = None
    remote_ip = None
    for i in hosts:
        if i in local_ips:
            local_ip = i
        else:
            remote_ip = i

    assert(local_ip != None and remote_ip != None)
    return [local_ip, remote_ip]

#The local disk state is always displayed first, the remote disk state last.
def drbd_dstate():
    cmd = "drbdadm dstate mds"
    states , _ = exec_shell(cmd, need_return=True)
    return [x.strip() for x in states.split('/')]

#返回的两个状态没有顺序
def drbd_cstate():
    states = []
    for h in drbd_hosts():
        state = None
        cmd = "drbdadm cstate mds"
        try:
            state, _ = exec_remote(h, cmd, exectimeout=7)
        except Exp, e:
            dwarn(e)

        states.append(state)

    return states

def _mount():
    cmd = "mount | grep /dev/drbd0|grep /var/lib/leveldb"
    try:
        exec_shell(cmd)
    except Exp, e:
        dwarn('%s' % e)
        return False

    return True

def _stop_remote_redis(host):
    while True:
        try:
            cmd = "pkill -15 redis-server"
            exec_remote(host, cmd, exectimeout=7)
        except Exp, e:
            dwarn(e)

        try:
            cmd = "ps aux|grep -v grep|grep redis-server"
            exec_remote(host, cmd, exectimeout=7)
        except Exp, e:
            dwarn(e)
            p = ping(host)
            if p:
                dwarn("redis-server stopped!")
            else:
                dwarn("%s network down!" % (host))
            break

        time.sleep(3)
        dwarn("wait redis-server stop!")

#尝试停止其他节点的redis, 和 drbd secondary
def _drbd_secondary_remote(host):
    try:
        cmd = "umount /dev/drbd0"
        exec_remote(host, cmd, exectimeout=7)
    except Exp, e:
        dwarn(e)

    try:
        cmd = "drbdadm secondary mds"
        exec_remote(host, cmd, exectimeout=7)
    except Exp, e:
        dwarn(e)

#尝试secondary其他节点
def _drbd_secondary_other():
    h = drbd_hosts()[1]
    dmsg("secondary %s" % (h))
    _drbd_secondary_remote(h)

def _test_exec_remote():
    cmd = "date"
    host = drbd_hosts()[1]
    exec_remote(host, cmd, exectimeout=1)

def _drbd_primary_prep():
    try:
        _drbd_secondary_other()
    except Exp, e:
        dwarn(e)

    hosts = drbd_hosts()
    cmd = "drbdadm connect mds"
    for h in hosts:
        try:
            exec_remote(h, cmd, exectimeout=7)
        except Exp, e:
            pass

def _drbd_primary_set():
    #wait priamry
    cmd = "drbdadm primary mds"
    exec_shell(cmd)

    if not _mount():
        cmd = "mount /dev/drbd0 /var/lib/leveldb"
        exec_shell(cmd)

    cmd = "mount|grep /var/lib/leveldb"
    exec_shell(cmd)

def drbd_primary():
    try:
        _drbd_primary_set()
    except Exception, e:
        _drbd_primary_prep()
        _drbd_primary_set()
        dwarn(e)

def is_redis_stop():
    cmd = "ps -ef | grep redis-server | grep -v grep"
    try:
        exec_shell(cmd) #redis exists
        return False
    except Exp, e:
        return True     #redis stopped

def confirm_redis_stop():
    while True:
        is_stop = is_redis_stop()
        if not is_stop:
            cmd = "pkill -9 redis-server"
            try:
                exec_shell(cmd)
            except Exp, e:
                pass
        else:
            break

def _drbd_secondary():
    cmd = "umount /dev/drbd0"
    try:
        exec_shell(cmd)
    except Exp, e:
        pass

    cmd = "drbdadm secondary mds"
    exec_shell(cmd)

def drbd_secondary():
    retry = 0
    retry_max = 10
    while True:
        try:
            _drbd_secondary()
            break
        except Exp, e:
            dwarn("wait secondary %s" % (e))
            retry = retry + 1
            time.sleep(1)

        if (retry > retry_max):
            raise Exp(1, "secondary fail")

def _check_dual_mount():
    hosts = drbd_hosts()
    mount = 0

    for h in hosts:
        cmd = "mount|grep '/var/lib/leveldb'"
        try:
            exec_remote(h, cmd, exectimeout=7)
            mount = mount + 1
        except Exp, e:
            pass

    if (mount > 1):
        raise Exp("may be split-brain, dual mount")
        #derror("may be split-brain, dual mount")
        #cmd = "uss.node stop"
        #exec_shell(cmd)

def _check_localdisk():
    states = drbd_dstate()
    if states[0] in ['Failed', 'Diskless']:
        raise Exp("disk error")
        #cmd = "uss.node stop"
        #exec_shell(cmd)

    cmd = "mount|grep '/var/lib/leveldb'"
    exec_shell(cmd)

def _check_dual_standalone():
    states = drbd_cstate()
    if states.count("Connected") == 2:
        return None

    hosts = drbd_hosts()
    for h in hosts:
        cmd = "drbdadm connect mds"
        try:
            exec_remote(h, cmd, exectimeout=7)
        except Exp, e:
            pass

    states = drbd_cstate()
    if states.count("StandAlone") == 2:
        raise Exp("may be split-brain, dual standalone")

#def drbd_primary_check():
    #_check_localdisk()
    #_check_dual_mount()
    #_check_dual_standalone()

def _check_zk(node):
    zk_fail = True
    retry = 3

    while (retry > 0):
        try:
            exec_shell(node.config.uss_zk)
            zk_fail = False
            break
        except Exp, e:
            derror("zk fence retry %s. %s" % (retry, e))
            time.sleep(3)
            retry = retry - 1

    if zk_fail:
        derror("zk fence fail. %s" % (e))
        node.stop()
        drbd_secondary()
        node.start()

def drbd_fence():
    drbd_conf = "/etc/drbd.d/mds.res"
    if not os.path.isfile(drbd_conf):
        return None

    config = Config()
    node = Node(config)

    try:
        _check_dual_mount()
        _check_dual_standalone()
    except Exp, e:
        derror("drbd fence fail. %s" % (e))
        node.stop()
        drbd_secondary()
        node.start()

    #_check_zk(node)

    if node.is_master() and (not config.test):
        try:
            _check_localdisk()
        except Exp, e:
            derror("drbd check master fail. %s" % (e))
            node.stop()
            drbd_secondary()
            node.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    subparsers = parser.add_subparsers()

    parser_set = subparsers.add_parser('primary', help='set primary')
    parser_set.set_defaults(func=drbd_primary)

    parser_unset = subparsers.add_parser('secondary', help='set secondary')
    parser_unset.set_defaults(func=drbd_secondary)

    #parser_check = subparsers.add_parser('primary_check', help='check drbd')
    #parser_check.set_defaults(func=drbd_primary_check)

    parser_fence = subparsers.add_parser('fence', help='fence drbd')
    parser_fence.set_defaults(func=drbd_fence)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func()
