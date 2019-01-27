#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys
import subprocess
import signal
import time
import paramiko
import platform
import threading
import errno

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../../admin')
sys.path.insert(0, admin)

from utils import  Exp, dwarn, derror, exec_shell, exec_pipe1
from config import Config

NUM = 5

protocol = ["cifs", "ftp", "nfs"]
op = ['s', 'G', 'r']
group_name = 'maketest_share_group'
user_name = 'maketest_share_user'
share_name = 'share'
path = "/maketest_share"


'''
def share_get_check(username, uid = 0, gid = 0, pwd = None):
    try:
        cmd = ["sdfs.user", "-G", username]
        out, err = exec_pipe1(cmd, 0, True)

        name = out.split('\n')[0].split(':')[1]
        nameuid = out.split('\n')[1].split(':')[1]
        namegid = out.split('\n')[2].split(':')[1]
        namepwd = out.split('\n')[3].split(':')[1]
        if name == username and namegid == gid and\
           nameuid == uid and namepwd == pwd:
            print 'get group success!'
        else:
            raise Exp(1, "sdfs.group get group fail! \n")
        return (0,err)
    except Exp, e:
        if e.err:
            #dwarn(str(e.err))
            return (e.errno, e.err)
    print '\n'
'''

def share_test_shell(cmd, ignore_errno = 0):
    try:
        exec_shell(cmd)
    except Exception as e:
        ret = e.errno
        if (ret != ignore_errno):
            derror('%s fail ret:%d, %s' % (cmd, ret, e))
            return ret
        else :
            return ignore_errno

    return 0

def share_set_error(path = None):
    #sdfs.share -s /share -H uss01 -g group1 -u user1 -n share -m rw -p nfs

    cmd = ["hostname"]
    out, err = exec_pipe1(cmd, 0, False)
    hostname = out.split('\n')[0]

    for p in protocol:
        if p != 'nfs':
            share_err_test(path, op[0], hostname, group_name, 'rw',
                           p, None, user_name)
            share_err_test(path, op[0], None, None, 'rw',
                           p, share_name, None)
        else:
            share_err_test(path, op[0], hostname, group_name, 'rw',
                           p, None, None)
    print '======create share err test success!!!======='


#cmd = "sdfs.share -H uss01 -m rw -p cifs -s -n share3 /perf"
#cmd = "sdfs.share -r /perf -p cifs -u group1 -n share3"
#cmd = "sdfs.share -m rw -p cifs -G -n share3 /perf -g group1"

def share_op(path, opt = 's', host = None, group_name = None, mode = None, 
             protocol = None, share_name = None, user_name = None):
    cmd = "sdfs.share -%s %s " % (opt, path)
    if protocol == 'nfs' and host:
        cmd = cmd + " -H %s" % host
    if user_name:
        cmd = cmd + " -u %s" % user_name
    if group_name and protocol != 'ftp':
        cmd = cmd + " -g %s" % group_name
    if mode:
        cmd = cmd + " -m %s" % mode
    if protocol:
        cmd = cmd + " -p %s" % protocol
    if share_name:
        cmd = cmd + " -n %s" % share_name

    return share_test_shell(cmd)


def share_err_test(path, opt = None, host = None, group_name = None, mode = None,
                   protocol = None, share_name = None, user_name = None, err = errno.EPERM):
    cmd = "sdfs.share -%s %s " % (opt, path)
    if host:
        cmd = cmd + " -H %s" % host
    if user_name:
        cmd = cmd + " -u %s" % user_name
    if group_name:
        cmd = cmd + " -g %s" % group_name
    if mode:
        cmd = cmd + " -m %s" % mode
    if protocol:
        cmd = cmd + " -p %s" % protocol
    if share_name:
        cmd = cmd + " -n %s" % share_name

    ret = share_test_shell(cmd, err)
    if (ret != errno.EPERM):
        derror('%s fail, need return EPERM but ret:%d' % (cmd, ret))
        sys.exit(ret)

def share_create_get_del(path = '/'):
    #set share
    cmd = ["hostname"]
    out, err = exec_pipe1(cmd, 0, False)
    hostname = out.split('\n')[0]

    for p in protocol:
        for o in op:
            if (p != 'nfs'):
                ret = share_op(path, o, hostname, None, 'rw',
                         p, share_name, user_name)
                if ret:
                    raise Exp(ret, "cmd %s faile:%d" % (cmd, ret))
            if (p != 'ftp'):
                ret = share_op(path, o, None, group_name, 'rw',
                         p, share_name, None)
                if ret:
                    raise Exp(ret, "cmd %s faile:%d" % (cmd, ret))

    print 'create get del share success!!!\n'


def share_ls(path, pro = None):
    if pro is not None:
        cmd = "sdfs.share -l -p %s %s" % (pro, path)
        share_test_shell(cmd)
    else:
        for p in protocol:
            cmd = "sdfs.share -l -p %s %s" % (p, path)
            share_test_shell(cmd)
    print '\n'


def sdfs_share.help():
    cmd = "sdfs.share --help"
    share_test_shell(cmd)
    print '\n'

def share_test():
    print('mkdir share path :%s' % (path))
    cmd = "sdfs.mkdir %s" % path
    share_test_shell(cmd)

    sdfs_share.help()
    share_ls(path)
    share_create_get_del(path)
    share_set_error(path)

    print('rmdir share path :%s' % (path))
    cmd = "sdfs.rmdir %s" % path
    share_test_shell(cmd)

    print "!!!!sdfs.share test success!!!"

if __name__ == "__main__":
    share_test()
