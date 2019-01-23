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

NUM = 10
gid = 12345678

def group_get(groupname, groupid):
    try:
        cmd = ["sdfs.group", "-G", groupname]
        out, err = exec_pipe1(cmd, 0, False)
        print out
        for line in out.splitlines():
            name = line.split('\t')[0].split(':')[1]
            nameid = line.split('\t')[1].split(':')[1]
            if name == groupname and nameid == groupid:
                print 'get group success!'
            else:
                raise Exp(1, "sdfs.group get group fail! \n")
        return (0,err)
    except Exception as e:
        if e.err:
            #dwarn(str(e.err))
            return (e.errno, e.err)

def group_test_shell(cmd, ignore_errno = 0):
    try:
        exec_shell(cmd)
    except Exception as e:
        ret = e.errno
        if (ret != ignore_errno):
            derror('%s fail ret:%d, %s' % (cmd, ret, e))
            sys.exit(ret)
        else :
            return ignore_errno

    return 0

def group_error_test():
    group_name = "maketest_group%d" % NUM
    cmd = "sdfs.group -s %s -g %s " % (group_name, str(NUM))
    group_test_shell(cmd)

    group_name_new = "maketest_group%d" % (NUM + 1)
    cmd = "sdfs.group -s %s -g %s " % (group_name_new, str(NUM))
    ret = group_test_shell(cmd, errno.EPERM)
    if (ret != errno.EPERM):
        derror('%s fail ret:%d' % (cmd, ret))
        sys.exit(ret)
    else:
        cmd = "sdfs.group -r %s" % (group_name)
        group_test_shell(cmd)
    print '======create group repeat test success!!!======='

def group_create_and_del():
    print 'create group test :'
    for i in range(NUM):
        group_name = "maketest_group%d" % i
        group_id = gid + i
        cmd = ["sdfs.group", "-s", group_name, "-g", str(group_id)]
        exec_pipe1(cmd, 0, True)

        (ret, strerr) = group_get(group_name, str(group_id))
        if ret == errno.ENOENT:
            derror('create group fail %s' % (strerr))
            sys.exit(ret)

    group_ls()
    print 'create group success!!!\n'

    print 'delete group test :'
    for i in range(NUM):
        group_name = "maketest_group%d" % i
        cmd = ["sdfs.group", "-r", group_name]
        exec_pipe1(cmd, 0, True)
        (ret, strerr) = group_get(group_name, str(i))
        if ret != errno.ENOENT:
            derror(strerr)

    print 'delete group success!!!\n'


def group_ls():
    try:
        cmd = "sdfs.group -l"
        exec_shell(cmd)
    except Exception as e:
        derror("%s: ret:%s, %s" % (cmd, e.errno, os.strerror(e.errno)))
        sys.exit(e.errno)
    print '\n'

def group_help():
    try:
        cmd = "sdfs.group --help"
        exec_shell(cmd)
    except Exception as e:
        derror("%s: ret:%s, %s" % (cmd, e.errno, os.strerror(e.errno)))
        sys.exit(e.errno)
    print '\n'
 

def group_test():
    group_help()
    group_ls()
    group_create_and_del()
    group_error_test()
    print "!!!!sdfs.group test success!!!"

if __name__ == "__main__":
    group_test()
