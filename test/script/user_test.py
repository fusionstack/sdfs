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
uid = 12345678
user_group_name = 'maketest_user_group'
user_name = 'maketest_user_name'


def user_get(username, uid = 0, gid = 0, pwd = None):
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
    except Exception as e:
        if e.err:
            #dwarn(str(e.err))
            return (e.errno, e.err)
    print '\n'

def user_test_shell(cmd, ignore_errno = 0):
    try:
        exec_shell(cmd)
    except Exception as e:
        ret = e.errno
        if (ret != ignore_errno):
            derror('%s fail ret:%d, %s' % (cmd, ret, e))
        else :
            return ignore_errno

    return 0

def user_error_test():
    cmd = 'sdfs.user -s tmp_user -g %d' % (gid + 1)
    ret = user_test_shell(cmd, errno.ENOENT)
    if (ret != errno.ENOENT):
        derror('%s fail ret:%d' % (cmd, ret))
        sys.exit(ret)
    else:
        print '======create user but group not exist test success!!!======='

def user_set(user, uid = 0, gid = 0, pwd = None):
    cmd = "sdfs.user -s %s " % (user)
    if gid:
        cmd = cmd + " -g %d" % gid
    if uid:
        cmd = cmd + " -u %d" % uid
    if pwd is not None:
        cmd = cmd + " -p %s" % pwd

    return user_test_shell(cmd)


def user_delete(user):
    cmd = "sdfs.user -r %s" % (user)
    ret = user_test_shell(cmd)

def user_create_and_del():
    cmd = 'sdfs.group -s %s -g %d' % (user_group_name, gid)
    ret = user_test_shell(cmd)

    pwd = 'pass'
    for i in range(NUM):
        user = '%s_%d' % (user_name, i)
        user_set(user, uid + i, gid, pwd)
        user_get(user, str(uid + i), str(gid), pwd)

    user_ls()
    print 'create user success!!!\n'

    print 'delete user test :'
    for i in range(NUM):
        user = '%s_%d' % (user_name, i)
        ret = user_delete(user)

    print 'delete user success!!!\n'

    cmd = 'sdfs.group -r %s' % (user_group_name)
    ret = user_test_shell(cmd)

def user_ls():
    cmd = "sdfs.user -l"
    user_test_shell(cmd)
    print '\n'

def user_help():
    cmd = "sdfs.user --help"
    user_test_shell(cmd)
    print '\n'

def user_test():
    user_help()
    user_ls()
    user_create_and_del()
    user_error_test()

    print "!!!!sdfs.user test success!!!"

if __name__ == "__main__":
    user_test()
