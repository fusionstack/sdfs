#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import errno
import os
import sys
import uuid

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + "/../../admin")
sys.path.insert(0, admin)

from utils import exec_shell, Exp

CUR_PATH = os.path.abspath(os.path.split(os.path.realpath(__file__))[0])
LOCKTEST = CUR_PATH + "/../locks/locktests"
NFS_LOCK_PATH = "/mnt/nfs/lock/"
NFS_LOCK_FILE = NFS_LOCK_PATH + '1'

#create locks file
def locks_init():
    content = str(uuid.uuid1())
    cmd = "mkdir %s" % NFS_LOCK_PATH
    try:
        exec_shell(cmd)
    except Exp, e:
        ret = e.errno
        if (ret != errno.EEXIST):
            raise Exp(ret, "%s failed. ret: %d, %s" % (cmd, ret, e))           

    cmd = "echo '%s' > %s" % (content, NFS_LOCK_FILE)
    try:
        exec_shell(cmd)
    except Exp, e:
            ret = e.errno
            raise Exp(ret, "%s failed. ret: %d, %s" % (cmd, ret, e))      

def locks_test():
    worker_num = 10
    worker_prefix1 = "%d process of" % worker_num
    worker_prefix2 = "%d process of %d" % (worker_num, worker_num)
    #10 processes lock file test
    #10 process of 10 successfully ran test : WRITE ON A READ  LOCK 
    cmd = "%s -n %d -f %s" % (LOCKTEST, worker_num, NFS_LOCK_FILE)
    try:
        stdout, stderr = exec_shell(cmd, need_return=True)
        stdout_list = stdout.split('\n')
        for line in stdout_list:
            print line
            if ((0 == line.find(worker_prefix1)) and
                (-1 == line.find(worker_prefix2))):
                raise
    except Exp, e:
            ret = e.errno
            raise Exp(ret, "%s failed. ret: %d, %s" % (cmd, ret, e)) 

    worker_prefix1 = "%d thread of" % worker_num
    worker_prefix2 = "%d thread of %d" % (worker_num, worker_num)
    #10 thread of 10 successfully ran test : WRITE ON A READ  LOCK
    #10 threads lock file test
    cmd = "%s -n %d -f %s -T" % (LOCKTEST, worker_num, NFS_LOCK_FILE)
    try:
        stdout, stderr = exec_shell(cmd, need_return=True)
        stdout_list = stdout.split('\n')
        for line in stdout_list:
            print line
            if ((0 == line.find(worker_prefix1)) and
                (-1 == line.find(worker_prefix2))):
                raise
    except Exp, e:
            ret = e.errno
            raise Exp(ret, "%s failed. ret: %d, %s" % (cmd, ret, e)) 

def locks_destroy():
    cmd = "rm -rf %s" % NFS_LOCK_PATH
    try:
        exec_shell(cmd)
    except Exp, e:
            ret = e.errno
            raise Exp(ret, "%s failed. ret: %d, %s" % (cmd, ret, e)) 

if __name__ == "__main__":
    locks_init()
    locks_test()
    locks_destroy()

