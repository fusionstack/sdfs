#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os, sys
import errno
import logging
import time

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../../admin')
sys.path.insert(0, admin)
from utils import exec_shell, Exp

logging.basicConfig(level=logging.DEBUG, format='%(filename)s[line:%(lineno)d] %(message)s')

QUOTA_LVM="/quota_home"
QUOTA_LEVEL_2="/quota_home/l2"
QUOTA_LEVEL_3="/quota_home/l2/l3"

def uss_mkdir(path):
    _exec_mkdir = 'sdfs.mkdir %s' % path
    try:
        exec_shell(_exec_mkdir)
    except Exp, e:
        logging.error('sdfs.mkdir %s failed: %s\n' % (path, e.err))
        sys.exit(e.errno)

def uss_create_quota(inode, space, path):
    _exec_quota = 'sdfs.quota --create --space-hardlimit %d --bytes --inode-hardlimit %d --directory %s' % (space, inode, path)
    try:
        exec_shell(_exec_quota)
    except Exp, e:
        logging.error('sdfs.quota %s failed: %s\n' % (path, e.err))
        sys.exit(e.errno)

def quota_test_prep():
    uss_mkdir(QUOTA_LVM)

    uss_mkdir(QUOTA_LEVEL_2)
    uss_create_quota(5, 10, QUOTA_LEVEL_2)

    uss_mkdir(QUOTA_LEVEL_3)
    uss_create_quota(10, 20, QUOTA_LEVEL_3)

def uss_touch(start, end, parent):
    for i in range(start, end):
        _exec_touch = 'sdfs.touch %s/%d' % (parent, i)
        try:
            exec_shell(_exec_touch)
        except Exp, e:
            return e.errno
    return True

def uss_write(filename, content):
    _exec_write = 'sdfs.write %s %s' % (content, filename)
    try:
        exec_shell(_exec_write)
    except Exp, e:
        return e.errno
    return 0

def quota_test_1():
    exceed = False
    quota_test_prep()

    rst = uss_touch(1, 5, QUOTA_LEVEL_3)
    if rst is not True:
        logging.error('sdfs.touch failed: %d\n' % rst)
        return False

    _exec_touch = 'sdfs.touch %s/5' % QUOTA_LEVEL_3
    try:
        exec_shell(_exec_touch)
    except Exp, e:
        if e.errno == errno.EDQUOT:
            logging.error('disk quota exceed\n')
            exceed = True
        else:
            logging.error('sdfs.touch failed: %s\n', e.err)
            return False

    if not exceed:
        return False

    file1 = os.path.join(QUOTA_LEVEL_3, '1')
    rst = uss_write(file1, "111111111")
    if rst != 0:
        logging.error('sdfs.write %s failed: %d' % (file1, rst))
        return False

    for i in range(13):
        print i
        time.sleep(1)

    file2 = os.path.join(QUOTA_LEVEL_3, '2')
    rst = uss_write(file2, "2222")
    if rst != errno.EDQUOT:
        print("test fail:" + str(rst))
        return False

    return True

def uss_modify_inode(inode):
    _exec_modify = 'sdfs.quota --modify --inode-hardlimit %d --directory %s' % (inode, QUOTA_LEVEL_2)
    try:
        exec_shell(_exec_modify)
    except Exp, e:
        logging.error('sdfs.quota modify failed: %s\n', e.err)
        sys.exit(e.errno)

def quota_test_2():
    uss_modify_inode(10)

    rst = uss_touch(5, 10, QUOTA_LEVEL_3)
    if rst is not True:
        logging.error('sdfs.touch failed: %d\n' % rst)
        return False

    _exec_touch = 'sdfs.touch %s/10' % QUOTA_LEVEL_3
    try:
        exec_shell(_exec_touch)
    except Exp, e:
        if e.errno == errno.EDQUOT:
            logging.error('disk quota exceed\n')
            return True
        else:
            logging.error('sdfs.touch failed: %s\n' % e.err)
            return False
    return False

def uss_remove_quota(path):
    _exec_quota_remove = 'sdfs.quota --remove --directory %s' % path
    try:
        exec_shell(_exec_quota_remove)
    except Exp, e:
        logging.error('sdfs.quota remove failed: %s\n' % e.err)
        sys.exit(e.errno)

def uss_rm(filename):
    _exec_remove = 'sdfs.rm %s' % filename
    try:
        exec_shell(_exec_remove)
    except Exp, e:
        logging.error('sdfs.rm failed: %s\n' % e.err)
        sys.exit(e.errno)

def uss_rmdir(path):
    _exec_rmdir = 'sdfs.rmdir %s' % path
    try:
        exec_shell(_exec_rmdir)
    except Exp, e:
        logging.error('sdfs.rmdir failed: %s\n' % e.err)
        sys.exit(e.errno)

def quota_clean():
    uss_remove_quota(QUOTA_LEVEL_3)
    uss_remove_quota(QUOTA_LEVEL_2)

    for i in range(1, 10):
        filename = os.path.join(QUOTA_LEVEL_3, "%d" % i)
        uss_rm(filename)

    uss_rmdir(QUOTA_LEVEL_3)
    uss_rmdir(QUOTA_LEVEL_2)
    logging.error('rm vol not implimented\n')
    #uss_rmdir(QUOTA_LVM)

    return True

def quota_test():
    rst = quota_test_1()
    if not rst:
        print '!!!!!test1 failed!!!!!!'
        sys.exit(-1)
    rst = quota_test_2()
    if not rst:
        print '!!!!!test2 failed!!!!!!'
        sys.exit(-1)
    rst = quota_clean()
    if rst:
        print '!!!!!quota test succ!!!!!'

if __name__ == "__main__":
    quota_test()
