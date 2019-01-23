#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import os
import errno
import uuid
import getopt
import subprocess
import sys
import time

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../admin')
sys.path.insert(0, admin)

from utils import  get_value, Exp, dwarn, dmsg, derror, exec_shell
from config import Config
#from fail import Fail

TEST_PATH = None

def fail_exit(msg):
    derror(msg)
    os.system('for pid in `ps -ef | grep test_list.py | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('for pid in `ps -ef | grep test.py | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('for pid in `ps -ef | grep "objck" | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('kill -9 ' + str(os.getpid()))

def test_coredump():
    """
    检查是否有core产生, 如果有就exit
    """
    p = "%s/core/" % (TEST_PATH)
    cores = os.listdir(p)
    if (len(cores)):
        dmsg("%s cores: %s" % (p, cores))
    if cores:
        #raise Exp(1, "has core %s" % str(cores))
        derror("has core %s" % str(cores))
        exit(errno.EPERM)

def test_invalid_rw():
    """
    检查是否有Invalid read Invalid write, 如果有就exit
    """
    p = "%s/log/valgrind*" % (TEST_PATH)

    cmd = "grep 'Invalid read' %s 2>/dev/null" % (p)
    #dmsg(cmd)
    ret = os.system(cmd)
    if (ret == 0):
        derror("has invalid read")
        exit(errno.EPERM)

    cmd = "grep 'Invalid write' %s 2>/dev/null" % (p)
    #dmsg(cmd)
    ret = os.system(cmd)
    if (ret == 0):
        derror("has invalid write")
        exit(errno.EPERM)

def test_exec(cmd):
    p = subprocess.Popen(cmd, shell=True)
    try:
        res = p.communicate()
        ret = p.wait()
        if (ret == 0):
            pass
        else:
            msg = "exec cmd: %s, fail: %s" % (cmd, str(res))
            raise Exp(ret, msg)
    except KeyboardInterrupt as err:
        dwarn("interupted")
        p.kill()
        exit(errno.EINTR)

    test_coredump()
    test_invalid_rw()
    
class Test_list:
    def __init__(self, parent, count, config):
        self.bin = "fake-bin"
        self.count = count
        self.parent = parent
        self.config = config
        self.dict = {}
        for i in range(self.count):
            self.dict[str(uuid.uuid1())] = str(uuid.uuid1())

    def create(self):
        idx = 0
        total = len(self.dict.items())
        for (k, v) in self.dict.items():
            idx += 1
            cmd_create = self.cmd_create(k, v)
            cmd_remove = self.cmd_remove(k, v)

            retry = 0
            while (1):
                dmsg(cmd_create + '[%d/%d]' % (idx, total) + " retry %d" % (retry))
                try:
                    test_exec(cmd_create)
                except Exp, e:
                    if (e.errno == errno.EIO or retry > 300):
                        raise

                    if (e.errno == errno.EEXIST):
                        if (retry == 0):
                            raise Exp(errno.EEXIST, "create fail, exist")
                        else:
                            try:
                                test_exec(cmd_remove)
                            except Exp, e:
                                pass

                    time.sleep(1)
                    retry = retry + 1
                    continue
                break

    def check(self):
        idx = 0
        total = len(self.dict.items())
        for (k, v) in self.dict.items():
            idx += 1
            cmd_check = self.cmd_check(k, v)

            retry = 0
            while (1):
                dmsg(cmd_check + ' [%d/%d]' % (idx, total) + " retry %d" % (retry))
                try:
                    test_exec(cmd_check)
                except Exp, e:
                    if (e.errno == errno.EIO or retry > 300):
                        raise

                    time.sleep(1)
                    retry = retry + 1
                    continue
                break

    def update(self):
        if (self.updateable == False):
            return

        idx = 0
        total = len(self.dict.items())
        for (k, v) in self.dict.items():
            idx += 1
            self.dict[k] = v + str(uuid.uuid1())

        for (k, v) in self.dict.items():
            cmd_update = self.cmd_update(k, v)

            retry = 0
            while (1):
                dmsg(cmd_update + '[%d/%d]' % (idx, total) + " retry %d" % (retry))
                try:
                    test_exec(cmd_update)
                except Exp, e:
                    if (e.errno == errno.EIO or retry > 300):
                        raise

                    time.sleep(1)
                    retry = retry + 1
                    continue
                break

def _test_exec(cmd):
    retry = 0
    while (1):
        try:
            test_exec(cmd)
        except Exp, e:
            if (e.errno == errno.EEXIST):
                break
            elif (e.errno == errno.EIO or retry > 300):
                raise

            time.sleep(1)
            retry = retry + 1
            continue
        break

def test_mkdir(config, target, ec=None):
    if ec:
        cmd = "%s %s -e %s" % (config.uss_mkdir, target, ec);
    else:
        cmd = "%s %s" % (config.uss_mkdir, target);
    _test_exec(cmd)
    return target

def test_touch(target):
    cmd = "%s %s" % (config.uss_touch, target);
    cmd = "%s -s 1G %s" % (config.uss_truncate, target);
    _test_exec(cmd)
    return target

class Dir_test(Test_list):
    def __init__(self, parent, count, config):
        Test_list.__init__(self, parent, count, config)
        self.updateable = False
    def cmd_create(self, key, value):
        return "%s %s/%s" % (self.config.uss_mkdir, self.parent, key)

    def cmd_check(self, key, value):
        return "%s %s/%s > /dev/null " % (self.config.uss_ls, self.parent, key)

    def cmd_remove(self, key, value):
        return "%s %s/%s" % (self.config.uss_rmdir, self.parent, key)

class File_test(Test_list):
    def __init__(self, parent, count, config):
        Test_list.__init__(self, parent, count, config)
        self.updateable = True
    def cmd_create(self, key, value):
        return "%s %s/%s" % (self.config.uss_touch, self.parent, key)

    def cmd_check(self, key, value):
        return "got=`%s %s/%s ` && need='%s' && if [ $need != $got ]; then echo need: $need got: $got && exit 5; fi" %\
                (self.config.uss_cat, self.parent, key, value)

    def cmd_remove(self, key, value):
        return "%s %s/%s" % (self.config.uss_rm, self.parent, key)

    def cmd_update(self, key, value):
        return "%s %s %s/%s" % (self.config.uss_write, value, self.parent, key)

class Attr_test(Test_list):
    def __init__(self, parent, count, config):
        Test_list.__init__(self, parent, count, config)
        self.updateable = True
    def cmd_create(self, key, value):
        return "%s -s %s -V %s %s" % (self.config.uss_attr, key, value, self.parent)

    def cmd_check(self, key, value):
        return "got=`%s -g %s %s ` && need='%s' && if [ $need != $got ]; then echo need: $need got: $got && exit 5; fi" %\
                (self.config.uss_attr, key, self.parent, value)

    def cmd_remove(self, key, value):
        return "%s -r %s %s" % (self.config.uss_attr, key, self.parent)

    def cmd_update(self, key, value):
        return "%s -s %s -V %s %s" % (self.config.uss_attr, key, value, self.parent)

def usage():
    print ("usage:")
    print (sys.argv[0] + " --home <home> --length [length] --type [dir,file,attr] [-e k+r]")

def main():
    try:
        opts, args = getopt.getopt(
                sys.argv[1:], 
                'hv', ['length=', 'type=', "home=", "ec="]
                )
    except getopt.GetoptError, err:
        print str(err)
        usage()
        exit(errno.EINVAL)

    home = None
    t = 'all'
    ec = "2+1"
    for o, a in opts:
        if o in ('--help'):
            usage()
            exit(0)
        elif o in ('-v', '--verbose'):
            verbose = 1
        elif o in ('--length'):
            length = int(a)
        elif o in ('--home'):
            home = a
        elif o in ('--type'):
            t = a
        else:
            assert False, 'oops, unhandled option: %s, -h for help' % o
            exit(1)

    global TEST_PATH
    TEST_PATH = home
    config = Config(home)

    test = []
    if (t == 'dir'):
        target = test_mkdir(config, "/testdir")
        test.append(Dir_test(target, length))
    elif (t == 'file'):
        target = test_mkdir(config, "/testfile")
        test.append(File_test(target, length))
    elif (t == 'attr'):
        target = test_mkdir(config, "/testattr")
        test.append(Attr_test(target, length))
    elif (t == 'ec'):
        target = test_mkdir(config, "/testdir_ec", ec)
        test.append(Dir_test(target, length, config))
        target = test_mkdir(config, "/testfile_ec", ec)
        test.append(File_test(target, length, config))

    elif (t == 'all'):
        target = test_mkdir(config, "/testdir")
        test.append(Dir_test(target, length, config))

        target = test_mkdir(config, "/testfile")
        test.append(File_test(target, length, config))

        target = test_mkdir(config, "/testattr")
        test.append(Attr_test(target, length, config))

        #测试小文件，小文件会放在leveldb中
        target = test_mkdir(config, "/small")
        target = test_mkdir(config, "/small/testfile")
        test.append(File_test(target, length, config))

        """
        derror("ec test disabled")
        """

        target = test_mkdir(config, "/testdir_ec", ec)
        test.append(Dir_test(target, length, config))

        target = test_mkdir(config, "/testfile_ec", ec)
        test.append(File_test(target, length, config))
        target = test_mkdir(config, "/small/testfile_ec", ec)

    else:
        assert False, 'oops, unhandled option: %s, -h for help' % t
        exit(1)

    for i in test:
        try:
            i.create()
        except Exp, e:
            derror(e.err)
            os.system('kill -9 ' + str(os.getpid()))

    for i in test:
        try:
            i.update()
        except Exp, e:
            derror(e.err)
            os.system('kill -9 ' + str(os.getpid()))

    for i in test:
        try:
            i.check()
        except Exp, e:
            derror(e.err)
            os.system('kill -9 ' + str(os.getpid()))

if __name__ == '__main__':
    if (len(sys.argv) == 1):
        usage()
    else:
        main()
