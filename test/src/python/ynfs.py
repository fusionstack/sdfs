#!/bin/python

import os
import sys
import unittest

"""
YNFS test
@pre 
1. has a mds user
2. mount to /mnt/yfs

Test case:
    mkdir
    rmkdir
    rm
    cp 
    mv
    pwd
    chown
    chmod
    chgrp
"""

# global variables
mount_dir  = '/mnt/yfs'
input_dir  = '/tmp/ynfs_input'
output_dir = '/tmp/ynfs_output'

def make_test_file(n):
    s = 'abcdefghijklmnopqrstuvwxyz0123456789'
    f = open('%s/test_%d' % (input_dir, n), 'w')

    p = n / len(s)
    q = n % len(s)

    for i in range(p):
        f.write(s)
    f.write(s[:q])
    f.close()

class NFS_test(unittest.TestCase):
    def setUp(self):
        os.system("mount -o noacl,rsize=524288,wsize=524288,proto=tcp localhost:/mds/data %s" % mount_dir)
        os.system("cd %s" % mount_dir)
        pass
    def tearDown(self):
        os.system("cd /sysy/yfs")
        os.system("umount -f %s" % mount_dir)
        pass
    def test_mkdir(self):
        s = '%s/top' % mount_dir
        os.system("mkdir -p %s" % s)
        for i in range(60):
            s = s + '/%d' % i
            os.system("mkdir -p %s" % s)

        os.system("rm -rf %s/top" % mount_dir)
        pass
    def test_pwd(self):
        pass
    def test_mv(self):
        # dir
        os.system("rm -rf %s/testdir_*" % mount_dir)

        for i in range(100):
            s = '%s/testdir_%d' % (mount_dir, i)
            print "*** test_mv: %s" % s

            os.system("mkdir %s" % s)
            assert os.path.isdir(s), "mv failed"

            # XXX
            os.system("rm -rf %s" % s)
            assert not os.path.isdir(s), "%s exists" % s

        # file

        pass
    def test_1_write(self):
        os.system("rm -f %s/test_*" % mount_dir)
        os.system("cp -f %s/test_* %s" % (input_dir, mount_dir))
        os.system("sleep 10")
        pass
    def test_2_read(self):
        os.system("cp -f %s/test_* %s/" % (mount_dir, output_dir))
        pass
    def test_3_md5(self):
        print 'MD5 ===================================='
        print '*** input ***'
        os.system("md5sum %s/*" % input_dir)
        print '*** output ***'
        os.system("md5sum %s/*" % output_dir)
        pass

def suite():
    suite = unittest.TestSuite()
    suite.addTest(NFS_test("test_mkdir"))
    suite.addTest(NFS_test("test_pwd"))
    suite.addTest(NFS_test("test_mv"))
    suite.addTest(NFS_test("test_1_write"))
    suite.addTest(NFS_test("test_2_read"))
    suite.addTest(NFS_test("test_3_md5"))
    return suite

if __name__ == "__main__":
    # prepare test environment
    os.system("pkill -9 yfs_mds")
    os.system("pkill -9 yfs_cds")
    os.system("pkill -9 ynfs_server")

    os.system("/sysy/yfs/app/sbin/yfs_mds -n 1")
    os.system("/sysy/yfs/app/sbin/yfs_cds -n 1 -m 1058426880")
    os.system("/sysy/yfs/app/sbin/yfs_cds -n 2 -m 1058426880")
    os.system("/sysy/yfs/app/sbin/ynfs_server")
    os.system("sleep 6")

    # prepare test dir
    if os.path.exists(input_dir):
        os.system("rm -rf %s" % input_dir)
    if os.path.exists(output_dir):
        os.system("rm -rf %s" % output_dir)

    os.mkdir(input_dir)
    os.mkdir(output_dir)

    # create test files
    k64 = 64 * 1024
    make_test_file(k64 - 1)
    make_test_file(k64)
    make_test_file(k64 + 1)

    m64 = 64 * 1024 * 1024
    make_test_file(m64 - 1)
    make_test_file(m64)
    make_test_file(m64 + 1)

    # 4k, 40k, 400k, 4m, 40m, 400m ...
    for i in range(4):
        make_test_file(4 * 1024 * 10 ** i)

    # run test
    unittest.main()
