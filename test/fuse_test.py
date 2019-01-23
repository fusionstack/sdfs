#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys
import subprocess
import time
admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../admin')
sys.path.insert(0, admin)

import argparse
from argparse import RawTextHelpFormatter

from utils import  Exp, dwarn
from config import Config

VERSION_2_9=2
VERSION_3_0=3

def test_exec(cmd):
    p = subprocess.Popen(cmd, shell=True)
    try:
        ret = p.wait()
        stdout, stderr = p.communicate()
        ret = p.returncode
        if (ret == 0):
            return
        else:
            msg = ""
            msg = msg + "cmd: " + cmd
            msg = msg + "\nstdout: " + str(stdout)
            msg = msg + "\nstderr: " + str(stderr)
            raise Exp(ret, msg)
    except KeyboardInterrupt as err:
        dwarn("interupted")
        p.kill()
        exit(errno.EINTR)

def yfuse_test(version, home):
    config = Config(home)
    slp = 0

    cmd_line="fusermount -u /tmp/fuse/"

    try:
        test_exec(cmd_line)
    except Exception as e:
        pass

    test_exec('mkdir -p /tmp/fuse')
    test_exec('sdfs.mkdir /yfuse')
    if version == VERSION_2_9:
        cmd_line="%s --dir=/yfuse --service=1 /tmp/fuse" % (config.uss_fuse)
    elif version == VERSION_3_0:
        cmd_line="%s --dir=/yfuse --service=1 /tmp/fuse" % (config.uss_fuse3)
    else:
        print "unsupported fuse version"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="touch /tmp/fuse/hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="echo helloworld >> /tmp/fuse/hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="cat /tmp/fuse/hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="mkdir /tmp/fuse/testdir"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="cp /tmp/fuse/hello /tmp/fuse/testdir"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="rm -rf /tmp/fuse/testdir/hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="rmdir /tmp/fuse/testdir"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="mv /tmp/fuse/hello /tmp/fuse/hello_back"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="ln /tmp/fuse/hello_back /tmp/fuse/hard_hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="ln -s /tmp/fuse/hello_back /tmp/fuse/soft_hello"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="chmod +x /tmp/fuse/hello_back"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    cmd_line="rm -rf /tmp/fuse/*"
    print cmd_line
    test_exec(cmd_line)
    time.sleep(slp)

    if version == VERSION_2_9:
        cmd_line="fusermount -u /tmp/fuse/"
    elif version == VERSION_3_0:
        cmd_line="fusermount3 -u /tmp/fuse/"
    else:
        print "unsupported fuse version"

    print cmd_line
    retry = 3
    while (retry < 3):
        try:
            test_exec(cmd_line)
            break
        except Exp, e:
            #todo umount 会出错
            dwarn("retry %s %s" % (cmd_line, e))
            retry = retry - 1

    time.sleep(slp)

    test_exec('rm -rf /tmp/fuse')
    test_exec('sdfs.rmdir /yfuse')
    print "!!!fuse_test success!!!"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument("--home", required=True, help="")

    args = parser.parse_args()
    home = args.home

    print "start testing fuse_2_9"
    yfuse_test(VERSION_2_9, home)
    print "start testing fuse_3_0"
    yfuse_test(VERSION_3_0, home)
