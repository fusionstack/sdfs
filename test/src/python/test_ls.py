#!/usr/bin/env python

import os, sys
import subprocess
import signal
import time
import paramiko
import platform
import threading
import errno

TEST_DIR = "/maketest"

class Exp(Exception):
    def __init__(self, errno, err, out = None):
        self.errno = errno
        self.err = err
        self.out = out

    def __str__(self):
        exp_info = 'errno:%s, err:%s'%(self.errno, self.err)
        if self.out is not None:
            exp_info += ' stdout:' + self.out
        return repr(exp_info)

def exec_shell(cmd, retry=3, p=True, need_return=False,
               shell=True, timeout=0):
    if (p):
        print cmd

    _retry = 0
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    while (1):
        try:
            if need_return:
                p = subprocess.Popen(cmd, shell=shell,
                                     stdout=subprocess.PIPE,
                                     stdin=subprocess.PIPE,
                                     stderr=subprocess.PIPE, env=env)
            else:
                p = subprocess.Popen(cmd, shell=shell, env=env)

            if timeout != 0:
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(timeout)

            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                return (stdout, stderr)

            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                raise Exp(ret, "%s: %s" % (cmd, os.strerror(ret)))
        except KeyboardInterrupt as err:
            p.kill()
            exit(errno.EINTR)

def ls_prep():
    try:
        cmd = "sdfs.mkdir %s" % (TEST_DIR)
        exec_shell(cmd)
    except Exception as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise Exp(e.errno, "%s: %s" % (cmd, os.strerror(e.errno)))

    for i in range(10):
        try:
            cmd = "uss.touch %s/%d.c" % (TEST_DIR, i)
            exec_shell(cmd)
        except Exception as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise Exp(e.errno, "%s: %s" % (cmd, os.strerror(e.errno)))

        try:
            cmd = "uss.touch %s/%d.h" % (TEST_DIR, i)
            exec_shell(cmd)
        except Exception as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise Exp(e.errno, "%s: %s" % (cmd, os.strerror(e.errno)))

def ls_test():
    cmd = "uss.ls %s/*.c" % (TEST_DIR)
    out, err = exec_shell(cmd, p=True, need_return=True)
    for line in out.split('\n'):
        print line
        if len(line) == 0:
            continue
        if line[-1] == 'c' and line[-2] == '.':
            pass
        else:
            raise Exp(1, "uss.ls test fail\n")

    cmd = "uss.ls %s/*.h" % (TEST_DIR)
    out, err = exec_shell(cmd, p=True, need_return=True)
    for line in out.split('\n'):
        print line
        if len(line) == 0:
            continue
        if line[-1] == 'h' and line[-2] == '.':
            pass
        else:
            raise Exp(1, "uss.ls test fail\n")

def ls_clean():
    for i in range(10):
        cmd = "uss.rm %s/%d.c" % (TEST_DIR, i)
        exec_shell(cmd)

        cmd = "uss.rm %s/%d.h" % (TEST_DIR, i)
        exec_shell(cmd)

    cmd = "uss.rmdir %s" % (TEST_DIR)
    exec_shell(cmd)

def main():
    ls_prep()
    ls_test()
    ls_clean()
    print "ls test ok."

if __name__ == '__main__':
    main()
