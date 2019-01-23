#!/usr/bin/python

import os
import sys
import time

__all__ = ['debug', 'logout', 'logerr', 'logdbg', 'daemonize']

debug = True

def logout(message):
    tm = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    sys.stdout.write('[%s] %s\n' % (tm, message))

def logdbg(message):
    if debug:
        tm = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
        sys.stdout.write('[%s] %s\n' % (tm, message))

def logerr(message):
    tm = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    sys.stderr.write('[%s] %s\n' % (tm, message))

def daemonize(pid_file, out_log, err_log):
    sys.stdout.flush()
    sys.stderr.flush()

    try:
        os.open(pid_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except OSError, e:
        sys.stderr.write('Is another daemon running?, stop it first.\n')
        sys.exit(1)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork daemon #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    os.chdir('/')
    os.umask(0)
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork daemon #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    pid = str(os.getpid())
    open(pid_file, 'w').write(pid)

    si = open('/dev/null', 'r')
    so = open(out_log, 'a+', 1024)
    se = open(err_log, 'a+')

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())



