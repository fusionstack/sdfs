#!/usr/bin/env python2.7

import commands
import argparse
import fcntl
import os
import re
import sys
import string
import struct
import socket
import time
import thread
from threading import Thread

NUM = 0
numlock = thread.allocate_lock()

class testping(Thread):
   def __init__ (self, ip):
      Thread.__init__(self)
      self.ip = ip
      self.status = -1

   def run(self):
        global NUM

        cmd = 'ping -c 2 -i 0.1 -w 2 ' + self.ip
        (status, output) = commands.getstatusoutput(cmd)
        if status == 0:
                numlock.acquire()
                NUM += 1
                numlock.release()

def log_append(message, home):
    log = '%s/log/fence_test.log' % (home)
    cmd = "mkdir -p %s" % (os.path.dirname(log))
    os.system(cmd)

    with open(log, 'a') as f:
        f.write('[%s %s] %s\n' % (time.time(), time.strftime('%F %T'), message))

def do_ping(home):
    cluster_conf = os.path.join(home, "etc/cluster.conf")
    iplist = []
    with open(cluster_conf) as f:
        for l in f.readlines():
            iplist.append(l.strip().split()[0])

    limit = len(iplist) / 2 + 1
    log_append('==> %s, limit %d' % (iplist, limit), home)
    
    pinglist = []
    for ip in iplist:
        current = testping(ip)
        pinglist.append(current)
        current.start()

    for p in pinglist:
        p.join()

    log_append('<== passed/total: %d/%d' % (NUM, len(iplist)), home)
    if NUM >= limit or limit == 1:
        sys.exit(0)
    else:
        sys.exit(64)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--home", action="store", dest='home', required=True, help="home")
    args = parser.parse_args()
    do_ping(args.home)
