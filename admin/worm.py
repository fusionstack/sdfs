#!/usr/bin/env python2
#-*- coding: utf-8 -*-
import os
import time

from utils import exec_shell
from node import Node

def worm_set():
    cmd = "uss.ls / | awk '{print $9}'"
    lvm_list, err = exec_shell(cmd, p=True, need_return=True)
    for lvm in str(lvm_list).split('\n'):
        if lvm != "":
            try:
                cmd = "uss.attr -g worm1 /%s" % (lvm)
                outmsg, errmsg = exec_shell(cmd, p=False, need_return=True)
                print "begin worm check /%s" % lvm

                worm_start_time = int(float(str(outmsg).strip().split(":")[0]))
                worm_protect_time = int(float(str(outmsg).strip().split(":")[1]))
                now = int(time.time())

                if now > worm_start_time:
                    cmd = "uss.attr -g worm /WORM_CLOCK"
                    worm_clock_time, errmsg = exec_shell(cmd, need_return=True)
                    worm_end_time = int(str(worm_clock_time).strip()) + worm_protect_time
                    cmd = "uss.attr -s worm -V %d /%s && uss.attr -r worm1 /%s" % (worm_end_time, lvm, lvm)
                    exec_shell(cmd)

                    #restart nfs samba
                    cmd = "uss-multi-exec -c 'systemctl restart nfs-ganesha; systemctl restart uss_samba'"
                    exec_shell(cmd)
            except Exception as e:
                print e
                pass

if __name__ == "__main__":
    node = Node()
    if node.is_master():
        worm_set()
    else:
        print "not master"

