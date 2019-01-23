#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import argparse
from utils import  Exp, exec_shell, dmsg, dwarn

#dev:/dev/sda
def is_ssd(dev):
    if len(dev) == 0:
        dwarn("invalid device name")
        return False
    d_name = dev.split('/')[-1]
    if len(d_name) == 0:
        dwarn("invalid device name")
        return False
    dst_path = "/sys/block/%s/queue/rotational" % d_name
    _exec_cat = "cat %s" % dst_path
    try:
        val, _ = exec_shell(_exec_cat, need_return=True)
        if val == '0':
            return True
        else:
            return False
    except Exp, err:
        dwarn("operation failed")
        return False

def construct_argument(alist):
    astr = ""
    for dev in alist:
        astr = astr + dev + ' '
    return astr

#  To make bcache devices known to the kernel, echo them to /sys/fs/bcache/register
def register(slist, hlist):
    for ssd in slist:
        _exec_echo = "echo %s > /sys/fs/bcache/register" % ssd
        try:
            exec_shell(_exec_echo)
        except Exp, err:
            dwarn('%s register failed' % (ssd))
            return False

    for hdd in hlist:
        _exec_echo = "echo %s > /sys/fs/bcache/register" % hdd
        try:
            exec_shell(_exec_echo)
        except Exp, err:
            dwarn('%s register failed' % (hdd))
            return False

    return True


#--ssd /dev/sda,/dev/sdb
#--hdd /dev/sda,/dev/sdb
#  make-bcache has the ability to format multiple devices at the same time - if
#  you format your backing devices and cache device at the same time, you won't
#  have to manually attach:
#  make-bcache -B /dev/sda /dev/sdb -C /dev/sdc
def _init(args):
    ssd_list = args.ssd.split(',')
    hdd_list = args.hdd.split(',')

    #  Cache devices are managed as sets; multiple caches per set isn't supported yet
    #  but will allow for mirroring of metadata and dirty data in the future. Your new
    #  cache set shows up as /sys/fs/bcache/<UUID>
    if len(ssd_list) > 1:
        dwarn("support only one ssd device")
        return False

    ssd_str = construct_argument(ssd_list)
    hdd_str = construct_argument(hdd_list)

    #cache device is a ssd
    if not is_ssd(ssd_str.strip()):
        return False

    _exec_init = "make-bcache -B %s -C %s" % (hdd_str, ssd_str)
    try:
        exec_shell(_exec_init)
    except Exp, err:
        return False

    if not register(ssd_list, hdd_list):
        return False

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_format = subparsers.add_parser('init', help='format devices and attach backing devices to a cache set')
    parser_format.add_argument('--ssd', required=True, help='ssd device')
    parser_format.add_argument('--hdd', required=True, help='hdd device')
    parser_format.set_defaults(func=_init)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
