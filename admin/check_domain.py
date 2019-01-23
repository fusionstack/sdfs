#!/usr/bin/env python
# -*- coding:utf-8 -*-

import errno
import os
import argparse
import sys

from utils import exec_shell, Exp
from config import Config

def get_mode(config):
    _exec_get_mode = "%s -g mode /system" % (config.uss_attr)

    try:
        out, err = exec_shell(_exec_get_mode, need_return=True)
    except Exp, e:
        errcode = e.errno
        if errcode != 126 and errcode != errno.ENOENT:
            raise Exp(errcode, "get mode attr failed by %s" % os.strerror(errcode))
        out = 'user'

    return out.strip('\n')

def connected_ad():
    _exec_wbinfo = "/usr/local/samba/bin/wbinfo -t"
    try:
        exec_shell(_exec_wbinfo)
        return True
    except Exp, e:
        return False

def enable_ad(common_script):
    _exec_ad_enable = "python %s ad --config enable" % (common_script)
    exec_shell(_exec_ad_enable)

def enable_ldap(common_script):
    _exec_ldap_enable = "python %s ldap --config enable" % (common_script)
    exec_shell(_exec_ldap_enable)

def enable_user(common_script):
    _exec_ad_disable = "python %s ad --config disable" % (common_script)
    exec_shell(_exec_ad_disable)

    _exec_ldap_disable = "python %s ldap --config disable" % (common_script)
    exec_shell(_exec_ldap_disable)

def check_and_join(config):
    mode = get_mode(config)
    common_script = os.path.join(config.home, "app/admin/cluster.py")

    if mode == 'ad':
        if not connected_ad():
            enable_ad(common_script)
    elif mode == 'ldap':
        enable_ldap(common_script)
    else:
        enable_user(common_script)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def _check(args):
        config = Config()
        check_and_join(config)
    parser_check = subparsers.add_parser('check', help='check if joined ad or ldap')
    parser_check.set_defaults(func=_check)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
