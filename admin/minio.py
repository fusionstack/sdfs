#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

"""
Author : jiangyang
Date : 2017.03.08
Description : start or stop minio services in FusionNAS
"""

import os
import sys
import errno
import syslog
import argparse

#from argparse import RawTextHelpFormatter

from config import Config
from utils import exec_shell, Exp, dmsg, dwarn,\
                json_load, json_store, check_process_exists

ERROR_FAILED = -1
ERROR_SUCCESS = 0

MINIO_LOG_ERR = syslog.LOG_ERR
MINIO_LOG_INFO = syslog.LOG_INFO

MINIO_NAME = 'minio'
MINIO_LINK = '/opt/minio/lminio'
MINIO_PATH = '/opt/minio/nfs_minio'
NFS_MINIO_PATH = '/nfs_minio'
MINIO_CONF_PATH = '/opt/minio/config'

def _minio_mkdir(path_name, mode=0755):
    try:
        os.mkdir(path_name, mode)
    except OSError, e:
        if (e.errno != errno.EEXIST):
           minio_log = "mkdir %s failed, %s." % (path_name, os.strerror(e.errno))
           #redis_log(MINIO_LOG_ERR, MINIO_NAME, minio_log)
           return ERROR_FAILED

    return ERROR_SUCCESS

def _minio_cmd(cmd):
    try:
        exec_shell(cmd)
        return ERROR_SUCCESS
    except Exp, e:
        minio_log = "%s failed, %s." % (cmd, os.strerror(e.errno))
        #redis_log(MINIO_LOG_ERR, MINIO_NAME, minio_log)
        return e.errno

class Minio:
    def __init__(self):
        #redis_openlog()
        pass

    def start(self):

        #redis_log(MINIO_LOG_INFO, MINIO_NAME, "minio is start...")

        #create minio dir
        if (ERROR_FAILED == _minio_mkdir(MINIO_PATH, 0755)):
            return ERROR_FAILED

        #check and create nfs dir
        config = Config()
        cmd = "%s %s" % (config.uss_stat, NFS_MINIO_PATH)
        if (ERROR_SUCCESS != _minio_cmd(cmd)):
            cmd = "%s %s" % (config.uss_mkdir, NFS_MINIO_PATH)
            ret = _minio_cmd(cmd)
            if ((ERROR_SUCCESS != ret) and (errno.EEXIST != ret)):
                return ERROR_FAILED

        #mount minio dir by nfs, mount failed maybe already mount, so only send log
        cmd = "df -h | grep nfs_minio >/dev/null || mount -t nfs -o vers=3,nolock 127.0.0.1:%s %s" % \
              (NFS_MINIO_PATH, MINIO_PATH)
        _minio_cmd(cmd)

        #create soft link /opt/minio/lminio-->/opt/minio/nfs_minio/minio
        minio_path = os.path.join(MINIO_PATH, MINIO_NAME)
        if (ERROR_FAILED == _minio_mkdir(minio_path, 0755)):
            return ERROR_FAILED

        try:
            os.symlink(minio_path, MINIO_LINK)
        except OSError, e:
            if (e.errno != errno.EEXIST):
                minio_log = "%s link to %s failed, %s." % \
                            (MINIO_LINK, minio_path, os.strerror(e.errno))
                #redis_log(MINIO_LOG_ERR, MINIO_NAME, minio_log)
                return ERROR_FAILED

        #start minio server by systemctl command
        cmd = "minioctl tenant_start --home %s" % (MINIO_PATH)
        if (ERROR_FAILED == _minio_cmd(cmd)):
            return ERROR_FAILED

        #redis_log(MINIO_LOG_INFO, MINIO_NAME, "minio start success")
        return ERROR_SUCCESS

    def stop(self):
        #redis_log(MINIO_LOG_INFO, MINIO_NAME, "minio is stopping...")

        try:
            os.unlink(MINIO_LINK)
        except OSError, e:
            minio_log = "unlink %s failed, %s." % \
                        (MINIO_LINK, os.strerror(e.errno))
            #redis_log(MINIO_LOG_ERR, MINIO_NAME, minio_log)

        cmd = "umount -l %s" % MINIO_PATH
        if (ERROR_FAILED == _minio_cmd(cmd)):
            return ERROR_FAILED

        #stop minio server
        cmd = "minioctl tenant_stop"
        if (ERROR_FAILED == _minio_cmd(cmd)):
            return ERROR_FAILED

        #redis_log(MINIO_LOG_INFO, MINIO_NAME, "minio stop finish.")

        return ERROR_SUCCESS


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def _start(args, minio):
        minio.start()
    parser_start = subparsers.add_parser('start', help='start minio service')
    parser_start.set_defaults(func=_start)

    def _stop(args, minio):
        minio.stop()
    parser_stop = subparsers.add_parser('stop', help='stop minio service')
    parser_stop.set_defaults(func=_stop)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(-1)

    minio = Minio()
    args = parser.parse_args()
    args.func(args, minio)

