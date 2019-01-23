#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys
import uuid
import errno
import logging
import argparse
from argparse import RawTextHelpFormatter
from socket import error as socket_error
from ftplib import FTP, error_temp, error_perm, error_reply, all_errors

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../../admin')
sys.path.insert(0, admin)
from utils import exec_shell, Exp
from config import Config

logging.basicConfig(level=logging.DEBUG, format='%(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

DEFAULT_FTP_SHARE='/ftp'
DEFAULT_FTP_LOCAL='/mnt/ftp'
DEFAULT_USER='ftp'
DEFAULT_GROUP='ftp'
DEFAULT_PASSWORD='password'

def connect(host='127.0.0.1', user=None, password=None):
    if user is None or password is None:
        logging.error('must provide username and password')
        sys.exit(errno.EPERM)

    try:
        ftp = FTP(host)
        ftp.login(user, password)
    except (error_temp, error_perm, socket_error):
        logging.error('connect or login failed')
        sys.exit(errno.EPERM)

    return ftp

def upload(ftp, localpath, remotepath):
    abs_local = os.path.join(DEFAULT_FTP_LOCAL, localpath)
    fp = open(abs_local, 'rb')

    testdir = os.path.join(DEFAULT_FTP_SHARE, 'testdir')
    abs_remote = os.path.join(testdir, remotepath)

    try:
        ftp.storbinary('STOR ' + abs_remote, fp)
    except (all_errors):
        logging.error('upload %s failed' % (abs_remote))
        sys.exit(-1)

def download(ftp, localpath, remotepath):
    abs_local = os.path.join(DEFAULT_FTP_LOCAL, localpath)
    fp = open(abs_local, 'wb').write

    testdir = os.path.join(DEFAULT_FTP_SHARE, 'testdir')
    abs_remote = os.path.join(testdir, remotepath)

    try:
        ftp.retrbinary('RETR ' + abs_remote, fp)
    except (all_errors):
        logging.error('download %s failed' % (abs_remote))
        sys.exit(-1)

def rename(ftp, fromname, toname):
    abs_from = os.path.join(DEFAULT_FTP_SHARE, fromname)
    abs_to = os.path.join(DEFAULT_FTP_SHARE, toname)
    try:
        ftp.rename(abs_from, abs_to)
    except (all_errors):
        logging.error('rename from %s to %s failed' %
                        (abs_from, abs_to))
        sys.exit(-1)

def delete(ftp, path):
    abspath = os.path.join(DEFAULT_FTP_SHARE, path)
    try:
        ftp.delete(abspath)
    except (all_errors):
        logging.error('delete %s failed' % (abspath))
        sys.exit(-1)

#  在ftp server端共享目录下创建目录
def mkdir(ftp, path):
    abspath = os.path.join(DEFAULT_FTP_SHARE, path)
    try:
        ftp.mkd(abspath)
    except (all_errors):
        logging.error('mkdir %s failed' % (abspath))
        sys.exit(-1)

def rmdir(ftp, path):
    abspath = os.path.join(DEFAULT_FTP_SHARE, path)
    try:
        ftp.rmd(abspath)
    except (all_errors):
        logging.error('rmdir %s failed' % abspath)
        sys.exit(-1)

#  递归创建8级子目录
def recusive_mkdir(ftp):
    prefix = 'dir-level'
    abspath = ''
    for level in range(8):
        path = '%s-%d' % (prefix, level)
        abspath = os.path.join(abspath, path)
        mkdir(ftp, abspath)

def ftp_test(host, user, password):
    ftp = connect(host, user, password)
    logging.info('connected to %s' % host)

    #  在共享目录下创建目录testdir
    mkdir(ftp, 'testdir')
    logging.info('create remote directory testdir')

    #  在共享目录下创建至少8级子目录
    recusive_mkdir(ftp)
    logging.info('recusive create 8 level directory')

    #  将子目录更名为testdir1
    rename(ftp, 'testdir', 'testdir1')
    logging.info('rename from testdir to testdir1')

    #  删除子目录testdir1
    rmdir(ftp, 'testdir1')
    logging.info('delete directory testdir1')

    #  在本地临时目录/mnt/tmp中创建文件file
    filename = os.path.join(DEFAULT_FTP_LOCAL, 'origin-file')
    try:
        os.stat(filename)
        res = 0
    except Exception as e:
        res = e.errno
        if res != errno.ENOENT:
            sys.exit(res)

    if res == errno.ENOENT:
        os.mknod(filename)
        logging.info('create local file : %s/%s' % (DEFAULT_FTP_LOCAL, 'origin-file'))
    else:
        logging.info('local file already exist')

    #  在共享目录下创建目录testdir
    mkdir(ftp, 'testdir')
    logging.info('create remote directory testdir')

    #  将主机上的文件file上传至testdir
    upload(ftp, 'origin-file', 'remote-file')
    logging.info('upload local file to remote')

    #  删除主机上的文件file
    os.unlink(filename)
    logging.info('delete local file')

    #  将testdir下的file下载至主机
    download(ftp, 'local-file', 'remote-file')
    logging.info('download remote file')

    #  删除testdir下的file
    delete(ftp, 'testdir/remote-file')
    logging.info('delete remote file')

    filename = os.path.join(DEFAULT_FTP_LOCAL, 'local-file')
    os.unlink(filename)

    os.rmdir(DEFAULT_FTP_LOCAL)

def create_ftp_tmp(tmp):
    try:
        os.stat(tmp)
        res = 0
    except Exception as e:
        res = e.errno
        if res != errno.ENOENT:
            sys.exit(res)

    if res == errno.ENOENT:
        os.mkdir(tmp)
    #  保证本地存在/mnt/ftp目录

def create_group(config, groupname):
    _exec = '%s -s -g 5000 %s' % (config.uss_group, groupname)

    try:
        exec_shell(_exec, timeout=10)
    except Exp, e:
        logging.error('errno:%d, error:%s' %
                        (e.errno, str(e)))
        sys.exit(-1)

def create_ftp_user(config, username, password):
    _exec = '%s -s -g 5000 -u 5000 -p %s %s' % \
                    (config.uss_user, password, username)
    try:
        exec_shell(_exec, timeout=10)
    except Exp, e:
        logging.error('errno:%d, error:%s' %
                        (e.errno, str(e)))
        sys.exit(-1)

def create_ftp_share(home, share_dir):
    mkdir = os.path.join(home, 'app/bin/sdfs.mkdir')
    os.system(mkdir + " /ftp")
    
    share_path = os.path.join(home, 'app/bin/sdfs.share')
    _exec = '%s -s -n ftpshare -u %s -m rw -p ftp %s' % \
                    (share_path, DEFAULT_USER, share_dir)
    try:
        exec_shell(_exec, timeout=10)
    except Exp, e:
        logging.error('errno:%d, error:%s' %
                        (e.errno, str(e)))
        sys.exit(-1)

def prepare_ftp(config):
    #  创建本地临时目录
    create_ftp_tmp(DEFAULT_FTP_LOCAL)
    logging.info('create local directory : %s' % DEFAULT_FTP_LOCAL)

    #  创建用户之前首先需要创建group
    create_group(config, DEFAULT_GROUP)
    logging.info('create group : %s' % DEFAULT_GROUP)

    #  创建临时用户
    create_ftp_user(config, DEFAULT_USER, DEFAULT_PASSWORD)
    logging.info('create user : %s, password : %s' % (DEFAULT_USER, DEFAULT_PASSWORD))

    #  创建共享目录
    create_ftp_share(config.home, DEFAULT_FTP_SHARE)
    logging.info('create share directory : %s' % DEFAULT_FTP_SHARE)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument("--home", required=True, help="specify project home directory")

    args = parser.parse_args()
    home = args.home

    config = Config(home)
    prepare_ftp(config)
    ftp_test('127.0.0.1', DEFAULT_USER, DEFAULT_PASSWORD)
    logging.info('!!!ftp test succ!!!')
