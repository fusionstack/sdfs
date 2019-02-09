#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys, stat
import uuid
import errno
import logging
import time
import argparse
from argparse import RawTextHelpFormatter

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../../admin')
sys.path.insert(0, admin)
from utils import exec_shell, Exp

logging.basicConfig(level=logging.DEBUG, format='%(filename)s[line:%(lineno)d] %(message)s')

def get_stat_info(filename):
    statinfo = os.stat(filename)
    return statinfo

def get_lstat_info(filename):
    statinfo = os.lstat(filename)
    return statinfo

def get_file_mode(filename):
    m = os.stat(filename).st_mode
    mode = oct(stat.S_IMODE(m))
    return mode

def get_file_uid(filename):
    return os.stat(filename).st_uid

def get_file_gid(filename):
    return os.stat(filename).st_gid

def t_chmod(mount_point):
    f_uuid = str(uuid.uuid1())
    d_uuid = str(uuid.uuid1())
    fn = os.path.join(mount_point, f_uuid)
    dn = os.path.join(mount_point, d_uuid)

    #  文件
    os.mknod(fn, 0644)
    mode = get_file_mode(fn)
    if mode != '0644':
        logging.error('t_chmod failed, filename : %s mode : %s\n' %
                        (fn, mode))
        sys.exit(-1)
    os.chmod(fn, 0111)
    mode = get_file_mode(fn)
    if mode != '0111':
        logging.error('t_chmod failed, mode : %s\n' % (mode))
        sys.exit(-1)

    os.unlink(fn)

    #  目录
    os.mkdir(dn, 0755)
    mode = get_file_mode(dn)
    if mode != '0755':
        logging.error('t_chmod failed, mode : %s\n' % (mode))
        sys.exit(-1)
    os.chmod(dn, 0753)
    mode = get_file_mode(dn)
    if mode != '0753':
        logging.error('t_chmod failed, mode : %s\n' % (mode))
        sys.exit(-1)
    os.rmdir(dn)

def t_chown(mount_point):
    f_uuid = str(uuid.uuid1())
    d_uuid = str(uuid.uuid1())
    fn = os.path.join(mount_point, f_uuid)
    dn = os.path.join(mount_point, d_uuid)

    #  文件
    os.mknod(fn, 0644)
    #  uid:124, gid:456
    os.chown(fn, 123, 456)
    uid = get_file_uid(fn)
    gid = get_file_gid(fn)
    if uid != 123 or gid != 456:
        logging.error('t_chown failed, uid:%d, gid:%d\n' %
                    (uid, gid))
        sys.exit(-1)
    os.unlink(fn)

    #  目录
    os.mkdir(dn, 0755)
    os.chown(dn, 123, 456)
    uid = get_file_uid(dn)
    gid = get_file_gid(dn)
    if uid != 123 or gid != 456:
        logging.error('t_chown failed, uid:%d, gid:%d\n' %
                    (uid, gid))
        sys.exit(-1)
    os.rmdir(dn)

def t_link(mount_point):
    f1_uuid = str(uuid.uuid1())
    f2_uuid = str(uuid.uuid1())
    f1n = os.path.join(mount_point, f1_uuid)
    f2n = os.path.join(mount_point, f2_uuid)

    #  文件
    try:
        os.mknod(f1n, 0644)
    except Exception as e:
        logging.error('t_link failed, mknod failed, filename : %s\n' %
                        (f1n))
        sys.exit(-1)
    statinfo = get_stat_info(f1n)
    #  type
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_link failed, not regular file\n')
        sys.exit(-1)
    #  mode
    mode = get_file_mode(f1n)
    if mode != '0644':
        logging.error('t_link failed, mode : %s\n' % (mode))
        sys.exit(-1)
    #  nlink
    if statinfo.st_nlink != 1:
        logging.error('t_link failed, nlink : %d\n' % (statinfo.st_nlink))
        sys.exit(-1)

    try:
        os.link(f1n, f2n)
    except Exception as e:
        logging.error('t_link failed, src : %s, dest : %s\n' %
                        (f1n, f2n))
        sys.exit(-1)
    statinfo = get_stat_info(f1n)
    #  src file info
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_link failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(f1n)
    if mode != '0644':
        logging.error('t_link failed, mode : %s\n' % (mode))
        sys.exit(-1)
    if statinfo.st_nlink != 2:
        logging.error('t_link failed, nlink : %d\n' % (statinfo.st_nlink))
        sys.exit(-1)

    statinfo = get_stat_info(f2n)
    #  dest file info
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_link failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(f2n)
    if mode != '0644':
        logging.error('t_link failed, mode : %s\n' % (mode))
        sys.exit(-1)
    if statinfo.st_nlink != 2:
        logging.error('t_link failed, nlink : %d\n' % (statinfo.st_nlink))
        sys.exit(-1)

    #  删除源文件
    os.unlink(f1n)
    #  源文件是否成功删除
    try:
        os.stat(f1n)
        res = 0
    except Exception as e:
        res = e.errno
    if res != errno.ENOENT:
        logging.error('t_link failed, unlink failed\n')
        sys.exit(-1)

    statinfo = get_stat_info(f2n)
    #  目的文件nlink是否减为1
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_link failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(f2n)
    if mode != '0644':
        logging.error('t_link failed, mode : %s\n' % mode)
        sys.exit(-1)
    #  file_refresh默认为10
    if statinfo.st_nlink != 1:
        logging.error('t_link failed, filename : %s, nlink : %d\n' % (f2n, statinfo.st_nlink))
        #sys.exit(-1)
    os.unlink(f2n)

def t_mkdir(mount_point):
    uid = str(uuid.uuid1())
    dn = os.path.join(mount_point, uid)

    os.mkdir(dn, 0755)
    statinfo = get_stat_info(dn)
    if not stat.S_ISDIR(statinfo.st_mode):
        logging.error('t_mkdir failed, not directory\n')
        sys.exit(-1)
    mode = get_file_mode(dn)
    if mode != '0755':
        logging.error('t_mkdir failed, mode : %s\n' % (mode))
        sys.exit(-1)
    os.rmdir(dn)

def t_open(mount_point):
    uid = str(uuid.uuid1())
    fn = os.path.join(mount_point, uid)

    try:
        fd = os.open(fn, os.O_CREAT|os.O_WRONLY, 0755)
    except Exception as e:
        logging.error("errno : %d, error : %s\n" % (e.errno, str(e)))
        sys.exit(e.errno)
    statinfo = get_stat_info(fn)
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_open failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(fn)
    if mode != '0755':
        logging.error('t_open failed, mode : %s\n' % (mode))
        sys.exit(-1)

    os.close(fd)
    os.unlink(fn)

def t_rename(mount_point):
    s_uuid = str(uuid.uuid1())
    d_uuid = str(uuid.uuid1())
    s_fn = os.path.join(mount_point, s_uuid)
    d_fn = os.path.join(mount_point, d_uuid)

    #  src file
    os.mknod(s_fn, 0644)
    statinfo = get_stat_info(s_fn)
    s_inode = statinfo.st_ino
    #  type
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_rename failed, not regular file\n')
        sys.exit(-1)
    #  mode
    mode = get_file_mode(s_fn)
    if mode != '0644':
        logging.error('t_rename failed, mode : %s\n' % (mode))
        sys.exit(-1)
    #  nlink
    if statinfo.st_nlink != 1:
        logging.error('t_rename failed, nlink : %d\n' % (statinfo.st_nlink))
        sys.exit(-1)

    os.rename(s_fn, d_fn)
    try:
        os.stat(s_fn)
        res = 0
    except Exception as e:
        res = e.errno
    if res != errno.ENOENT:
        logging.error('t_rename failed\n')
        sys.exit(-1)

    statinfo = get_stat_info(d_fn)
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_rename failed, not regular file\n')
        sys.exit(-1)
    d_inode = statinfo.st_ino
    if s_inode != d_inode:
        logging.error('t_rename failed, invalid inode\n')
        sys.exit(-1)
    mode = get_file_mode(d_fn)
    if mode != '0644':
        logging.error('t_rename failed, mode : %s\n' % (mode))
        sys.exit(-1)
    if statinfo.st_nlink != 1:
        logging.error('t_rename failed, nlink : %d\n' % (statinfo.st_nlink))
        sys.exit(-1)
    os.unlink(d_fn)

def t_rmdir(mount_point):
    uid = str(uuid.uuid1())
    dn = os.path.join(mount_point, uid)

    os.mkdir(dn, 0755)
    statinfo = get_stat_info(dn)
    if not stat.S_ISDIR(statinfo.st_mode):
        logging.error('t_rmdir failed, not directory\n')
        sys.exit(-1)
    try:
        os.rmdir(dn)
    except Exception as e:
        logging.error('t_rmdir failed, errno : %d, error : %s\n' % (e.errno, str(e)))
        sys.exit(-1)
    try:
        os.stat(dn)
        res = 0
    except Exception as e:
        res = e.errno
    if res != errno.ENOENT:
        logging.error('t_rmdir failed, errno : %d\n' % (res))
        sys.exit(-1)

def t_symlink(mount_point):
    s_uuid = str(uuid.uuid1())
    d_uuid = str(uuid.uuid1())
    s_fn = os.path.join(mount_point, s_uuid)
    d_fn = os.path.join(mount_point, d_uuid)

    os.mknod(s_fn, 0644)
    statinfo = get_stat_info(s_fn)
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_symlink failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(s_fn)
    if mode != '0644':
        logging.error('t_symlink failed, mode : %s\n' % (mode))
        sys.exit(-1)
    try:
        os.symlink(s_fn, d_fn)
    except Exception as e:
        logging.error('t_symlink failed, errno : %d, error : %s\n' %
                        (e.errno, str(e)))
        sys.exit(-1)
    #  lstat获取软连接本身的属性
    lstatinfo = get_lstat_info(d_fn)
    if not stat.S_ISLNK(lstatinfo.st_mode):
        logging.error('t_symlink failed\n')
        sys.exit(-1)
    #  stat获取软连接指向的文件的属性
    statinfo = get_stat_info(d_fn)
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_symlink failed, not regular file\n')
        sys.exit(-1)
    mode = get_file_mode(d_fn)
    if mode != '0644':
        logging.error('t_symlink failed, mode : %s\n' % (mode))
        sys.exit(-1)
    os.unlink(s_fn)

    #  此时stat软连接文件，应报ENOENT
    try:
        os.stat(d_fn)
        res = 0
    except Exception as e:
        res = e.errno
    if res != errno.ENOENT:
        logging.error('t_symlink failed\n')
    os.unlink(d_fn)

def t_truncate(mount_point):
    uid = str(uuid.uuid1())
    fn = os.path.join(mount_point, uid)

    try:
        fd = open(fn, 'w')
    except Exception as e:
        logging.error('t_truncate failed, open failed, error : %s\n' %
                        (str(e)))
        sys.exit(-1)

    try:
        fd.truncate(1234567)
    except Exception as e:
        logging.error('t_truncate failed\n')
        sys.exit(-1)
    statinfo = get_stat_info(fn)
    if statinfo.st_size != 1234567:
        logging.error('t_truncate failed, invalid size\n')
        sys.exit(-1)

    fd.close()
    os.unlink(fn)

def t_write(mount_point, uid):
    fn = os.path.join(mount_point, uid)

    try:
        fd = open(fn, 'w')
    except Exception as e:
        logging.error('t_write failed, open failed, error : %s\n' %
                        (str(e)))
        sys.exit(-1)

    try:
        fd.write(uid)
    except Exception as e:
        logging.error('t_write failed\n')
        sys.exit(-1)

    fd.close()
    
def t_read(mount_point, uid):
    fn = os.path.join(mount_point, uid)

    try:
        fd = open(fn, 'r')
    except Exception as e:
        logging.error('t_write failed, open failed, error : %s\n' %
                        (str(e)))
        sys.exit(-1)

    try:
        res = fd.read()
        if (res != uid):
            logging.error('t_read cmp %s failed, need %s, got %s\n' % (fn, res, uid))
            sys.exit(-1)
    except Exception as e:
        logging.error('t_read %s failed\n' % (fn))
        sys.exit(-1)

    fd.close()

def t_io(mount_point, remount):
    lst = []
    for i in range(10):
        lst.append(str(uuid.uuid1()))

    for i in lst:
        t_write(mount_point, i)

    print(remount)
    os.system(remount)
        
    for i in lst:
        t_read(mount_point, i)

    for i in lst:
        fn = os.path.join(mount_point, i)
        os.unlink(fn)
        
def t_unlink(mount_point):
    uid = str(uuid.uuid1())
    fn = os.path.join(mount_point, uid)

    os.mknod(fn, 0644)
    statinfo = get_stat_info(fn)
    if not stat.S_ISREG(statinfo.st_mode):
        logging.error('t_unlink failed, not regular file\n')
        sys.exit(-1)
    os.unlink(fn)

    try:
        os.stat(fn)
        res = 0
    except Exception as e:
        res = e.errno
    if res != errno.ENOENT:
        logging.error('t_unlink failed\n')
        sys.exit(-1)

def nfs_test(mount_point='/mnt/nfs'):
    cmd = "sdfs.mkdir /nfs_test"
    os.system(cmd)
    cmd = "sdfs share -p nfs -m rw -n nfs_test -s /nfs_test -H 0.0.0.0"
    os.system(cmd)
    cmd = "mkdir -p " + mount_point
    os.system(cmd)
    cmd = "mount -o nfsvers=3,noacl,nolock 127.0.0.1:/nfs_test " + mount_point
    os.system(cmd)

    remount = "sync && umount -f %s && %s " % (mount_point, cmd)
    
    t_chmod(mount_point)
    logging.info('!!!t_chmod succ!!!')
    t_chown(mount_point)
    logging.info('!!!t_chown succ!!!')
    t_link(mount_point)
    logging.info('!!!t_link succ!!!')
    t_mkdir(mount_point)
    logging.info('!!!t_mkdir succ!!!')
    t_open(mount_point)
    logging.info('!!!t_open succ!!!')
    t_rename(mount_point)
    logging.info('!!!t_rename succ!!!')
    t_rmdir(mount_point)
    logging.info('!!!t_rmdir succ!!!')
    t_symlink(mount_point)
    logging.info('!!!t_symlink succ!!!')
    t_truncate(mount_point)
    logging.info('!!!t_truncate succ!!!')
    t_unlink(mount_point)
    logging.info('!!!t_unlink succ!!!')
    t_io(mount_point, remount)
    logging.info('!!!t_io succ!!!')
    
    
if __name__ == '__main__':
    #  --home 参数先保留
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument("--home", required=True, help="specify project home directory")

    args = parser.parse_args()
    home = args.home

    nfs_test()
