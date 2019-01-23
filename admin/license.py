#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import errno
import string
import argparse
import time
from Crypto.Cipher import AES
from config import Config
from utils import Exp, exec_shell, exec_remote, \
                dmsg, derror

"""
info.file format:
        secret_key:uuid
        [MAC]FFFFFFFFFF
        [CREATE_TIME]FFFFFFFFFF
        [TOTAL_CAP]FFFFFFFFFF
"""

NET_PATH="/sys/class/net"
INFO_PATH="/opt/sdfs"
ATTR_NAME="secret_key"

#  PADDING='\006'
BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s : s[0:-ord(s[-1])]

# 截取前16位作为secret_key
def get_secret_key(uss_attr):
    _exec_attr = '%s -g %s /system' % (uss_attr, ATTR_NAME)
    try:
        key, _ = exec_shell(_exec_attr, need_return=True, timeout=30)
    except Exp, e:
        derror("%s : %s" %  (_exec_attr, str(e)))
        sys.exit(-1)

    return key[0:16]

def _collect_mac(host=None):
    _exec_ls = 'ls %s' % (NET_PATH)
    try:
        net_interface, _ = exec_remote(host, _exec_ls)
    except Exp, e:
        derror("%s : %s" %  (_exec_ls, str(e)))
        sys.exit(-1)

    net_list = net_interface.split('\n')
    if len(net_list) == 0:
        derror("not found any ethernet interface")
        sys.exit(errno.ENOENT)
    for net in net_list:
        if net == 'lo' \
                or net == '.' \
                or net == '..' \
                or net == '':
            continue
        else:
            address = os.path.join(NET_PATH, net, 'address')
            _exec_cat = 'cat %s' % (address)
            try:
                mac, _ = exec_remote(host, _exec_cat)
            except Exp, e:
                derror("%s : %s" %  (_exec_cat, str(e)))
                sys.exit(-1)
            return mac

# 假定集群已经创建
def _collect_statvfs_info(uss_statvfs):
    _exec = "%s /system" % (uss_statvfs)
    try:
        output, _ = exec_shell(_exec, need_return=True, timeout=10)
    except Exp, e:
        derror("%s : %s" % (_exec, str(e)))
    value_list = output.split(' ')
    total_cap = value_list[1].strip('\t')
    cap_int = (string.atol(total_cap)) / 1024 / 1024 / 1024
    return str(cap_int)

def _collect_createtime(uss_attr):
    _exec = "%s -g create_time /system" % (uss_attr)
    try:
        create_time, _ = exec_shell(_exec, need_return=True, timeout=10)
    except Exp, e:
        derror("%s : %s" % (_exec, str(e)))
    return create_time.strip('\n')

# 收集信息:
# 1.所有节点MAC地址
# 2.集群总容量
# 3.集群创建时间
def _collect(config = None):
    print "collect..."
    if config is None:
        derror("config must be specified")
        sys.exit(ENOENT)
    # 获取集群节点IP
    hosts = config.cluster.keys()

    mac_list = []
    if hosts is None:
        derror("hosts must be specified")
        sys.exit(errno.ENOENT)
    for h in hosts:
        mac =  _collect_mac(h)
        mac_list.append(mac.strip('\n'))

    create_time = _collect_createtime(config.uss_attr)

    total_cap = _collect_statvfs_info(config.uss_statvfs)

    _exec_mkdir = "mkdir -p %s" % (INFO_PATH)
    exec_shell(_exec_mkdir)

    secret_key = get_secret_key(config.uss_attr)

    info_file = os.path.join(INFO_PATH, 'info.file')
    with open(info_file, "w") as fp:
        fp.write("secret_key:%s\n" %(secret_key))

        mac_encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
        mac_str = '|'.join(mac_list)
        mac_str_padding = pad(mac_str).encode('hex')
        mac_str_cipher = mac_encryptor.encrypt(mac_str_padding)
        fp.write(mac_str_cipher.encode('hex') + '\n')

        create_time_encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
        create_time_padding = pad(create_time).encode('hex')
        create_time_cipher = create_time_encryptor.encrypt(create_time_padding)
        fp.write(create_time_cipher.encode('hex') + '\n')

        total_cap_encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
        total_cap_padding = pad(total_cap).encode('hex')
        total_cap_cipher = total_cap_encryptor.encrypt(total_cap_padding)
        fp.write(total_cap_cipher.encode('hex') + '\n')

def _dump(config=None):
    print 'dump...'
    secret_key = get_secret_key(config.uss_attr)

    info_file = os.path.join(INFO_PATH, 'info.file')
    with open(info_file, "r") as fp:
        line_list = fp.readlines()
        mac_cipher = line_list[1].rstrip('\n').decode('hex')
        create_time_cipher = line_list[2].rstrip('\n').decode('hex')
        total_cap_cipher = line_list[3].rstrip('\n').decode('hex')

    mac_decryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    mac_str_padding = mac_decryptor.decrypt(mac_cipher)
    mac_str = unpad(mac_str_padding.decode('hex'))
    print 'mac_str:', mac_str

    # 解密 create_time
    create_time_encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    create_time_padding = create_time_encryptor.decrypt(create_time_cipher)
    create_time = unpad(create_time_padding.decode('hex'))
    print 'create_time:', create_time

    total_cap_decryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    total_cap_padding = total_cap_decryptor.decrypt(total_cap_cipher)
    total_cap = unpad(total_cap_padding.decode('hex'))
    print 'total_cap:', total_cap

def _list(config=None):
    print 'list...'
    info_file = os.path.join(INFO_PATH, 'license')
    with open(info_file, "r") as fp:
        line_list = fp.readlines()
        secret_key = line_list[0].rstrip('\n').split(':')[1]
        mac_cipher = line_list[1].rstrip('\n').decode('hex')
        create_time_cipher = line_list[2].rstrip('\n').decode('hex')
        total_cap_cipher = line_list[3].rstrip('\n').decode('hex')

    print 'secret_key:', secret_key
    mac_decryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    mac_str_padding = mac_decryptor.decrypt(mac_cipher)
    mac_str_hex = unpad(mac_str_padding)
    mac_str = mac_str_hex.decode('hex')
    print 'mac_str:', mac_str

    # 解密 create_time
    create_time_encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    create_time_padding = create_time_encryptor.decrypt(create_time_cipher)
    create_time_hex = unpad(create_time_padding)
    create_time = create_time_hex.decode('hex')
    timestamp = string.atol(create_time)
    time_local = time.localtime(timestamp)
    due_date = time.strftime("%Y-%m-%d %H:%M:%S",time_local)

    print 'due date:', due_date

    total_cap_decryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    total_cap_padding = total_cap_decryptor.decrypt(total_cap_cipher)
    total_cap_hex = unpad(total_cap_padding)
    total_cap = total_cap_hex.decode('hex')
    cap_int = string.atol(total_cap)
    print 'total_cap: %d Gb' % (cap_int)


def _test_aes(config):
    secret_key = get_secret_key(config.uss_attr)
    print 'secret_key: ', secret_key

    encryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    padding_text = pad("1504259063")
    print 'create_time_padding:', padding_text, len(padding_text)
    cipher_text = encryptor.encrypt(padding_text)
    print 'len create_time_cipher:', len(cipher_text)

    print 'decrypt...'

    decryptor = AES.new(secret_key, AES.MODE_CBC, secret_key)
    padding_text = decryptor.decrypt(cipher_text)
    print 'create_time_padding:', padding_text, len(padding_text)
    create_time = unpad(padding_text)
    print 'create_time:', create_time, len(create_time)

if __name__ == '__main__':
    # 获取集群节点ip
    config = Config()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_collect = subparsers.add_parser('collect', help='collect node info')
    parser_collect.set_defaults(func=_collect)

    parser_dump = subparsers.add_parser('dump', help='print license info')
    parser_dump.set_defaults(func=_dump)

    parser_list = subparsers.add_parser('list', help='print license info')
    parser_list.set_defaults(func=_list)

    parser_test_aes = subparsers.add_parser('test', help='test AES')
    parser_test_aes.set_defaults(func=_test_aes)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(config)
