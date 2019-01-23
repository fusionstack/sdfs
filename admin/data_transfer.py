#!/usr/bin/env python2.7
# -*- coding:utf-8 -*-

import os
import sys
import argparse
from utils import  Exp, exec_shell, dmsg, dwarn
from config import Config
from multiprocessing import Pool
from errno import EAGAIN, EINVAL

def linked(config, filename):
    is_linked = file_get_attr(config, "link", filename)
    return is_linked

def copyed(config, filename):
    is_copyed = file_get_attr(config, "copy", filename)
    return is_copyed

#列出src目录中所有文件
def list_dir(config, path):
    cmd = "%s %s | awk '{print $9}'" % (config.uss_ls, path)
    file_list, _ = exec_shell(cmd, need_return=True)
    file_list = [x.strip() for x in file_list.split()]
    return file_list

#拷贝文件uss2uss
#uss.cp :/dir01/xx :/dir01_ec/xx
def file_copy(abs_file):
    retry = 0
    config = abs_file['conf']
    _exec_cp = "%s :%s :%s" % (config.uss_cp, abs_file['abs_src_file'], abs_file['abs_dst_file'])
    _exec_rm = "%s %s" % (config.uss_rm, abs_file['abs_dst_file'])
    while True:
        try:
            if retry <= 3:
                exec_shell(_exec_cp)
                file_set_attr(config, "copy", "true", abs_file['abs_src_file'])
                return True
            else:
                return False
        except Exp, err:
            retry = retry + 1
            exec_shell(_exec_rm)
            continue

#重命名
#uss.mv /dir01/xx /dir01/xx_bak
def file_move(config, src_file, dst_file):
    cmd = "%s %s %s" % (config.uss_mv, src_file, dst_file)
    try:
        exec_shell(cmd)
        file_set_attr(config, "move", "true", dst_file)
        return True
    except Exp, err:
        file_set_attr(config, "move", "false", dst_file)
        return False

#硬链接
#uss.ln target(源文件) link_name
def file_link(config, target, link_name):
    cmd = "%s %s %s" % (config.uss_ln, target, link_name)
    try:
        #if linked(config, target) == "false": #link属性不存在，或者属性值为false
        exec_shell(cmd)
        file_set_attr(config, "link", "true", target)
        return True
    except Exp, err:
        file_set_attr(config, "link", "false", target)
        return False

#如果拷贝完成，需删除源文件
#如果拷贝失败，需删除不完整的目标文件
def file_remove(abs_file_list):
    config = abs_file_list['conf']
    src_file = abs_file_list['abs_src_file']
    dst_file = abs_file_list['abs_dst_file']
    should_remove = abs_file_list['should_remove_src']
    del_src_cmd = "%s %s" % (config.uss_rm, src_file)
    del_dst_cmd = "%s %s" % (config.uss_rm, dst_file)
    try:
        if 'true' in copyed(config, src_file) and should_remove:
            exec_shell(del_src_cmd)
            return True
        else:
            exec_shell(del_dst_cmd)
            return True
    except Exp, err:
        return False

def file_set_attr(config, key, value, filename):
    cmd = "%s -s %s -V %s %s" % (config.uss_attr, key, value, filename)
    try:
        exec_shell(cmd)
        return True
    except Exp, err:
        return False

def file_get_attr(config, key, filename):
    cmd = "%s -g %s %s" % (config.uss_attr, key, filename)
    try:
        value, _ = exec_shell(cmd, need_return=True)
        return value  #属性存在，返回属性值
    except Exp, err:
        return "false" #属性不存在，返回false属性值

def _start_copy(concurrent, abs_file_list):
    process = Pool(concurrent)
    process.map(file_copy, abs_file_list)
    process.close()
    process.join()

def _start_remove(concurrent, abs_file_list):
    process = Pool(concurrent)
    process.map(file_remove, abs_file_list)
    process.close()
    process.join()

def print_file(filename):
    print filename

def _check(config, src_dir):
    src_file_list = list_dir(config, src_dir)
    if len(src_file_list) == 0:
        dmsg("transfer ok")
        return True
    else:
        map(print_file, src_file_list)
        sys.exit(EAGAIN)

def _check_valid_directory(config, directory):
    _exec_stat = "%s %s" % (config.uss_stat, directory)

    try:
        stat, _ = exec_shell(_exec_stat, need_return=True)
        mode = stat.split(';')[2].split(':')[1].split('/')[0].split()[0]
        if mode == 'd':
            return True
        else:
            return False
    except Exp, err:
        return False

def _transfer(args):
    config = Config()
    abs_file_list_1 = []
    abs_file_list_2 = []

    #检查源目录是否有效
    if not _check_valid_directory(config, args.src):
        dwarn('%s is not valid directory' % (args.src))
        sys.exit(EINVAL)
    #检查目的目录是否有效
    if not _check_valid_directory(config, args.dst):
        dwarn('%s is not valid directory' % (args.dst))
        sys.exit(EINVAL)

    src_file_list = list_dir(config, args.src) #列出源目录中所有文件

    if len(src_file_list) == 0:
        dmsg('empty directory')
        return False

    #获取文件的绝对路径，保存到abs_file_list
    for src_file in src_file_list:
        abs_src_file = os.path.join(args.src, src_file)
        abs_dst_file = os.path.join(args.dst, src_file)
        abs_file_list_1.append({'abs_src_file':abs_src_file, 'abs_dst_file':abs_dst_file, 'conf':config, 'should_remove_src':False})
        abs_file_list_2.append({'abs_src_file':abs_src_file, 'abs_dst_file':abs_dst_file, 'conf':config, 'should_remove_src':True})

    #如果之前目的路径包含不完整的文件，需要首先删除
    _start_remove(args.concurrent, abs_file_list_1)
    _start_copy(args.concurrent, abs_file_list_1)
    _start_remove(args.concurrent, abs_file_list_2)
    _check(config, args.src)



def _remove(args):
    config = Config()
    file_list = list_dir(config, args.dir)
    for file in file_list:
        abs_file = os.path.join(args.dir, file)
        cmd = '%s %s' % (config.uss_rm, abs_file)
        try:
            exec_shell(cmd)
        except Exp, err:
            return False

def _create(args):
    config = Config()
    for i in range(args.n):
        filename = "xx_%d.txt" % (i)
        abs_file = os.path.join(args.dir, filename)
        cmd = "%s %s" % (config.uss_touch, abs_file)
        exec_shell(cmd)

def _write(args):
    config = Config()
    abs_file_list = []
    file_list = list_dir(config, args.dir)
    for file in file_list:
        abs_file = os.path.join(args.dir, file)
        abs_file_list.append(abs_file)

    for abs_file in abs_file_list:
        cmd = "%s hello %s" % (config.uss_write, abs_file)
        exec_shell(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_mv = subparsers.add_parser('transfer', help='transfer data  between directorys')
    parser_mv.add_argument('--src', required=True, help='source directory')
    parser_mv.add_argument('--dst', required=True, help='destation directory')
    parser_mv.add_argument('-c', '--concurrent', default=20, type=int, help='concurrent operations [default : 20]')
    parser_mv.set_defaults(func=_transfer)

    parser_rm = subparsers.add_parser('remove', help='remove directory')
    parser_rm.add_argument('--dir', required=True, help='which directory')
    parser_rm.set_defaults(func=_remove)

    parser_rm = subparsers.add_parser('create', help='create tmp file')
    parser_rm.add_argument('--dir', required=True, help='which directory')
    parser_rm.add_argument('-n', required=True, type=int, help='num of files')
    parser_rm.set_defaults(func=_create)

    parser_rm = subparsers.add_parser('write', help='write file')
    parser_rm.add_argument('--dir', required=True, help='which directory')
    parser_rm.set_defaults(func=_write)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
