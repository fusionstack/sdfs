#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import errno
from multiprocessing import Pool

from config import Config
from utils import  Exp, exec_shell, dwarn, dmsg

def _start_rm(arg):
        _exec_rm = "%s %s" % (arg['conf'].uss_rm, arg['file'])
        try:
                exec_shell(_exec_rm)
        except Exp, err:
                pass

def test_rm(concurrent, arg_list):
        process = Pool(concurrent)
        process.map(_start_rm, arg_list)
        process.close()
        process.join()

def list_dir(config, path):
    _exec_ls = "%s %s | awk '{print $9}'" % (config.uss_ls, path)
    file_list, _ = exec_shell(_exec_ls, need_return=True)
    file_list = [x.strip() for x in file_list.split()]
    return file_list


if __name__ == "__main__":
        if(len(sys.argv) != 3):
            print 'uss.rm dir concurrent'
            sys.exit(-1)

        path = sys.argv[1]
        concurrent = int(sys.argv[2])

        config = Config()
        file_list = list_dir(config, path)

        arg_list = []
        for file in file_list:
                abs_file = os.path.join(path, file)
                arg_list.append({'file':abs_file, 'conf':config})

        test_rm(concurrent, arg_list)

