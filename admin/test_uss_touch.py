#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import errno

from config import Config
from utils import  Exp, exec_shell, dwarn, dmsg

def test_mkdir(config):
        try:
                _exec_mkdir = "%s /zz" % (config.uss_mkdir)
                exec_shell(_exec_mkdir)
                _exec_stat = "%s /zz" % (config.uss_stat)
                exec_shell(_exec_stat)
        except Exp, err:
                dmsg("test_mkdir or test_stat failed\n")
                sys.exit(-1);

def test_touch(config):
        for i in range(100):
                try:
                        _exec_touch = "%s /zz/zz%d" % (config.uss_touch, i)
                        exec_shell(_exec_touch)
                        _exec_stat = "%s /zz/zz%d" % (config.uss_stat, i)
                        exec_shell(_exec_stat)
                        _exec_rm = "%s /zz/zz%d" % (config.uss_rm, i)
                        exec_shell(_exec_rm)
                except Exp, err:
                        dmsg("test_touch or test_stat failed\n")
                        sys.exit(-1);

if __name__ == "__main__":
        config = Config();
        test_mkdir(config)
        test_touch(config)
