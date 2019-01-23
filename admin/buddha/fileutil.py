#!/usr/bin/env python
# -*- coding: utf-8 -*-


from utils import _str2dict


class LichFile(object):
    def __init__(self, filename=None):
        self.filename = filename

    @staticmethod
    def read_int(filename):
        with open(filename) as f:
            res = f.read()
            return int(res.strip('\n').strip('\0'))

    @staticmethod
    def write_int(filename, n):
        with open(filename, 'w') as f:
            f.write('%d\n' % n)
            f.flush()

    @staticmethod
    def read_dict(filename):
        with open(filename) as f:
            res = f.read()
            return _str2dict(res)
