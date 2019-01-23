#!/usr/bin/env python2

import os
import stat
import errno
import re
import time

from utils import _exec_pipe1

class LVM:
    def __init__(self):
        self.lvs = None
        self.pvs = None

    def get_major_minor(self, dev):
        dev = dev.split('/')[-1]
        with file('/proc/partitions', 'rb') as proc_partitions:
            for line in proc_partitions.read().split('\n')[2:]:
                fields = line.split()
                if len(fields) < 4:
                    continue
                name = fields[3].split('/')[-1]
                if name == dev:
                    return (fields[0], fields[1])

    def get_sys_pvs(self):
        pvs = {}
        res,err = _exec_pipe1(["pvs"], 0, False, 5)
        for line in res.splitlines()[1:]:
            fields = line.split()
            if fields[1] not in pvs:
                pvs[fields[1]] = []
            if fields[0] not in pvs[fields[1]]:
                pvs[fields[1]].append(fields[0])

        return pvs

    def get_sys_lvs(self):
        lvs = {}
        res,err = _exec_pipe1(["lvdisplay"], 0, False, 5)
        for line in res.splitlines():
            m = re.search('\s*LV Name\s*(.*)', line)
            if m is not None:
                lv = m.group(1)
            m = re.search('\s*VG Name\s*(.*)', line)
            if m is not None:
                vg = m.group(1)
            m = re.search('\s*Block device\s*(.*)', line)
            if m is not None:
                block = m.group(1)
                if vg not in lvs:
                    lvs[vg] = {}
                lvs[vg][lv] = (block.split(':')[0], block.split(':')[1])

        return lvs

    def get_dev_bylvm(self, dev):
        major_minor = self.get_major_minor(dev)
        if self.pvs is None:
            self.pvs = self.get_sys_pvs()
        if self.lvs is None:
            self.lvs = self.get_sys_lvs()
        for vg in self.lvs:
            for lv in self.lvs[vg]:
                if self.lvs[vg][lv] == major_minor:
                    return self.pvs[vg]
