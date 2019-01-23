#!/usr/bin/env python2

import os
import stat
import errno
import re

from utils import Exp, _exec_pipe, _exec_pipe1

class SMART:
    def __init__(self):
        try:
            _exec_pipe(['smartctl', '-h'], 0, False)
        except Exception, e:
            #raise Exp(errno.EPERM, "smartctl execute failed")
            pass

    def __ret2status(self, ret):
        status = {}
        for i in range(0, 8):
            status[i] = ret & 2**i and 1
        return status

    def __get_health_warn(self, dev, arg=None):
        warn = ''
        try:
            cmd = ["smartctl", "-H"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            out_msg = e.out

        for line in out_msg.splitlines():
            m = re.search('\d+\s+(\S+)\s+\S+\s+(\S+)\s+\S+\s+(\S+)\s+\S+\s+\w+\s+(\S+)\s+\S+', line)
            if m is not None:
                if (m.group(4) != '-'):
                    warn = m.group(1) + '(value:' + m.group(2) + '/thresh:' + m.group(3) + ')'

        return warn

    def __get_health_errlog(self, dev, arg=None):
        errlog = ''
        islog = 0

        try:
            cmd = ["smartctl", "-l", "xerror"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            out_msg = e.out

        for line in out_msg.splitlines():
            m = re.search('SMART Extended Comprehensive Error Log Version:', line)
            if m is not None:
                islog = 1
                continue
            if islog == 1:
                errlog += line

        return errlog

    def __get_health_testlog(self, dev, arg=None):
        testlog = ''
        islog = 0

        cmd = ["smartctl", "-l", "xselftest"]
        if arg is not None:
            cmd += ["-d", arg]
        cmd += [dev]
        (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        for line in out_msg.splitlines():
            m = re.search('SMART Extended Self-test Log Version', line)
            if m is not None:
                islog = 1
                continue
            if islog == 1:
                testlog += line

        return testlog

    def get_dev_health(self, dev, arg=None):
        dev_health = 'Unknow'
        ret = 0

        try:
            cmd = ["smartctl", "-i"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            out_msg = e.out
            err_msg = e.err

        for line in out_msg.splitlines():
            m = re.search('Unable to detect device type', line)
            if m is not None:
                dev_health = 'Not-support'
                return dev_health

            m = re.search('Device does not support SMART', line)
            if m is not None:
                dev_health = 'Not-support'
                return dev_health

            m = re.search('SMART support is: Disabled', line)
            if m is not None:
                try:
                    cmd = ["smartctl", "-S", "on"]
                    if arg is not None:
                        cmd += ["-d", arg]
                    cmd += [dev]
                    (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
                except Exp, e:
                    pass
                break

        try:
            cmd = ["smartctl", "-a"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            out_msg = e.out
            err_msg = e.err
            ret = e.errno

        for line in out_msg.splitlines():
            m = re.search('SMART overall-health self-assessment test result: (\w+)', line)
            if m is not None:
                dev_health = m.group(1)
                if (dev_health == 'PASSED'):
                    dev_health = 'Normal'
                elif (dev_health == 'FAILED'):
                    dev_health = 'Failed'

        status = self.__ret2status(ret)

        if 1 in status.values():
            if dev_health != 'Failed':
                dev_health = 'Warnning'

        for k, v in status.iteritems():
            if k == 0 and v == 1:
                dev_health += ':command-not-parse'
            if k == 1 and v == 1:
                dev_health += ':low-power-mode'
            if k == 2 and v == 1:
                dev_health += ':command-failed'
            if k == 3 and v == 1:
                dev_health += ':self-test-failed'
            if k == 4 and v == 1:
                warn = self.__get_health_warn(dev, arg)
                dev_health += ':' + warn
            if k == 5 and v == 1:
                warn = self.__get_health_warn(dev, arg)
                dev_health += ':' + warn
            if k == 6 and v == 1:
                errlog = self.__get_health_errlog(dev, arg)
                dev_health += ':' + errlog
            if k == 7 and v == 1:
                testlog = self.__get_health_testlog(dev, arg)
                dev_health += ':' + testlog

        return dev_health

    def get_dev_rotation(self, dev, arg=None):
        try:
            cmd = ["smartctl", "-i"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            return None

        for line in out_msg.splitlines():
            m = re.search('Rotation Rate:\s+(\d+) rpm', line)
            if m is not None:
                return int(m.group(1))

    def get_dev_info(self, dev, arg=None):
        out_msg = ''
        try:
            cmd = ["smartctl", "-i"]
            if arg is not None:
                cmd += ["-d", arg]
            cmd += [dev]
            (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
        except Exp, e:
            if e.errno == errno.ENOENT:
                raise
            else:
                return None
        return out_msg
