#!/usr/bin/env python2

import os
import time
import errno

from daemon import Daemon
from utils import Exp, lock_file1, unlock_file1

class DiskLightDaemon(Daemon):
    def __init__(self, dev, raid_type, start_cmd, stop_cmd):
        self.raid_type = raid_type
        self.start_cmd = start_cmd
        self.stop_cmd = stop_cmd
        pidfile = '/var/run/fusionstack_disk_%s.light' % dev.split('/')[-1]
        super(DiskLight, self).__init__(pidfile, name='disk light')

    def run(self):
        while True:
            if self.raid_type == 'HPRAID':
                try:
                    fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                    os.system(self.start_cmd)
                    _unlock_file1(fd)
                except Exp, e:
                    pass
            else:
                os.system(self.start_cmd)
            time.sleep(3)

            if self.raid_type == 'HPRAID':
                try:
                    fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                    os.system(self.stop_cmd)
                    _unlock_file1(fd)
                except Exp, e:
                    pass
            else:
                os.system(self.stop_cmd)
            time.sleep(3)

class DiskLight():
    def __init__(self, dev='', raid_type='', start_cmd='', stop_cmd=''):
        self.dev = dev
        self.raid_type = raid_type
        self.start_cmd = start_cmd
        self.stop_cmd = stop_cmd
        self.home = '/var/run/fusionstack_disk_light'
        if not os.path.exists(self.home):
            os.mkdir(self.home)
        self.path = os.path.join(self.home, dev.split('/')[-1])

    def start(self, force):
        if self.raid_type == 'HPRAID':
            try:
                fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                os.system(self.start_cmd)
                _unlock_file1(fd)
            except Exp, e:
                pass
        else:
            os.system(self.start_cmd)

        if os.path.exists(self.path):
            raise Exp(errno.EPERM, self.dev + " already started")
        os.system('touch %s' % self.path)

    def stop(self, force):
        if self.raid_type == 'HPRAID':
            try:
                fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                os.system(self.stop_cmd)
                _unlock_file1(fd)
            except Exp, e:
                pass
        else:
            os.system(self.stop_cmd)

        if not os.path.exists(self.path):
            raise Exp(errno.EPERM, self.dev + " already stopped")
        os.system('rm -rf %s' % self.path)

    def stat(self):
        if os.path.exists(self.path):
            return True
        else:
            return False

    def list(self):
        return os.listdir(self.home)
