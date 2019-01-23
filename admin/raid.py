#!/usr/bin/env python2

import errno
import platform

from mega_raid import MegaRAID
from hp_raid import HPRAID
from utils import Exp, dwarn, derror, _exec_pipe, _exec_pipe1

class RAID:
    def __init__(self, config):
        self.raid_type = self.__check_raid_env()
        if self.raid_type == 'MegaRAID':
            self.raid_tool = MegaRAID()
        elif self.raid_type == 'HPRAID':
            self.raid_tool = HPRAID()

    def __check_raid_env(self):
        host_type = platform.architecture()[0]
        raid_type = []
        ls_pci = None

        try:
            ls_pci = _exec_pipe(["lspci"], 0, False)
        except:
            return None

        for line in ls_pci.splitlines():
            if 'RAID bus controller: LSI Logic / Symbios Logic MegaRAID' in line:
                cmd = "/opt/MegaRAID/MegaCli/MegaCli"
                if host_type == '64bit':
                    cmd += '64'
                try:
                    (out_msg, err_msg) = _exec_pipe1([cmd, "-v", "-Nolog"], 0, False, 10)
                except Exception, e:
                    raise Exp(errno.EPERM, cmd + " command execute failed")
                try:
                    (out_msg, err_msg) = _exec_pipe1(["disk2lid"], 0, False)
                except Exception, e:
                    _derror("disk2lid command execute failed")
                    return None
                if 'MegaRAID' not in raid_type:
                    raid_type.append('MegaRAID')
            elif 'RAID bus controller: Hewlett-Packard Company Smart Array' in line:
                '''
                try:
                    fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                    (out_msg, err_msg) = _exec_pipe1(["hpacucli", "-h"], 0, False)
                    _unlock_file1(fd)
                except Exp, e:
                    raise
                except Exception, e:
                    raise Exp(errno.EPERM, "hpacucli command not found")
                '''
                if 'HPRAID' not in raid_type:
                    raid_type.append('HPRAID')

        if len(raid_type) == 0:
            return None
        else:
            for i in range(len(raid_type)):
                if raid_type[i] != raid_type[0]:
                    raise Exp(errno.EPERM, "Can not support different raid tools")
            return raid_type[0]

    def raid_add(self, dev, force):
        if self.raid_type:
            self.raid_tool.add_raid0(dev, force)
        else:
            raise Exp(errno.EPERM, "No raid card found")

    def raid_del(self, dev, force):
        try:
            self.raid_tool.del_raid0(dev)
        except Exception, e:
            if force:
                self.raid_tool.del_raid_force(dev)
            else:
                raise Exp(errno.EPERM, "can not drop raid %s.\nIf you want, you can try use --force" % dev)

    def raid_import(self):
        if self.raid_type == "MegaRAID":
            self.raid_tool.import_raid_foreign()
        else:
            raise Exp(errno.EPERM, "now only support MegaRAID")

    def raid_flush(self):
        if self.raid_type == "MegaRAID":
            self.raid_tool.raid_cache_flush()
        else:
            raise Exp(errno.EPERM, "now only support MegaRAID")

    def raid_cache(self, switch, devs, cacheconf, cache):
        if self.raid_type:
            if switch == 'show':
                for dev in devs:
                    raid_cache = self.raid_tool.get_raid_cache(dev)
                    print dev, ":", raid_cache
            elif switch == 'set' and cache:
                for dev in devs:
                    self.raid_tool.set_raid_policy(dev, cache)
            elif switch == 'set':
                self.raid_check(devs, cacheconf);
                for dev in devs:
                    self.raid_tool.set_raid_ratio(dev)
        else:
            raise Exp(errno.EPERM, "No raid card found")

    def raid_miss(self):
        if self.raid_type:
            self.raid_tool.del_raid_missing()
        else:
            raise Exp(errno.EPERM, "No raid card found")

    def raid_light(self, switch, devs):
        if self.raid_type:
            if switch == 'list':
                disk_light = self.raid_tool.get_light_flash()
                if len(disk_light) == 0:
                    print "No disk light is starting"
                else:
                    print disk_light
            for dev in devs:
                self.raid_tool.set_light_flash(switch, dev)
        else:
            raise Exp(errno.EPERM, "No raid card found")

    def raid_check(self, devs, cacheconf, force=False, setcache=True):
        if len(cacheconf) == 0:
            return

        cache_devs = {}
        for dev in devs:
            if self.raid_type:
                check_cache = self.raid_tool.check_raid_cache(dev, cacheconf, setcache)
                if len(check_cache) == 0:
                    continue
                else:
                    cache_devs[dev] = check_cache

        if not setcache:
            return cache_devs

        for dev in cache_devs:
            try:
                self.raid_tool.set_raid_cache(dev, cache_devs[dev])
            except Exp, e:
                if not force:
                    raise Exp(e.errno, "set raid %s cache faile:%s" %(dev, e.err))
                else:
                    _dwarn("set raid %s cache faile, %s" %(dev, e.err))

    def raid_info(self, dev):
        if self.raid_type:
            return self.raid_tool.get_dev_info(dev)
        else:
            return None

    def disk_info(self, disk):
        if self.raid_type:
            return self.raid_tool.get_disk_info(disk)
        else:
            return None

    def disk_list(self):
        if self.raid_type:
            return self.raid_tool.get_new_disk()
        else:
            return None

    def disk_model(self, dev):
        if self.raid_type:
            return self.raid_tool.get_dev_device_model(dev)
        else:
            return ''

    def disk_rotation(self, dev):
        if self.raid_type:
            return self.raid_tool.get_disk_rotation(self.raid_type, dev)
        else:
            return None
