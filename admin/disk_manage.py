#!/usr/bin/env python2

import os
import errno
import re
import ConfigParser
import json
import socket
import time
import mmap
import shutil
import sys

from disk import Disk
from raid import RAID
from mega_raid import MegaRAID
from utils import Exp, dmsg, dwarn, derror, _human_readable, _human_unreadable, _exec_pipe1, exec_system, \
        _str2dict, _syserror, _syswarn, lock_file1, unlock_file1

DISK_MAX_TIER = 1
DISK_MAX_NUM = 255
# struct ext4_super_block offset = 1024; ext4_super_block.s_reserved offset = 588;
VALID_DATA_OFFSET = 1024 + 588
# sizeof(ext4_super_block.s_reserved) = 109
VALID_DATA_LEN = 109 + 1024
DISK_INFO_OFFSET = 4096
FILE_BLOCK_LEN = 4096

class DiskManage(object):
    def __init__(self, node):
        self.config = node.config
        self.raid = None
        self.disk = Disk()
        self.disk_home = os.path.join(self.config.home, 'cds')
        self.cacheconf = None

    def ssd_models(self):
        models = []
        path = os.path.join(self.config.home, 'etc/ssd.models')
        if not os.path.exists(path):
            return []

        with file(path, 'r') as fd:
            for line in fd.readlines():
                line = line.strip()
                if not len(line) or line.startswith('#'):
                    continue
                models.append(line)

        return models

    def parse_cacheconf(self, cacheconf, devs=None):
        cache = {}

        if (self.config.testing):
            return cache

        lich_disk = self.__get_lich_disk()
        if devs is not None:
            lich_disk.extend([('diskx', x, None) for x in devs])
        sys_dev = self.disk.get_sys_dev()
        for dev in cacheconf:
            if dev == 'ssd' or dev == 'hdd':
                cache[dev] = cacheconf[dev]
            elif dev.startswith('disk'):
                for item in lich_disk:
                    if item[0] == dev:
                        cache[item[1]] = cacheconf[dev]
            elif dev == 'sysdev':
                for item in sys_dev:
                    cache[item] = cacheconf[dev]

        models = self.ssd_models()
        if not self.raid:
            self.raid = RAID(self.config)
            if self.raid.raid_type == 'HPRAID':
                return cache

        for (disk,dev,pool) in lich_disk:
            if disk.startswith('disk'):
                dev_type = 'UNKNOW'
                try:
                    dev_type = self.disk.get_dev_type(dev)
                except:
                    continue
                if dev_type != 'RAID':
                    continue

                dev = self.disk.get_dev(dev)
                model = self.raid.disk_model(dev)
                if any(m in model for m in models):
                    if dev not in cache and not cache['ssd']['skip']:
                        cache[dev] = {}
                        cache[dev]['raid_cache'] = 'disable'
                        cache[dev]['disk_cache'] = 'enable'

        return cache

    def __get_disk_num(self):
        disk_num = 0
        disk_path = os.path.join(self.config.home, 'data/disk/disk')
        bitmap_path = os.path.join(self.config.home, 'data/disk/bitmap')
        info_path = os.path.join(self.config.home, 'data/disk/info')
        while disk_num <= DISK_MAX_NUM:
            if os.path.exists(disk_path) and str(disk_num) + '.disk' in os.listdir(disk_path):
                disk_num += 1
            elif os.path.exists(bitmap_path) and str(disk_num) + '.bitmap' in os.listdir(bitmap_path):
                disk_num += 1
            elif os.path.exists(info_path) and str(disk_num) + '.info' in os.listdir(info_path):
                disk_num += 1
            else:
                break

        if disk_num > DISK_MAX_NUM:
            raise Exp(errno.EPERM, 'There is no number left')
        return disk_num

    def __get_disk_tiermask(self, disk_speed):
        mask = 0
        while disk_speed:
            disk_speed /= 10
            mask += 1

        return mask

    def __get_disk_tiermask_withspeed(self, lich_speed):
        mask = 0
        lich_mask = {}
        lich_mask_new = {}

        sort_speed = sorted(lich_speed.items(), key=lambda d: d[1])
        for i in range(len(sort_speed)):
            if mask not in lich_mask:
                lich_mask[mask] = []
            lich_mask[mask].append(sort_speed[i][0])
            if i == len(sort_speed) - 1:
                break
            if sort_speed[i + 1][1] > 2 * sort_speed[i][1]:
                mask += 1

        for i in lich_mask:
            for j in lich_mask[i]:
                lich_mask_new[j] = i

        return lich_mask_new

    def __get_disk_tier_withmask(self, lich_tier):
        max_tier = 0
        for disk in lich_tier:
            if lich_tier[disk] > max_tier:
                max_tier = lich_tier[disk]

        for disk in lich_tier:
            lich_tier[disk] = max_tier - lich_tier[disk]

        return lich_tier

    def __get_disk_tiermask_withrotation(self, lich_tier, lich_rotation):
        rotation_arr = []
        tier = 0
        for i in lich_rotation:
            if lich_rotation[i] not in rotation_arr:
                rotation_arr.append(lich_rotation[i])
        rotation_arr.sort()
        for i in lich_tier:
            tier = rotation_arr.index(lich_rotation[i])
            lich_tier[i] = tier

        return lich_tier

    def __get_lich_info(self, path):
        lich_info = {}
        if os.path.exists(path):
            for info in os.listdir(path):
                disk_num = int(info.split('.')[0])
                fp = open(os.path.join(path, info))
                value = fp.read().strip('\n').strip('\0')
                fp.close()
                lich_info[disk_num] = int(value)

        return lich_info

    def __get_pool_info(self, path, pool):
        pool_info = {}
        lich_info = self.__get_lich_info(path);
        pool_disk = self.get_pool_disk(pool)
        for i in pool_disk:
            if i in lich_info:
                pool_info[i] = lich_info[i]

        return pool_info

    def __get_lich_tier(self):
        return self.__get_lich_info(self.tier_path)

    def __get_pool_tier(self, pool):
        return self.__get_pool_info(self.tier_path, pool)

    def __get_lich_writeback_cache(self, dev):
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname

        fd = open(dev, 'rb')
        fd.seek(VALID_DATA_OFFSET, 0)
        buff = fd.read(VALID_DATA_LEN)
        fd.close()

        m = re.match('cluster=%s;node=%s;type=data;disk=(\d+);.*cache=(\d+);cached=(\d+);' %
                (clusteruuid, hostname), buff)

        if m:
            cache = int(m.group(2))
        else:
            cache = 0

        return cache

    def __get_lich_writeback_fromdisk(self):
        lich_writeback = {}
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname
        lich_link = self.__get_lich_link()
        for i in lich_link:
            dev = lich_link[i]
            fd = open(dev, 'rb')
            fd.seek(VALID_DATA_OFFSET, 0)
            buff = fd.read(VALID_DATA_LEN)
            fd.close()

            lich_writeback[i] = {}
            m = re.match('cluster=%s;node=%s;type=(\w+);disk=%s;cache=(\d+);cached=(\d+);' %
                    (clusteruuid, hostname, i), buff)
            if m:
                lich_writeback[i]['type'] = m.group(1)
                lich_writeback[i]['cache'] = int(m.group(2))
                lich_writeback[i]['cached'] = int(m.group(3))
            else:
                lich_writeback[i]['type'] = 'UNKNOW'
                lich_writeback[i]['cache'] = 0
                lich_writeback[i]['cached'] = 0


        return lich_writeback

    def __get_lich_writeback_fromdata(self):
        lich_writeback = {}

        block_path = os.path.join(self.config.home, 'data/disk/block')
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname

        if not os.path.exists(block_path):
            return lich_writeback

        for disk in os.listdir(block_path):
            i = int(disk.split('.')[0])
            disk_path = os.path.join(block_path, disk)

            fd = open(disk_path, 'rb')
            fd.seek(VALID_DATA_OFFSET, 0)
            buff = fd.read(VALID_DATA_LEN)
            fd.close()

            lich_writeback[i] = {}
            m = re.match('cluster=%s;node=%s;type=(\w+);disk=%s;cache=(\d+);cached=(\d+);' %
                    (clusteruuid, hostname, i), buff)
            if m:
                lich_writeback[i]['type'] = m.group(1)
                lich_writeback[i]['cache'] = int(m.group(2))
                lich_writeback[i]['cached'] = int(m.group(3))
            else:
                lich_writeback[i]['type'] = 'UNKNOW'
                lich_writeback[i]['cache'] = 0
                lich_writeback[i]['cached'] = 0

        return lich_writeback

    def __get_lich_writeback(self):
        return self.__get_lich_writeback_fromdisk()

    def __check_lich_speed(self):
        lich_link = self.__get_lich_link()
        lich_writeback = self.__get_lich_writeback_fromdisk()

        for i in lich_link:
            if lich_writeback[i]['type'] != 'data' or lich_writeback[i]['cache'] == 100:
                continue

            path = os.path.join(self.speed_path, str(i) + '.speed')
            if not os.path.exists(path):
                print "get %s speed start..." % lich_link[i]
                disk_speed = self.disk.get_dev_speed(lich_link[i])
                print "get %s speed %d" %(lich_link[i], disk_speed)
                _exec_system("echo %d > %s" %(disk_speed, path), True)

        for info in os.listdir(self.speed_path):
            disk_num = int(info.split('.')[0])
            if disk_num not in lich_link:
                _exec_system("rm -rf " + os.path.join(self.speed_path, info), True)

    def __get_lich_speed(self):
        return self.__get_lich_info(self.speed_path)

    def __get_pool_speed(self, pool):
        return self.__get_pool_info(self.speed_path, pool)

    def __check_lich_rotation(self):
        lich_link = self.__get_lich_link()
        lich_writeback = self.__get_lich_writeback_fromdisk()

        for i in lich_link:
            if lich_writeback[i]['type'] != 'data' or lich_writeback[i]['cache'] == 100:
                continue

            path = os.path.join(self.rotation_path, str(i) + '.rotation')
            if not os.path.exists(path):
                disk_rotation = self.__get_disk_rotation(lich_link[i])
                if disk_rotation is not None:
                    _exec_system("echo %d > %s" %(disk_rotation, path), True)

        for info in os.listdir(self.rotation_path):
            disk_num = int(info.split('.')[0])
            if disk_num not in lich_link:
                _exec_system("rm -rf " + os.path.join(self.rotation_path, info), True)

    def __get_lich_rotation(self):
        return self.__get_lich_info(self.rotation_path)

    def __get_pool_rotation(self, pool):
        return self.__get_pool_info(self.rotation_path, pool)

    def __get_disk_stat(self, lich_disk):
        disk_stat = {}
        diskstat = os.path.join(self.config.shm, 'nodectl/diskstat')

        if os.path.exists(diskstat):
            for disk in lich_disk:
                if not disk[0].startswith('disk'):
                    continue

                disk_num = disk[0][4:]
                stat_file = os.path.join(diskstat, "%s/%s.stat" % (disk[2], disk_num))
                if not os.path.exists(stat_file):
                    continue

                o = open(stat_file)
                res = o.read()
                o.close()
                d = _str2dict(res)
                disk_stat['disk' + disk_num] = d

        return disk_stat

    def __get_disk_rotation(self, dev):
        dev_type = self.disk.get_dev_type(dev)
        dev_rotation = None
        if dev_type == 'RAID':
            if not self.raid:
                self.raid = RAID(self.config)
            dev_rotation = self.raid.disk_rotation(self.disk.get_dev(dev))
        elif dev_type == 'HDD':
            dev_rotation = self.disk.get_dev_rotation(self.disk.get_dev(dev))
        else:
            pass
        return dev_rotation

    def __get_disk_pool(self, disk_num):
        if self.disk_search is None:
            self.disk_search = self.__disk_load_search()

        for disk in self.disk_search:
            if disk[0] == 'disk' + disk_num:
                return disk[2]

        return None

    def __get_all_pool(self):
        all_pool = []
        if self.disk_search is None:
            self.disk_search = self.__disk_load_search()

        for disk in self.disk_search:
            if disk[0].startswith('disk'):
                if disk[2] not in all_pool:
                    all_pool.append(disk[2])

        return all_pool

    def __get_lich_disk(self):
        lich_disk = []

        if os.path.exists(self.disk_home):
            for disk in os.listdir(self.disk_home):
                disk_num = disk.split('.')[0]
                disk_path = os.path.join(self.disk_home, disk)
                disk_dev = self.disk.get_dev_bymounted(disk_path)
                lich_disk.append(('disk' + disk_num, disk_dev, None))

        return lich_disk

    def get_lich_disk(self):
        return self.__get_lich_disk()

    def get_pool_disk(self, pool):
        pool_disk = []
        lich_disk = self.__get_lich_disk()
        for disk in lich_disk:
            if disk[0].startswith('disk') and disk[2] == pool:
                pool_disk.append(int(disk[0][4:]))

        return pool_disk

    def __get_lich_dev(self):
        lich_dev = []
        lich_disk = [dev for (x, dev, pool) in self.__get_lich_disk() if dev]
        for disk in lich_disk:
            dev = self.disk.get_dev(disk)
            if dev not in lich_dev:
                lich_dev.append(dev)

        return lich_dev

    def __get_lich_link(self):
        lich_link = {}
        if os.path.exists(self.disk_home):
            for disk in os.listdir(self.disk_home):
                disk_num = disk.split('.')[0]
                disk_path = os.path.join(self.disk_home, disk)
                if os.path.islink(disk_path):
                    disk_dev = os.readlink(disk_path)
                    if not self.disk.is_dev(disk_dev):
                         continue
                    lich_link[int(disk_num)] = disk_dev
        return lich_link

    def __get_pool_link(self, pool):
        pool_link = {}
        lich_link = self.__get_lich_link();
        pool_disk = self.get_pool_disk(pool)
        for i in pool_disk:
            if i in lich_link:
                pool_link[i] = lich_link[i]

        return pool_link

    def __disk_set_metawlog(self):
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname

        meta_path = os.path.join(self.config.home, 'data');
        if os.path.ismount(meta_path):
            dev = self.disk.get_dev_bymounted(meta_path)
            fd = open(dev, 'wb')
            fd.seek(VALID_DATA_OFFSET, 0)
            fd.write('cluster=%s;node=%s;type=meta;' % (clusteruuid, hostname))
            fd.close()
            self.disk.set_dev_label(dev, 'lich-meta')

        wlog_path = os.path.join(meta_path, 'wlog')
        if os.path.ismount(wlog_path):
            dev = self.disk.get_dev_bymounted(wlog_path)
            fd = open(dev, 'wb')
            fd.seek(VALID_DATA_OFFSET, 0)
            fd.write('cluster=%s;node=%s;type=wlog;' % (clusteruuid, hostname))
            fd.close()
            self.disk.set_dev_label(dev, 'lich-wlog')

    def __disk_set_clusteruuid(self, items):
        clusteruuid = self.config.getclusteruuid()
        for item in items:
            if not item[1]:
                continue
            dev = item[1]
            fd = open(dev, 'rb')
            fd.seek(VALID_DATA_OFFSET, 0)
            buff = fd.read(VALID_DATA_LEN)
            fd.close()
            m = re.match('cluster=(\w+);node=\w+;', buff)
            if m:
                if m.group(1) == clusteruuid:
                    continue
                buff = buff.replace(m.group(1), clusteruuid)
                fd = open(dev, 'wb')
                fd.seek(VALID_DATA_OFFSET, 0)
                fd.write(buff)
                fd.close()

    def __disk_set_nodename(self, items):
        hostname = self.config.hostname
        for item in items:
            if not item[1]:
                continue
            dev = item[1]
            fd = open(dev, 'rb')
            fd.seek(VALID_DATA_OFFSET, 0)
            buff = fd.read(VALID_DATA_LEN)
            fd.close()
            m = re.match('cluster=\w+;node=(\w+);', buff)
            if m:
                if m.group(1) == hostname:
                    continue
                buff = buff.replace(m.group(1), hostname)
                fd = open(dev, 'wb')
                fd.seek(VALID_DATA_OFFSET, 0)
                fd.write(buff)
                fd.close()

    def disk_set(self, arg):
        lich_disk = self.__get_lich_disk()
        if arg == 'clusteruuid':
            self.__disk_set_clusteruuid(lich_disk)
        elif arg == 'nodename':
            self.__disk_set_nodename(lich_disk)
        elif arg == 'all':
            self.__disk_set_clusteruuid(lich_disk)
            self.__disk_set_nodename(lich_disk)
        elif arg == 'metawlog':
            self.__disk_set_metawlog()
        else:
            raise Exp(errno.EINVAL, '%s is invalid argument, use --help for help' % arg)

    def __disk_load_valid(self, dev):
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname
        retry = 0
        while hostname == 'N/A':
            if retry > 10:
                _syserror(" disk_manage, get hostname fail, please check lich.conf or ifconfig")
                raise Exp(errno.EINVAL, 'get hostname fail, please check lich.conf or ifconfig')

            self.config.refresh()
            hostname = self.config.hostname
            time.sleep(1)
            retry += 1

        bitmap_path = os.path.join(self.config.home, 'data/disk/bitmap')
        info_path = os.path.join(self.config.home, 'data/disk/info')

        try:
            fd = open(dev, 'rb')
            fd.seek(VALID_DATA_OFFSET, 0)
            buff = fd.read(VALID_DATA_LEN)
            fd.close()
            m = re.match('cluster=%s;node=%s;type=(\w+);(disk=(\d+);pool=([^;]+);)?' % (clusteruuid, hostname), buff)
            if m:
                if m.group(1) == 'meta' or m.group(1) == 'wlog':
                    return (m.group(1), dev)
                elif m.group(1) == 'data':
                    disk_num = m.group(3)
                    pool = m.group(4)
                    disk_bitmap = os.path.join(bitmap_path, disk_num + '.bitmap')
                    disk_info = os.path.join(info_path, disk_num + '.info')
                    if not os.path.exists(disk_bitmap) or not os.path.exists(disk_info):
                        return None

                    fd = open(disk_info, 'rb')
                    buff = fd.read()
                    fd.close()

                    fd = open(dev, 'rb')
                    fd.seek(DISK_INFO_OFFSET, 0)
                    buff1 = fd.read(len(buff))
                    fd.close()

                    if buff == buff1:
                        return ('disk' + disk_num, dev, pool)
        except Exception, e:
            pass

        return None

    def __disk_load_search(self):
        lich_disk = []
        all_parts = self.disk.get_all_parts()
        for dev in all_parts:
            try:
                dev_type = self.disk.get_dev_type(dev)
            except:
                continue
            if dev_type == 'ISCSI':
                continue
            match = self.__disk_load_valid('/dev/' + dev)
            if match:
                lich_disk.append(match)
            else:
                for part in all_parts[dev]:
                    match = self.__disk_load_valid('/dev/' + part)
                    if match:
                        lich_disk.append(match)

        return lich_disk

    def __disk_load_check(self, items):
        lich_type = []
        lich_disk = []
        for item in items:
            if not item[1]:
                raise Exp(errno.EINVAL, '%s not specify' % item[0])
            if not item[1].startswith('/dev/'):
                raise Exp(errno.EINVAL, '%s is not block device' % item[1])
            if not self.disk.is_block(item[1]):
                raise Exp(errno.EINVAL, '%s is not block device' % item[1])
            if item[0] == 'meta' or item[0] == 'wlog':
                if item[0] in lich_type:
                    raise Exp(errno.EINVAL, '%s disk repeat' % item[0])
                lich_type.append(item[0])
                if item[1] in lich_disk:
                    raise Exp(errno.EINVAL, '%s repeat used' % item[1])
                lich_disk.append(item[1])
            elif item[0].startswith('disk'):
                m = re.match('disk(\d+)', item[0])
                if m is not None:
                    if int(m.group(1)) > DISK_MAX_NUM:
                        raise Exp(errno.EINVAL, '%s disk number too big' % item[0])
                else:
                    raise Exp(errno.EINVAL, '%s not support' % item[0])
                if item[0] in lich_type:
                    raise Exp(errno.EINVAL, '%s repeat' % item[0])
                lich_type.append(item[0])
                if item[1] in lich_disk:
                    raise Exp(errno.EINVAL, '%s repeat used' % item[1])
                lich_disk.append(item[1])
            else:
                raise Exp(errno.EINVAL, '%s=%s config not support' % (item[0], item[1]))

    def __disk_load_loding(self, items):
        info_path = os.path.join(self.config.home, 'data/disk/info')
        meta_path = os.path.join(self.config.home, 'data')
        if not os.path.exists(meta_path):
            os.mkdir(meta_path)

        lich_items = self.__get_lich_disk()
        cached_items = self.__get_lich_writeback_fromdata()

        '''
        #cached disk not found in system
        '''
        '''
        for item in cached_items:
            if not cached_items[item]['cached']:
                continue

            disk = 'disk' + str(item)
            found = False
            for i in items:
                if i[0] == disk:
                    found = True
                    break

            if not found:
                _syswarn(" disk_manage, %s cached but not found in system" % (disk))
                raise Exp(errno.EPERM, '%s cached but not found in system' % (disk))

            found = False
            for i in lich_items:
                if i[0] == disk:
                    found = True
                    break

            if not found:
                _syswarn(" disk_manage, %s cached but not found in system" % (disk))
                raise Exp(errno.EPERM, '%s cached but not found in system' % (disk))
        '''

        '''
        #lich disk not found in system
        '''
        for item in lich_items:
            found = False
            for i in items:
                if i[0] == item[0]:
                    found = True
                    break
            if (found):
                continue
            if item[1] is None:
                continue
            if (item[0] == 'meta' or item[0] == 'wlog'):
                raise Exp(errno.EPERM, '%s not mount to %s' % (item[1], item[0]))
            if item[0].startswith('disk'):
                disk_num = re.match('(\D+)(\d+)', item[0]).group(2)
                disk_info = os.path.join(info_path, disk_num + ".info")
                if not os.path.exists(disk_info):
                    continue

                disk_path = os.path.join(self.disk_home, disk_num + ".disk")
                _syswarn(" disk_manage, %s(%s) not found in system" % (item[0], item[1]))
                dwarn("lich disk %s(%s) not found in system" % (item[0], item[1]))
                _exec_system("rm -rf %s" % disk_path)
                self.__disk_del_info(disk_num)
            else:
                raise Exp(errno.EPERM, 'unknow %s(%s)' % (item[0], item[1]))

        '''
        #mount meta disk
        '''
        load_meta = False
        for item in items:
            if item[0] == 'meta':
                dev = self.disk.get_dev_bymounted(meta_path)
                if dev == item[1]:
                    load_wlog = True
                    continue
                elif dev is not None:
                    dwarn("%s mounted by %s not %s!"%(meta_path, dev, item[1]))
                    continue
                try:
                    self.disk.dev_mount(item[1], meta_path)
                    load_meta = True
                except Exp, e:
                    dwarn(e.err)
        #if not load_meta:
        #    dwarn("%s not mount to diskx" % meta_path)

        '''
        #mount wlog disk
        '''
        if self.config.writeback:
            wlog_path = os.path.join(meta_path, 'wlog')
            if not os.path.exists(wlog_path):
                os.mkdir(wlog_path)
            load_wlog = False
            for item in items:
                if item[0] == 'wlog':
                    dev = self.disk.get_dev_bymounted(wlog_path)
                    if dev == item[1]:
                        load_wlog = True
                        continue
                    elif dev is not None:
                        dwarn("%s mounted by %s not %s!"%(wlog_path, dev, item[1]))
                        continue
                    try:
                        self.disk.dev_mount(item[1], wlog_path)
                        load_wlog = True
                    except Exp, e:
                        dwarn(e.err)
            if not load_wlog:
                dwarn("%s not mount to disk" % wlog_path)

        if not os.path.exists(self.disk_home):
            os.makedirs(self.disk_home)
        else:
            pass

        '''
        #diskname changed, add disk again
        '''
        need_reset = False
        for item in items:
            if item[0].startswith('disk'):
                right = False
                for lich_item in lich_items:
                    if lich_item[0] == item[0] and lich_item[1] == item[1]:
                        right = True
                        break

                if right:
                    continue

                need_reset = True
                disk_num = re.match('(\D+)(\d+)', item[0]).group(2)
                lich_disk = str(disk_num) + ".disk"
                disk_path = os.path.join(self.disk_home, lich_disk)
                dev = item[1]
                pool = item[2]

                if self.__get_lich_writeback_cache(dev) != 0:
                    _syswarn(" lich disk %s(%s) not found in lich, but cached, so skip" % (item[0], item[1]))
                    dwarn(" lich disk %s(%s) not found in lich, but cached, so skip" % (item[0], item[1]))
                    continue

                _syswarn(" disk_manage, %s(%s) not found in lich" % (item[0], item[1]))
                dwarn(" lich disk %s(%s) not found in lich" % (item[0], item[1]))
                _exec_system("rm -rf %s" % disk_path)
                self.__disk_del_info(disk_num)

                try:
                    self.__disk_add_tier(pool, dev, disk_num)
                except:
                    derror("%s add tier failed." % dev)
                    continue

                try:
                    self.__disk_add_link(dev, disk_num, pool)
                except:
                    derror("%s add link failed." % dev)
                    raise

        if (need_reset):
            self.__disk_add_reset()

    def __disk_load_auto(self):
        if self.disk_search is None:
            self.disk_search = self.__disk_load_search()
        self.__disk_load_check(self.disk_search)
        self.__disk_load_loding(self.disk_search)

    def __disk_load_conf(self, conf):
        if not os.path.exists(conf):
            raise Exp(errno.EINVAL, '%s not exists' % conf)

        cf = ConfigParser.ConfigParser()
        cf.read(conf)
        items = cf.items("disk")

        self.__disk_load_check(items)
        self.__disk_load_loding(items)

    def __disk_load_args(self, args):
        for arg in args:
            if '=' not in arg:
                raise Exp(errno.EINVAL, '%s is invalid argument, use --help for help' % arg)
            if not arg.startswith('meta=') and \
                not arg.startswith('wlog=') and \
                not arg.startswith('disk'):
                    raise Exp(errno.EINVAL, '%s is invalid argument, use --help for help' % arg)

        items = []
        for arg in args:
            items.append(tuple(arg.split('=')))

        self.__disk_load_check(items)
        self.__disk_load_loding(items)

    def disk_load(self, args=None):
        if (self.config.testing):
            return
        if not args or len(args) == 0:
            return self.__disk_load_auto()
        elif len(args) == 1 and args[0].startswith('conf='):
            return self.__disk_load_conf(args[0].split('=')[-1])
        else:
            return self.__disk_load_args(args)

    def __disk_check_env(self):
        pass

    def __get_all_disk(self, is_all):
        all_disk = {}
        all_devs = []
        sys_dev = []

        try:
            all_devs = self.disk.get_all_devs()
            sys_dev = self.disk.get_sys_dev(True)
        except:
            pass

        for dev in all_devs:
            dev = '/dev/' + dev
            try:
                dev_type = self.disk.get_dev_type(dev)
            except:
                continue
            if dev_type == "ISCSI":
                continue
            if dev_type == "UNKNOW" and not is_all:
                continue
            if self.disk.is_swap(dev):
                continue
            if dev  in sys_dev:
                continue

            all_disk[dev] = {}

            dev_parts = self.disk.get_dev_parts(dev)
            if len(dev_parts) != 0:
                all_disk[dev]['part_info'] = {}
            for part in dev_parts:
                part = '/dev/' + part
                if part in sys_dev:
                    continue
                try:
                    all_disk[dev]['part_info'][part] = {}
                except Exception, e:
                    pass

        return all_disk

    def __disk_list_getall(self, is_all=False):
        if not self.raid:
            self.raid = RAID(self.config)
        all_disk_json = {}
        sys_dev = []
        lich_items = self.__get_lich_disk()
        lich_disk = [dev for (x, dev, pool) in lich_items if dev]
        lich_dev = self.__get_lich_dev()
        #disk_stat = self.__get_disk_stat(lich_items)
        all_disk = self.__get_all_disk(is_all)
        try:
            sys_dev = self.disk.get_sys_dev()
        except:
            pass

        for dev in all_disk:
            dev_info = self.disk.get_dev_info(dev)
            if dev_info['type'] == 'RAID':
                try:
                    raid_info = self.raid.raid_info(dev)
                    if raid_info is not None:
                        disk_info = self.raid.disk_info(raid_info['disk'][0])
                        if 'curr_temp' not in disk_info:
                            disk_info['curr_temp'] = '0'
                        if 'max_temp' not in disk_info:
                            disk_info['max_temp'] = '0'
                except:
                    raid_info = None

            if dev in lich_disk:
                all_disk_json[dev] = all_disk[dev]
                all_disk_json[dev]['dev_info'] = dev_info
                if dev_info['type'] == 'RAID' and raid_info is not None:
                    all_disk_json[dev]['raid_info'] = raid_info
                    all_disk_json[dev]['dev_info']['media_type'] = disk_info['media_type']
                    all_disk_json[dev]['dev_info']['interface'] = disk_info['interface']
                    all_disk_json[dev]['dev_info']['curr_temp'] = disk_info['curr_temp']
                    all_disk_json[dev]['dev_info']['max_temp'] = disk_info['max_temp']
                all_disk_json[dev]['flag'] = 'lich'
                all_disk_json[dev]['mode'] = 'dev'
                for (item_type, item_dev, item_pool) in lich_items:
                    if item_dev == dev:
                        all_disk_json[dev]['type'] = item_type
                        all_disk_json[dev]['pool'] = item_pool
                        #if item_type in disk_stat:
                        #    all_disk_json[dev]['disk_stat'] = disk_stat[item_type]
                continue
            if 'part_info' in all_disk[dev].keys():
                for part in all_disk[dev]['part_info']:
                    if part in lich_disk:
                        all_disk_json[part] = {}
                        all_disk_json[part]['dev_info'] = self.disk.get_part_info(part)
                        all_disk_json[part]['flag'] = 'lich'
                        all_disk_json[part]['mode'] = 'part'
                        #for (item_type, item_dev, item_pool) in lich_items:
                        #    if item_dev == part:
                        #        all_disk_json[part]['type'] = item_type
                        #        all_disk_json[part]['pool'] = item_pool
                        #        if item_type in disk_stat:
                        #            all_disk_json[part]['disk_stat'] = disk_stat[item_type]
                        all_disk_json[part]['dev_info']['cache'] = dev_info['cache']
                        if dev_info['type'] == 'RAID' and raid_info is not None:
                            all_disk_json[part]['raid_info'] = raid_info
                            all_disk_json[part]['dev_info']['media_type'] = disk_info['media_type']
                            all_disk_json[part]['dev_info']['interface'] = disk_info['interface']
                            all_disk_json[part]['dev_info']['curr_temp'] = disk_info['curr_temp']
                            all_disk_json[part]['dev_info']['max_temp'] = disk_info['max_temp']

            if dev in sys_dev:
                if not is_all:
                    continue
                all_disk_json[dev] = all_disk[dev]
                all_disk_json[dev]['dev_info'] = dev_info
                all_disk_json[dev]['flag'] = 'sys'
                all_disk_json[dev]['mode'] = 'dev'
                if 'part_info' in all_disk[dev].keys():
                    for part in all_disk[dev]['part_info']:
                        all_disk_json[dev]['part_info'][part] = self.disk.get_part_info(part)
                if dev_info['type'] == 'RAID' and raid_info is not None:
                    all_disk_json[dev]['raid_info'] = raid_info
                    all_disk_json[dev]['dev_info']['media_type'] = disk_info['media_type']
                    all_disk_json[dev]['dev_info']['interface'] = disk_info['interface']
                    all_disk_json[dev]['dev_info']['curr_temp'] = disk_info['curr_temp']
                    all_disk_json[dev]['dev_info']['max_temp'] = disk_info['max_temp']
                continue
            elif dev in lich_dev:
                continue
            else:
                all_disk_json[dev] = all_disk[dev]
                all_disk_json[dev]['dev_info'] = dev_info
                all_disk_json[dev]['flag'] = 'new'
                all_disk_json[dev]['mode'] = 'dev'
                if 'part_info' in all_disk[dev].keys():
                    for part in all_disk[dev]['part_info']:
                        all_disk_json[dev]['part_info'][part] = self.disk.get_part_info(part)
                if dev_info['type'] == 'RAID' and raid_info is not None:
                    all_disk_json[dev]['raid_info'] = raid_info
                    all_disk_json[dev]['dev_info']['media_type'] = disk_info['media_type']
                    all_disk_json[dev]['dev_info']['interface'] = disk_info['interface']
                    all_disk_json[dev]['dev_info']['curr_temp'] = disk_info['curr_temp']
                    all_disk_json[dev]['dev_info']['max_temp'] = disk_info['max_temp']

        new_raid_disk = self.raid.disk_list()
        if new_raid_disk:
            for adp, disks in new_raid_disk.iteritems():
                for disk in disks:
                    all_disk_json[disks[disk]['inq']] = {}
                    all_disk_json[disks[disk]['inq']]['flag'] = 'new'
                    all_disk_json[disks[disk]['inq']]['mode'] = 'disk'
                    all_disk_json[disks[disk]['inq']]['raid_info'] = disks[disk]

        return all_disk_json

    def __disk_list_getusable(self, all_disk, force):
        usable_disk = []
        for disk in all_disk:
            if all_disk[disk]['flag'] == 'new':
                if all_disk[disk]['mode'] == 'disk':
                    continue
                if self.disk.dev_check_mounted(disk) and not force:
                    continue
                if 'part_info' in all_disk[disk] and not force:
                    continue
                dev_parts = self.disk.get_dev_parts(disk)
                if len(dev_parts) and not force:
                    continue
                if all_disk[disk]['dev_info']['type'] == 'RAID':
                    if 'raid_info' in all_disk[disk]:
                        if all_disk[disk]['raid_info']['raid'] != '0' and not force:
                            continue
                elif all_disk[disk]['dev_info']['cache'] is None and not force:
                    continue
                usable_disk.append(disk)
        return usable_disk

    def __show_sys_raid(self, sys_raid):
        self.__show_unused_raid(sys_raid, {})

    def __show_sys_dev(self, sys_dev):
        self.__show_unused_dev(sys_dev, {})

    def __show_used_raid(self, used_raid):
        for adp in used_raid:
            print("RAID Adapter #%s:%s" %(adp, used_raid[adp]['adp_name']))
            for dev in used_raid[adp]['adp_dev']:
                if used_raid[adp]['adp_dev'][dev]['mode'] == 'dev':
                    lich_type = used_raid[adp]['adp_dev'][dev]['type']
                    lich_pool = used_raid[adp]['adp_dev'][dev]['pool']
                    raid_type = used_raid[adp]['adp_dev'][dev]['dev_info']['type'] + used_raid[adp]['adp_dev'][dev]['raid_info']['raid']
                    media_type = used_raid[adp]['adp_dev'][dev]['dev_info']['media_type']
                    interface = used_raid[adp]['adp_dev'][dev]['dev_info']['interface']
                    size = used_raid[adp]['adp_dev'][dev]['dev_info']['size']
                    if 'disk_stat' in used_raid[adp]['adp_dev'][dev]:
                        free = (int(used_raid[adp]['adp_dev'][dev]['disk_stat']['total']) -
                                int(used_raid[adp]['adp_dev'][dev]['disk_stat']['used'])) * (1024*1024)
                        free += (int(used_raid[adp]['adp_dev'][dev]['disk_stat']['wbtotal']) -
                                int(used_raid[adp]['adp_dev'][dev]['disk_stat']['wbused'])) * (1024*1024)
                        free = _human_readable(free)
                    else:
                        free = used_raid[adp]['adp_dev'][dev]['dev_info']['free']
                    mount = used_raid[adp]['adp_dev'][dev]['dev_info']['mount']

                    cache = ''
                    if 'smart_path' in used_raid[adp]['adp_dev'][dev]['raid_info']:
                        if used_raid[adp]['adp_dev'][dev]['raid_info']['smart_path'] == 'enable':
                            cache = 'cache:smart_path'

                    if cache == '':
                        cache = 'raid_cache:' + used_raid[adp]['adp_dev'][dev]['raid_info']['raid_cache']
                        cache += ' disk_cache:' + used_raid[adp]['adp_dev'][dev]['raid_info']['disk_cache']

                    label = used_raid[adp]['adp_dev'][dev]['dev_info']['label']
                    info = ''
                    #if lich_type.startswith('disk'):
                    #    info += 'tier:' + used_raid[adp]['adp_dev'][dev]['tier']
                    #    if 'disk_stat' in used_raid[adp]['adp_dev'][dev]:
                    #        if used_raid[adp]['adp_dev'][dev]['disk_stat']['cache'] != '0':
                    #            info += ' writeback:' + _human_readable(int(used_raid[adp]['adp_dev'][dev]['disk_stat']['wbtotal']) * (1024*1024))
                    #            if used_raid[adp]['adp_dev'][dev]['disk_stat']['cached'] == '1':
                    #                info += ',Enabled'
                    #            else:
                    #                info += ',Disabled'
                    #    else:
                    #        if used_raid[adp]['adp_dev'][dev]['writeback']['cache'] != '0':
                    #            info += ' writeback:' + _human_readable(_human_unreadable(size) * \
                    #                    int(used_raid[adp]['adp_dev'][dev]['writeback']['cache']) / 100)
                    #            if used_raid[adp]['adp_dev'][dev]['writeback']['cached'] == 1:
                    #                info += ',Enabled'
                    #            else:
                    #                info += ',Disabled'
                    if label and label != '':
                        info += ' label:' + label + ' '
                    info += ' ' + cache
                    print("  %-6s %-6s %-9s %-9s %-4s %-4s %-9s free:%-8s %s"
                            %(lich_type, dev, lich_pool, raid_type, media_type, interface, size, free, info))
                else:
                    lich_type = used_raid[adp]['adp_dev'][dev]['type']
                    lich_pool = used_raid[adp]['adp_dev'][dev]['pool']
                    media_type = used_raid[adp]['adp_dev'][dev]['dev_info']['media_type']
                    interface = used_raid[adp]['adp_dev'][dev]['dev_info']['interface']
                    size = used_raid[adp]['adp_dev'][dev]['dev_info']['size']
                    fs = used_raid[adp]['adp_dev'][dev]['dev_info']['fs']
                    mount = used_raid[adp]['adp_dev'][dev]['dev_info']['mount']
                    info = ''
                    #if lich_type.startswith('disk'):
                    #    info += 'tier:' + used_raid[adp]['adp_dev'][dev]['tier']
                    #    if 'disk_stat' in used_raid[adp]['adp_dev'][dev]:
                    #        if used_raid[adp]['adp_dev'][dev]['disk_stat']['cache'] != '0':
                    #            info += ' writeback:' + _human_readable(int(used_raid[adp]['adp_dev'][dev]['disk_stat']['wbtotal']) * (1024*1024))
                    #            if used_raid[adp]['adp_dev'][dev]['disk_stat']['cached'] == '1':
                    #                info += ',Enabled'
                    #            else:
                    #                info += ',Disabled'
                    #    else:
                    #        if used_raid[adp]['adp_dev'][dev]['writeback']['cache'] != '0':
                    #            info += ' writeback:' + _human_readable(_human_unreadable(size) * \
                    #                    int(used_raid[adp]['adp_dev'][dev]['writeback']['cache']) / 100)
                    #            if used_raid[adp]['adp_dev'][dev]['writeback']['cached'] == 1:
                    #                info += ',Enabled'
                    #            else:
                    #                info += ',Disabled'
                    print("  %-6s %-6s %-9s %-4s %-4s Partition %-9s %-13s %s" %
                            (lich_type, dev, lich_pool, media_type, interface, size, fs, info))


    def __show_used_dev(self, used_dev):
        for dev in used_dev:
            if used_dev[dev]['mode'] == 'dev':
                lich_type = used_dev[dev]['type']
                lich_pool = used_dev[dev]['pool']
                disk_type = used_dev[dev]['dev_info']['type']
                size = used_dev[dev]['dev_info']['size']
                if 'disk_stat' in used_dev[dev]:
                    free = (int(used_dev[dev]['disk_stat']['total']) -
                            int(used_dev[dev]['disk_stat']['used'])) * (1024*1024)
                    free += (int(used_dev[dev]['disk_stat']['wbtotal']) -
                            int(used_dev[dev]['disk_stat']['wbused'])) * (1024*1024)
                    free = _human_readable(free)
                else:
                    free = used_dev[dev]['dev_info']['free']
                mount = used_dev[dev]['dev_info']['mount']
                cache = used_dev[dev]['dev_info']['cache']
                label = used_dev[dev]['dev_info']['label']
                info = ''
                #if lich_type.startswith('disk'):
                #    info += 'tier:' + used_dev[dev]['tier']
                #    if 'disk_stat' in used_dev[dev]:
                #        if used_dev[dev]['disk_stat']['cache'] != '0':
                #            info += ' writeback:' + _human_readable(int(used_dev[dev]['disk_stat']['wbtotal']) * (1024*1024))
                #            if used_dev[dev]['disk_stat']['cached'] == '1':
                #                info += ',Enabled'
                #            else:
                #                info += ',Disabled'
                #    else:
                #        if used_dev[dev]['writeback']['cache'] != '0':
                #            info += ' writeback:' + _human_readable(_human_unreadable(size) * \
                #                    int(used_dev[dev]['writeback']['cache']) / 100)
                #            if used_dev[dev]['writeback']['cached'] == 1:
                #                info += ',Enabled'
                #            else:
                #                info += ',Disabled'
                if cache is None:
                    info += ' cache:Notsupport'
                else:
                    info += ' cache:' + cache
                if label is not None and label != '':
                    info += ' label:' + label
                print("%-6s %-6s %-9s %-9s %-9s free:%-8s %s" %(lich_type, dev, lich_pool, disk_type, size, free, info))
            else:
                lich_type = used_dev[dev]['type']
                lich_pool = used_dev[dev]['pool']
                size = used_dev[dev]['dev_info']['size']
                fs = used_dev[dev]['dev_info']['fs']
                mount = used_dev[dev]['dev_info']['mount']
                info = ''
                dev_type = self.disk.get_dev_type(self.disk.get_dev(dev))
                #if lich_type.startswith('disk'):
                #    info += 'tier:' + used_dev[dev]['tier']
                #    if 'disk_stat' in used_dev[dev]:
                #        if used_dev[dev]['disk_stat']['cache'] != '0':
                #            info += ' writeback:' + _human_readable(int(used_dev[dev]['disk_stat']['wbtotal']) * (1024*1024))
                #            if used_dev[dev]['disk_stat']['cached'] == '1':
                #                info += ',Enabled'
                #            else:
                #                info += ',Disabled'
                #    else:
                #        if used_dev[dev]['writeback']['cache'] != '0':
                #            info += ' writeback:' + _human_readable(_human_unreadable(size) * \
                #                    int(used_dev[dev]['writeback']['cache']) / 100)
                #            if used_dev[dev]['writeback']['cached'] == 1:
                #                info += ',Enabled'
                #            else:
                #                info += ',Disabled'
                print("%-6s %-6s %-9s %s(Part) %-9s %-13s %s" %(lich_type, dev, lich_pool, dev_type, size, fs, info))

    def __show_unused_raid(self, unused_raid, usable_disk):
        for adp in unused_raid:
            print("RAID Adapter #%s:%s" %(adp, unused_raid[adp]['adp_name']))
            for dev in unused_raid[adp]['adp_dev']:
                if unused_raid[adp]['adp_dev'][dev]['mode'] == 'dev':
                    type = unused_raid[adp]['adp_dev'][dev]['dev_info']['type'] + unused_raid[adp]['adp_dev'][dev]['raid_info']['raid']
                    media_type = unused_raid[adp]['adp_dev'][dev]['dev_info']['media_type']
                    interface = unused_raid[adp]['adp_dev'][dev]['dev_info']['interface']
                    size = unused_raid[adp]['adp_dev'][dev]['dev_info']['size']
                    #free = unused_raid[adp]['adp_dev'][dev]['dev_info']['free']
                    mount = unused_raid[adp]['adp_dev'][dev]['dev_info']['mount']

                    cache = ''
                    if 'smart_path' in unused_raid[adp]['adp_dev'][dev]['raid_info']:
                        if unused_raid[adp]['adp_dev'][dev]['raid_info']['smart_path'] == 'enable':
                            cache = 'cache:smart_path'

                    if cache == '':
                        cache = 'raid_cache:' + unused_raid[adp]['adp_dev'][dev]['raid_info']['raid_cache']
                        cache += ' disk_cache:' + unused_raid[adp]['adp_dev'][dev]['raid_info']['disk_cache']

                    label = unused_raid[adp]['adp_dev'][dev]['dev_info']['label']
                    info = ''
                    if mount is not None:
                        info += 'mount:' + mount + ' '
                    info += cache
                    if label is not None and label != '':
                        info += ' label:' + label
                    if dev in usable_disk:
                        info += ' (usable)'
                    print("  %-6s %-9s %-4s %-4s %-9s %s" %(dev, type, media_type, interface, size, info))
                    if 'part_info' in unused_raid[adp]['adp_dev'][dev].keys():
                        for part in sorted(unused_raid[adp]['adp_dev'][dev]['part_info']):
                            type = unused_raid[adp]['adp_dev'][dev]['part_info'][part]['type']
                            if type == 'extended':
                                print("    %s extended" % part)
                            else:
                                size = unused_raid[adp]['adp_dev'][dev]['part_info'][part]['size']
                                fs = unused_raid[adp]['adp_dev'][dev]['part_info'][part]['fs']
                                mount = unused_raid[adp]['adp_dev'][dev]['part_info'][part]['mount']
                                info = ''
                                if mount is not None:
                                    info += mount
                                if fs is None:
                                    fs = ''
                                print("    %-4s %-9s %-9s %-13s %s" %(part, type, size, fs, info))
            for dev in unused_raid[adp]['adp_dev']:
                if unused_raid[adp]['adp_dev'][dev]['mode'] == 'disk':
                    type = unused_raid[adp]['adp_dev'][dev]['raid_info']['media_type']
                    size = unused_raid[adp]['adp_dev'][dev]['raid_info']['size']
                    slot = unused_raid[adp]['adp_dev'][dev]['raid_info']['slot']
                    foreign = unused_raid[adp]['adp_dev'][dev]['raid_info']['foreign']
                    if foreign == 'None':
                        foreign = ''
                    else:
                        foreign = '(' + foreign + ')'
                    print("  inq:%-30s type:%s size:%s slot:%s  %s" %(dev, type, size, slot, foreign))

    def __show_unused_dev(self, unused_dev, usable_disk):
        for dev in unused_dev:
            type = unused_dev[dev]['dev_info']['type']
            size = unused_dev[dev]['dev_info']['size']
            #free = unused_dev[dev]['dev_info']['free']
            mount = unused_dev[dev]['dev_info']['mount']
            cache = unused_dev[dev]['dev_info']['cache']
            label = unused_dev[dev]['dev_info']['label']
            info = ''
            if mount is not None:
                info += 'mount:' + mount + ' '
            if cache is None:
                info += 'cache:Notsupport'
            else:
                info += 'cache:' + cache
            if dev in usable_disk:
                info += ' (usable)'
            print("%-6s %-9s %-9s %s" %(dev, type, size, info))
            if 'part_info' in unused_dev[dev].keys():
                for part in sorted(unused_dev[dev]['part_info']):
                    type = unused_dev[dev]['part_info'][part]['type']
                    if type == 'extended':
                        print("  %-6s extended" %(dev))
                    else:
                        size = unused_dev[dev]['part_info'][part]['size']
                        fs = unused_dev[dev]['part_info'][part]['fs']
                        mount = unused_dev[dev]['part_info'][part]['mount']
                        info = ''
                        if mount is not None:
                            info += mount
                        print("  %-4s %-9s %-9s %-13s %s" %(part, type, size, fs, info))

    def __disk_list_showall(self, all_disk):
        sys_disk = {}
        sys_raid = {}
        sys_dev = {}
        used_disk = {}
        used_raid = {}
        used_dev = {}
        unused_disk = {}
        unused_raid = {}
        unused_dev = {}

        usable_disk = self.__disk_list_getusable(all_disk, False)
        #lich_tier = self.__get_lich_tier()
        #lich_writeback = self.__get_lich_writeback()

        for disk in all_disk:
            if all_disk[disk]['flag'] == 'lich':
                used_disk[disk] = all_disk[disk]
            elif all_disk[disk]['flag'] == 'new':
                unused_disk[disk] = all_disk[disk]
            elif all_disk[disk]['flag'] == 'sys':
                sys_disk[disk] = all_disk[disk]

        if len(sys_disk) != 0:
            print("sysdev:")
            for disk in sys_disk:
                if 'raid_info' in sys_disk[disk].keys():
                    adp = sys_disk[disk]['raid_info']['adp']
                    if adp not in sys_raid.keys():
                        sys_raid[adp] = {}
                        sys_raid[adp]['adp_name'] = sys_disk[disk]['raid_info']['adp_name']
                        sys_raid[adp]['adp_dev'] = {}
                    sys_raid[adp]['adp_dev'][disk] = sys_disk[disk]
                else:
                    sys_dev[disk] = sys_disk[disk]

            self.__show_sys_dev(sys_dev)
            self.__show_sys_raid(sys_raid)

        if len(used_disk) != 0:
            print("used:")
            for disk in used_disk:
                '''
                if used_disk[disk]['type'].startswith('disk'):
                    disk_num = int(used_disk[disk]['type'][4:])
                    if disk_num in lich_tier:
                        used_disk[disk]['tier'] = str(lich_tier[disk_num])
                    else:
                        used_disk[disk]['tier'] = 'None'
                    if disk_num in lich_writeback:
                        used_disk[disk]['writeback'] = lich_writeback[disk_num]
                    else:
                        used_disk[disk]['writeback'] = {'cache':0, 'cached':0}
                '''
                if 'raid_info' in used_disk[disk].keys():
                    adp = used_disk[disk]['raid_info']['adp']
                    if adp not in used_raid.keys():
                        used_raid[adp] = {}
                        used_raid[adp]['adp_name'] = used_disk[disk]['raid_info']['adp_name']
                        used_raid[adp]['adp_dev'] = {}
                    used_raid[adp]['adp_dev'][disk] = used_disk[disk]
                else:
                    used_dev[disk] = used_disk[disk]

            self.__show_used_dev(used_dev)
            self.__show_used_raid(used_raid)

        if len(unused_disk) != 0:
            print("unused:")
            for disk in unused_disk:
                if 'raid_info' in unused_disk[disk].keys():
                    adp = unused_disk[disk]['raid_info']['adp']
                    if adp not in unused_raid.keys():
                        unused_raid[adp] = {}
                        unused_raid[adp]['adp_name'] = unused_disk[disk]['raid_info']['adp_name']
                        unused_raid[adp]['adp_dev'] = {}
                    unused_raid[adp]['adp_dev'][disk] = unused_disk[disk]
                else:
                    unused_dev[disk] = unused_disk[disk]

            self.__show_unused_dev(unused_dev, usable_disk)
            self.__show_unused_raid(unused_raid, usable_disk)

    def disk_list(self, is_all, is_json, verbose):
        self.__disk_check_env()
        all_disk = self.__disk_list_getall(is_all)
        if is_json:
            print json.dumps(all_disk)
        elif verbose:
            print json.dumps(all_disk, sort_keys=False, indent=4)
        else:
            self.__disk_list_showall(all_disk)

    def disk_list_with_return_json_value(self):
        self.__disk_check_env()
        all_disk = self.__disk_list_getall()
        return json.dumps(all_disk)

    def disk_speed(self, devs):
        for dev in devs:
            self.disk.get_dev_speed(dev, True)

    def __disk_add_check(self, devs, force, cache, pool):
        instance = self.node.instences[0]
        if not instance.running():
            raise Exp(errno.EINVAL, 'lichd not running')

        self.__disk_check_env()
        lich_disk = self.__get_lich_disk()
        sys_dev = self.disk.get_sys_dev(False)
        sys_dev_part = self.disk.get_sys_dev(True)
        add_disk = []
        for dev in devs:
            if self.disk.dev_check_mounted(dev) and not force:
                raise Exp(errno.EINVAL, '%s or partition was mounted, please use --force' % dev)

            if not dev.startswith('/dev/'):
                raise Exp(errno.EINVAL, '%s is not block device' % dev)
            if not self.disk.is_block(dev):
                raise Exp(errno.EINVAL, '%s is not block device' % dev)
            if dev in [disk[1] for disk in lich_disk]:
                raise Exp(errno.EINVAL, '%s already used by lich' % dev)
            if dev in sys_dev or dev in sys_dev_part:
                raise Exp(errno.EINVAL, '%s mounted on /' % dev)
            if self.disk.is_mounted(dev) is not None and not force:
                raise Exp(errno.EINVAL, '%s mounted, please use --force' % dev)

            if self.disk.is_part(dev) and not force:
                raise Exp(errno.EINVAL, 'dev %s is a partition' %(dev))

            dev_parts = self.disk.get_dev_parts(dev)
            if len(dev_parts) and not force:
                raise Exp(errno.EINVAL, 'dev %s has partitions %s' %(dev, dev_parts))

            dev_info = self.disk.get_dev_info(dev)
            dev_type = dev_info['type']
            if dev_type == 'ISCSI':
                raise Exp(errno.EINVAL, 'can not add disk %s type(%s) to lich' % (dev, dev_type))
            if dev_type == 'UNKNOW' and not force:
                raise Exp(errno.EINVAL, 'can not add disk %s type(%s) to lich' % (dev, dev_type))
            if dev_type == 'RAID':
                if not force and \
                        not dev_info['raidcard'].startswith('LSI') and \
                        not dev_info['raidcard'].startswith('Hewlett-Packard'):
                    raise Exp(errno.EINVAL, 'not support %s raidcard: %s' %(dev, dev_info['raidcard']))
                if not self.raid:
                    self.raid = RAID(self.config)

                dev_info = self.raid.raid_info(dev)
                disk_info = self.raid.disk_info(dev_info['disk'][0])
                disk_type = disk_info['media_type']
                if cache != 0 and disk_type != 'SSD' and not force:
                    raise Exp(errno.EINVAL, 'disk %s type %s can not be set to cache' %(dev, disk_type))

                if self.cacheconf is None:
                    self.cacheconf = self.parse_cacheconf(self.config.cacheconf, devs)
                if disk_type == 'SSD':
                    self.raid.raid_check([self.disk.get_dev(dev)], self.cacheconf, True)
                else:
                    self.raid.raid_check([self.disk.get_dev(dev)], self.cacheconf, force)
            else:
                if dev_info['type'] != 'NVMe' and not force and \
                        dev_info['cache'] != 'Enabled' and \
                        dev_info['cache'] != 'Disabled':
                    raise Exp(errno.EINVAL, 'not support %s cache: %s' %(self.disk.get_dev(dev), dev_info['cache']))
                if cache != 0 and dev_type != 'SSD' and not force:
                    raise Exp(errno.EINVAL, 'disk %s type %s can not be set to cache' %(dev, dev_type))

                self.__disk_check_cache({dev_type:[self.disk.get_dev(dev)]}, True)
            if dev in add_disk:
                raise Exp(errno.EINVAL, '%s repeat' % dev)
            else:
                add_disk.append(dev)

        try:
            self.__check_lich_speed()
            self.__check_lich_rotation()
        except:
            if not force:
                raise

        pool_list = self.pool_manage.pool_list()
        if pool != "default" and pool not in pool_list:
            raise Exp(errno.EPERM, "pool %s not found" % pool)
        elif pool == "default" and pool not in pool_list:
            self.pool_manage.pool_create(pool)

    def __disk_tier_adjust(self, pool, disk_num = None, disk_speed = None, p=False):
        lich_tier = self.__get_lich_tier()
        lich_speed = self.__get_lich_speed()
        lich_mask = {}
        idx_tier = -1
        cur_tier = -1

        for disk in lich_tier:
            if disk in lich_speed:
                lich_mask[disk] = self.__get_disk_tiermask(lich_speed[disk])

        if disk_num is not None and disk_speed is not None:
            lich_mask[disk_num] = self.__get_disk_tiermask(disk_speed)
        lich_mask = self.__get_disk_tier(lich_mask)

        sort_tier = sorted(lich_mask.items(), key=lambda d: d[1])
        for disk in sort_tier:
            tier = disk[1]
            if tier != cur_tier:
                cur_tier = tier
                if idx_tier != DISK_MAX_TIER:
                    idx_tier += 1
            lich_tier[disk[0]] = idx_tier

        if p:
            print "tier:", sorted(lich_tier.items(), key=lambda d: d[1])
        return lich_tier

    def __disk_tier_adjust_new(self, pool, disk_num = None, disk_speed = None, disk_rotation = None, p=False):
        lich_link = self.__get_pool_link(pool)
        lich_tier = self.__get_pool_tier(pool)
        lich_speed = self.__get_pool_speed(pool)
        lich_rotation = self.__get_pool_rotation(pool)
        lich_mask = {}
        idx_tier = -1
        cur_tier = -1

        if disk_num is not None and disk_speed is not None:
            lich_speed[disk_num] = disk_speed
            lich_link[disk_num] = None
        if disk_num is not None and disk_rotation is not None:
            lich_rotation[disk_num] = disk_rotation
            lich_link[disk_num] = None

        lich_mask = self.__get_disk_tiermask_withspeed(lich_speed)
        #if lich_mask.keys() == lich_link.keys() and \
        #        lich_rotation.keys() == lich_link.keys():
        #    lich_mask = self.__get_disk_tiermask_withrotation(lich_mask, lich_rotation)
        lich_mask = self.__get_disk_tier_withmask(lich_mask)

        sort_tier = sorted(lich_mask.items(), key=lambda d: d[1])
        for disk in sort_tier:
            tier = disk[1]
            if tier != cur_tier:
                cur_tier = tier
                if idx_tier != DISK_MAX_TIER:
                    idx_tier += 1
            lich_tier[disk[0]] = idx_tier

        if p:
            print "tier:", sorted(lich_tier.items(), key=lambda d: d[1])
        return lich_tier


    def __disk_tier_update(self, lich_tier):
        for disk in lich_tier:
            _exec_system("echo %d > %s" %(lich_tier[disk], os.path.join(self.tier_path, str(disk) + ".tier")), False)

    def __disk_add_speed(self, disk_num, disk_speed):
        _exec_system("echo %d >  %s" % (disk_speed, os.path.join(self.speed_path, str(disk_num) + ".speed")))

    def __disk_add_rotation(self, disk_num, disk_rotation):
        if disk_rotation is not None:
            _exec_system("echo %d >  %s" % (disk_rotation, os.path.join(self.rotation_path, str(disk_num) + ".rotation")))

    def __disk_add_tier_withspeed(self, pool, disk_num, disk_speed, disk_rotation):
        lich_tier = self.__disk_tier_adjust_new(pool, disk_num, disk_speed, disk_rotation, True)
        self.__disk_tier_update(lich_tier)
        self.__disk_add_speed(disk_num, disk_speed)
        self.__disk_add_rotation(disk_num, disk_rotation)

    def __disk_add_tier_withtype(self, disk_num, disk_type):
        lich_tier = 0 if disk_type == 'SSD' else 1
        print "add disk %s tier %s" %(disk_num, lich_tier)
        self.__disk_tier_update({disk_num:lich_tier})

    def __disk_add_link(self, dev, disk_num, pool):
        lich_disk = str(disk_num) + ".disk"
        '''ln -s /dev/sda /opt/fusionstack/data/disk/0.disk'''
        _exec_system("ln -s " + dev + " " + os.path.join(self.disk_home, lich_disk))

    def __disk_add_tier(self, pool, dev, disk_num):
        if self.tier_withtype:
            dev_type = self.disk.get_dev_type(dev)
            if dev_type == 'RAID':
                if not self.raid:
                    self.raid = RAID(self.config)
                dev_info = self.raid.raid_info(dev)
                disk_info = self.raid.disk_info(dev_info['disk'][0])
                disk_type = disk_info['media_type']
            else:
                disk_type = dev_type
            return self.__disk_add_tier_withtype(disk_num, disk_type)
        else:
            ''' get disk speed must before add link, because get speed need write test '''
            print "get %s speed start.." % dev
            disk_speed = self.disk.get_dev_speed(dev)
            print "get %s speed %d" %(dev, disk_speed)
            disk_rotation = self.__get_disk_rotation(dev)
            return self.__disk_add_tier_withspeed(pool, disk_num, disk_speed, disk_rotation)

    def __disk_add_block(self, dev, disk_num, pool, cache):
        clusteruuid = self.config.getclusteruuid()
        hostname = self.config.hostname

        data = 'cluster=%s;node=%s;type=new;disk=%s;pool=%s;cache=%s;cached=0;' % (clusteruuid, hostname, disk_num, pool, cache)

        fd = open(dev, 'wb')
        fd.seek(VALID_DATA_OFFSET, 0)
        fd.write(data)
        fd.close()

    def __disk_add_disk(self, devs, force, cache, pool):
        for dev in devs:
            if self.disk.is_mounted(dev) is not None and force:
                self.disk.dev_umount(dev)
            elif self.disk.is_mounted(dev):
                raise Exp(errno.EINVAL, '%s mounted, please use --force' % dev)

            new = True
            match = self.__disk_load_valid(dev)
            if match:
                if match[0].startswith('disk'):
                    raise Exp(errno.EINVAL, "disk %s used by lich %s, please restart lichd, "
                            "or cleanup disk use `dd if=/dev/zero of=%s bs=1M count=1'" % (dev, match[0], dev))
                    '''
                    if self.__get_lich_writeback_cache(dev) == 0:
                        new = False
                        disk_num = int(match[0][4:])
                    '''
                else:
                    raise Exp(errno.EINVAL, 'disk %s used by lich %s, please restart lichd' % (dev, match[0]))

            if new:
                disk_num = self.__get_disk_num()

            if cache != 100:
                try:
                    self.__disk_add_tier(pool, self.disk.get_dev(dev), disk_num)
                except Exception, e:
                    raise
                    if not force:
                        derror("%s add tier failed:%s" % (dev, e))
                        continue

            if new:
                self.__disk_add_block(dev, disk_num, pool, cache)

            try:
                self.__disk_add_link(dev, disk_num, pool)
            except Exception, e:
                derror("%s add link failed:%s" % (dev, e))
                raise

    def __disk_add_wait(self):
        disk_path = os.path.join(self.config.home, 'data/disk/disk')
        bitmap_path = os.path.join(self.config.home, 'data/disk/bitmap')
        info_path = os.path.join(self.config.home, 'data/disk/info')

        for disk in os.listdir(disk_path):
            disk_num = disk.split('.')[0]
            if not os.path.exists(os.path.join(bitmap_path, str(disk_num) + '.bitmap')):
                return True
            if not os.path.exists(os.path.join(info_path, str(disk_num) + '.info')):
                return True

        return False

    def __disk_add_reset(self):
        while self.__disk_add_wait():
            time.sleep(1)

        instance = self.node.instences[0]
        instance.stop()
        _exec_system("rm -rf %s/hsm" % self.config.shm)
        instance.start()

    def disk_add(self, devs, v, force, cache, pool):
        if len(devs) == 1 and devs[0] == 'all':
            all_disk = self.__disk_list_getall()
            devs = self.__disk_list_getusable(all_disk, force)
            if len(devs):
                dmsg("add disk %s" % devs)
            else:
                dmsg("no usable disk found!")

        if len(devs):
            if pool is None:
                pool = 'default'

            self.__disk_add_check(devs, force, cache, pool)

            fd = _lock_file1("/var/run/add_disk.lock")
            old_tier = self.__get_pool_tier(pool)
            self.__disk_add_disk(devs, force, cache, pool)
            new_tier = self.__get_pool_tier(pool)
            _unlock_file1(fd)

            for tier in old_tier:
                if new_tier[tier] != old_tier[tier]:
                    self.__disk_add_reset()
                    break

    def __disk_del_check(self, devs):
        lich_disk = self.__get_lich_disk()
        del_disk = []
        for dev in devs:
            if dev not in [disk[1] for disk in lich_disk]:
                raise Exp(errno.EINVAL, '%s not used by lich' % dev)
            elif dev not in [disk[1] for disk in lich_disk if disk[0].startswith('disk')]:
                raise Exp(errno.EINVAL, 'only support delete data disk')
            if dev in del_disk:
                raise Exp(errno.EINVAL, '%s repeat' % dev)
            else:
                del_disk.append(dev)

    def __disk_del_superblock(self, dev):
        buff = '\0' * FILE_BLOCK_LEN
        fd = open(dev, 'wb')
        fd.write(buff)
        fd.close()

    def __disk_del_wait(self, dev, disk_num):
        info_path = os.path.join(self.config.home, 'data/disk/info')
        used = 0
        while(used < 10*60*60):
            if not os.path.exists(os.path.join(info_path, disk_num + '.info')):
                return
            sys.stdout.write(str(10*60*60 - int(used)) + "\r")
            sys.stdout.flush()
            used += 1
            time.sleep(1)
        raise Exp(errno.EPERM, "delete disk %s failed" % dev)

    def __disk_del_info(self, disk_num, p = True):
        tier_path = os.path.join(self.tier_path, disk_num + '.tier')
        if os.path.exists(tier_path):
            ret = _exec_system("rm -rf " + tier_path, p)
            if ret:
                derror("delete %s tier failed." % dev)

        speed_path = os.path.join(self.speed_path, disk_num + '.speed')
        if os.path.exists(speed_path):
            ret = _exec_system("rm -rf " + speed_path, p)
            if ret:
                derror("delete %s speed failed." % dev)

        rotation_path = os.path.join(self.rotation_path, disk_num + '.rotation')
        if os.path.exists(rotation_path):
            ret = _exec_system("rm -rf " + rotation_path, p)
            if ret:
                derror("delete %s rotation failed." % dev)

    def __disk_del_disk(self, devs):
        lich_disk = self.__get_lich_disk()
        for dev in devs:
            disk_num = None
            for lich_dev in lich_disk:
                if dev == lich_dev[1]:
                    m = re.match('(\D+)(\d+)', lich_dev[0])
                    if m is not None:
                        disk_num = m.group(2)

            #cmd = "%s --castoff %s/%s"%(self.config.admin, self.node.instences[0].name, disk_num)
            #ret = _exec_system(cmd)
            #if ret:
            #    derror("%s delete failed." % dev)
            #    continue
            self.node.node_drop(int(disk_num))

            self.__disk_del_info(disk_num, False)

            #lich_tier = self.__disk_tier_adjust()
            #self.__disk_tier_update(lich_tier)

            self.__disk_del_superblock(dev)

    def __disk_del_dev_disk(self, devs):
        lich_disk = self.__get_lich_disk()
        for dev in devs:
            disk_num = None
            for lich_dev in lich_disk:
                if dev == lich_dev[1]:
                    m = re.match('(\D+)(\d+)', lich_dev[0])
                    if m is not None:
                        disk_num = m.group(2)

        dev_disk_path = os.path.join(self.disk_home, disk_num + '.disk')
        if os.path.exists(dev_disk_path):
            ret = _exec_system("rm -rf " + dev_disk_path, False)
            if ret:
                derror("delete %s tier failed." % dev)

    def disk_del(self, devs, v):
        self.__disk_del_dev_disk(devs)
        #self.__disk_del_check(devs)
        #self.__disk_del_disk(devs)

    def __disk_check_cache(self, devs, force=False, setcache=True):
        if self.cacheconf is None:
            self.cacheconf = self.parse_cacheconf(self.config.cacheconf)
        if len(self.cacheconf) == 0:
            return {}

        cache_stat = {}
        for dev_type in devs:
            for dev in devs[dev_type]:
                if dev in self.cacheconf:
                    disk = dev
                else:
                    disk = dev_type.lower()

                if disk == 'unknow':
                    if force:
                        continue
                    else:
                        raise Exp(errno.EINVAL, 'not support %s type: %s' %(dev, disk))

                if disk == 'nvme':
                    continue
                if 'skip'in self.cacheconf[disk] and self.cacheconf[disk]['skip']:
                    continue
                if 'disk_cache' in self.cacheconf[disk]:
                    disk_cache = self.cacheconf[disk]['disk_cache']
                else:
                    disk_cache = 'disable'

                dev_cache = self.disk.get_dev_cache(dev)
                if dev_cache is not None and dev_cache.lower() != disk_cache:
                    if setcache:
                        dmsg("set %s cache to %s" % (dev, disk_cache))
                        self.disk.set_dev_cache(dev, disk_cache, force)
                    else:
                        cache_stat[dev] = {'disk_cache': disk_cache}

        return cache_stat

    def __disk_check_split(self):
        raid_devs  = []
        hdd_devs = {'HDD':[], 'SSD':[]}
        if (self.config.testing):
            return raid_devs, hdd_devs

        lich_dev = self.__get_lich_disk()
        for (x, dev, pool) in lich_dev:
            if not dev:
                continue
            dev = self.disk.get_dev(dev)
            if dev is None:
                continue
            if self.disk.is_hba(dev):
                continue
            dev_type = self.disk.get_dev_type(dev)
            if dev_type == 'RAID' and dev not in raid_devs:
                raid_devs.append(dev)
            elif dev_type == 'HDD' and dev not in hdd_devs['HDD']:
                hdd_devs['HDD'].append(dev)
            elif dev_type == 'SSD' and dev not in hdd_devs['SSD']:
                hdd_devs['SSD'].append(dev)
            else:
                pass

        sys_dev = self.disk.get_sys_dev()
        for dev in sys_dev:
            if self.disk.is_hba(dev):
                continue
            dev_type = self.disk.get_dev_type(dev)
            if dev_type == 'RAID' and dev not in raid_devs:
                raid_devs.append(dev)
            elif dev_type == 'HDD' and dev not in hdd_devs['HDD']:
                hdd_devs['HDD'].append(dev)
            elif dev_type == 'SSD' and dev not in hdd_devs['SSD']:
                hdd_devs['SSD'].append(dev)

        return raid_devs, hdd_devs

    def __disk_check_health(self):
        if not self.raid:
            self.raid = RAID(self.config)

        raid_info = {}
        if self.raid.raid_type == 'MegaRAID':
            mega_raid = MegaRAID(self.config)
            raid_info =  mega_raid.get_all_ldpdinfo()

        disk_info = self.disk_list_with_return_json_value()
        all_disks = json.loads(disk_info)

        used_disk = {}
        for disk in all_disks:
            if all_disks[disk]['flag'] == 'lich':
                used_disk[disk] = all_disks[disk]

        for disk in used_disk:
            if 'raid_info' in used_disk[disk].keys():
                if used_disk[disk]['raid_info']['adp_type'] == 'LSI':
                    (adpid, dev_vd) = mega_raid.get_dev_vd(disk)
                    cmd = 'smartctl -d megaraid,%s %s -A' % (raid_info[adpid][dev_vd], disk)
                    cmd = cmd.split()
                    try:
                        (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
                    except Exp, e:
                        _syswarn(" smartctl, %s" % err_msg)
                        continue

                else:
                    _syswarn(" 3007  Don't support raid type: %s(%s)" % (used_disk[disk]['raid_info']['adp_type'], disk))
                    continue

            else:
                cmd = 'smartctl -i %s -A' % disk
                cmd = cmd.split()
                try:
                    (out_msg, err_msg) = _exec_pipe1(cmd, 0, False)
                except Exp, e:
                    _syswarn(" smartctl, %s" % err_msg)
                    continue

            num = out_msg.count('\n')
            lines = out_msg.splitlines(num)
            for line in lines:
                if 'Reallocated_Sector_Ct' in line:
                    if int(line.split()[-1]) > 0:
                        _syserror(' 3006  %s:%s' % (disk.split('/')[-1], line))

    def disk_check(self, arg):
        if self.cacheconf is None:
            self.cacheconf = self.parse_cacheconf(self.config.cacheconf)
        if arg == 'cache':
            if not self.raid:
                self.raid = RAID(self.config)
            (raid_devs, hdd_devs)  = self.__disk_check_split()

            if len(raid_devs) != 0:
                self.raid.raid_check(raid_devs, self.cacheconf, True)
            if len(hdd_devs) != 0:
                self.__disk_check_cache(hdd_devs, True)
        elif arg == 'cacheset':
            if not self.raid:
                self.raid = RAID(self.config)
            (raid_devs, hdd_devs)  = self.__disk_check_split()

            if len(raid_devs) != 0:
                self.raid.raid_check(raid_devs, self.cacheconf, True)
            if len(hdd_devs) != 0:
                self.__disk_check_cache(hdd_devs)
        elif arg == 'cachestat':
            cachestat = {}
            if not self.raid:
                self.raid = RAID(self.config)
            (raid_devs, hdd_devs)  = self.__disk_check_split()

            if len(raid_devs) != 0:
                stat = self.raid.raid_check(raid_devs, self.cacheconf, True, False)
                cachestat.update(stat)
            if len(hdd_devs) != 0:
                stat = self.__disk_check_cache(hdd_devs, True, False)
                cachestat.update(stat)

            return cachestat
        elif arg == 'tier':
            pool = self.pool_manage.pool_list()
            for p in pool:
                lich_tier = self.__get_pool_tier(p)
                if self.tier_withtype:
                    print p, ":", lich_tier
                else:
                    tier_adjust = self.__disk_tier_adjust_new(p)
                    if lich_tier == tier_adjust:
                        print p, ":", lich_tier
                    else:
                        print p, "tier:", lich_tier, "should:", tier_adjust
        elif arg == 'writeback':
            print self.__get_lich_writeback()
        elif arg == 'speed':
            pool = self.pool_manage.pool_list()
            for p in pool:
                print p, ":", self.__get_pool_speed(p)
        elif arg == 'rotation':
            pool = self.pool_manage.pool_list()
            for p in pool:
                print p, ":", self.__get_pool_rotation(p)
        elif arg == 'health':
            self.__disk_check_health()
        else:
            raise Exp(errno.EINVAL, '%s is invalid argument, use --help for help' % arg)

    def raid_add(self, devs, force):
        if not self.raid:
            self.raid = RAID(self.config)

        if (len(devs) == 1 and devs[0] == 'all'):
            devs = []
            new_raid_disk = self.raid.disk_list()
            if new_raid_disk:
                for adp, disks in new_raid_disk.iteritems():
                    for disk in disks:
                        devs.append(disks[disk]['inq'])

            if new_raid_disk:
                if len(new_raid_disk):
                    dmsg("add raid %s" % devs)
                else:
                    dmsg("no valid disk found!")
            else:
                dmsg("no valid disk found!")

        new_disk = []
        all_disk = self.disk.get_all_devs()
        for dev in devs:
            self.raid.raid_add(dev, force)
            now_disk = self.disk.get_all_devs()
            for disk in now_disk:
                if disk not in all_disk:
                    new_disk.append(disk)
            all_disk = now_disk

        return new_disk

    def __raid_del_check(self, devs, force):
        lich_disk = self.__get_lich_disk()
        sys_dev = self.disk.get_sys_dev()
        for dev in devs:
            if not self.disk.is_dev(dev):
                raise Exp(errno.EINVAL, "%s is not block device" % dev)
            if self.disk.get_dev_type(dev) != 'RAID':
                raise Exp(errno.EPERM, 'can not del disk %s, maybe not raid disk' % dev)
            if dev in [disk[1] for disk in lich_disk]:
                raise Exp(errno.EINVAL, '%s used by lich' % dev)
            if dev in sys_dev:
                raise Exp(errno.EINVAL, "can not delete system device")
            if self.disk.is_mounted(dev) is not None and not force:
                raise Exp(errno.EINVAL, '%s mounted, please use --force' % dev)

    def __raid_del_disk(self, devs, force):
        if not self.raid:
            self.raid = RAID(self.config)
        for dev in devs:
            if self.disk.is_mounted(dev) is not None and force:
                self.disk.dev_umount(dev, True)
            elif self.disk.is_mounted(dev) is not None:
                raise Exp(errno.EINVAL, '%s mounted, please use --force' % dev)
            self.raid.raid_del(dev, force)

    def raid_del(self, devs, force):
        self.__raid_del_check(devs, force)
        self.__raid_del_disk(devs, force)

    def raid_load(self):
        if not self.raid:
            self.raid = RAID(self.config)
        self.raid.raid_import()

    def raid_flush(self):
        if not self.raid:
            self.raid = RAID(self.config)
        self.raid.raid_flush()

    def __raid_cache_check(self, devs):
        for dev in devs:
            if self.disk.get_dev_type(dev) != 'RAID':
                raise Exp(errno.EPERM, 'can not set disk %s raid cache' % dev)

    def raid_cache(self, switch, devs, policy):
        if not self.raid:
            self.raid = RAID(self.config)
        if len(devs) == 0:
            (raid_devs, hdd_devs)  = self.__disk_check_split()
            sys_dev = self.disk.get_sys_dev()
            for dev in sys_dev:
                dev_type = self.disk.get_dev_type(dev)
                if dev_type == 'RAID' and dev not in raid_devs:
                    raid_devs.append(dev)
            devs = raid_devs

        if not policy:
            self.__raid_cache_check(devs)
        self.raid.raid_cache(switch, devs, self.cacheconf, policy)

    def raid_miss(self):
        if not self.raid:
            self.raid = RAID(self.config)
        self.raid.raid_miss()

    def __disk_light_check(self, devs):
        for dev in devs:
            if dev.startswith('/dev/') and self.disk.get_dev_type(dev) != 'RAID':
                raise Exp(errno.EPERM, 'can not set disk %s light flash' % dev)

    def disk_light(self, switch, devs):
        if not self.raid:
            self.raid = RAID(self.config)
        self.__disk_light_check(devs)
        self.raid.raid_light(switch, devs)
