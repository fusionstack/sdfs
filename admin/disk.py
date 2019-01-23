#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import os
import stat
import errno
import re
import time
import random

from buddha.smart import SMART

from lvm import LVM
from utils import Exp, dmsg, dwarn, derror, _exec_pipe, \
    _human_unreadable, _human_readable, _exec_pipe1, lock_file

FILE_BLOCK_LEN = 4096

class Disk:
    def __init__(self):
        self.smart = SMART()
        self.lvm = LVM()
        self.ls_pci = None
        self.disktool = self.check_tool()
        self.dev_check = {}

    def check_tool(self):
        return 'syscall'
        try:
            (out_msg, err_msg) = _exec_pipe1(["sgdisk"], 0, False)
        except Exp, e:
            if e.err.startswith('Usage:'):
                return 'sgdisk'

        return 'parted'

    def is_block(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        try:
            if not stat.S_ISBLK(os.lstat(os.path.realpath(dev)).st_mode):
                return False
        except Exception, e:
            return False

        return True

    def is_dev(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev

        dev = dev.split('/')[-1]
        all_parts = self.get_all_parts()
        for disk, parts in all_parts.iteritems():
            if dev == disk or dev in parts:
                return True

        return False

    def is_part(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part

        part = part.split('/')[-1]
        all_parts = self.get_all_parts()

        for dev, parts in all_parts.iteritems():
            if part in parts:
                return True

        return False

    def is_swap(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not stat.S_ISBLK(os.lstat(dev).st_mode):
            raise Exp(errno.EINVAL, '%s not a block device' % dev)

        with file('/proc/swaps', 'rb') as proc_swaps:
            for line in proc_swaps.readlines()[1:]:
                fields = line.split()
                if len(fields) < 3:
                    continue
                swaps_dev = fields[0]
                if swaps_dev.startswith('/') and os.path.exists(swaps_dev):
                    swaps_dev = os.path.realpath(swaps_dev)
                    if swaps_dev == dev:
                        return True
        return False

    def is_mounted(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, '%s not a block device' % dev)

        with file('/proc/mounts', 'rb') as proc_mounts:
            for line in proc_mounts:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mounts_dev = fields[0]
                path = fields[1]
                if mounts_dev.startswith('/') and os.path.exists(mounts_dev):
                    if mounts_dev == dev:
                        return path
                    mounts_dev = os.path.realpath(mounts_dev)
                    if mounts_dev == dev:
                        return path
        return None

    def dev_check_mounted(self, dev):
        if self.is_mounted(dev):
            return True

        parts = self.get_dev_parts(dev)
        for part in parts:
            if self.is_mounted(part):
                return True

        return False

    def is_dm(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not stat.S_ISBLK(os.lstat(dev).st_mode):
            raise Exp(errno.EINVAL, '%s not a block device' % dev)

        res = _exec_pipe(["dmsetup", 'table'], 0, False)
        for line in res.splitlines():
            dms = re.findall(r'\((/dev/\w+)\)', line, re.M)
            for dm in dms:
                (dm_dev, dm_part) = re.match('(\D+)(\d*)', dm).group(1, 2)
                if dev == dm_dev:
                    return True

        return False

    def get_dev_byserialno(self, serialno):
        all_devs = self.get_all_devs()
        for dev in all_devs:
            dev_serialno = self.get_dev_serialno(dev)
            if dev_serialno == serialno:
                return dev

        return None

    def get_dev_bymounted(self, path):
        dev = None
        res = _exec_pipe(["mount"], 0, False)
        for line in res.splitlines():
            mnt = re.search('^(/dev/.+) on %s type' % (path), line)
            if mnt is not None:
                dev = mnt.group(1)

        return dev

    def get_sys_dev_mount(self, withpart):
        sys_devs = []
        all_parts = self.get_all_parts();
        for dev, parts in all_parts.iteritems():
            if self.is_mounted(dev) == '/' or self.is_mounted(dev) == '/boot':
                sys_dev = '/dev/' + dev
                if sys_dev not in sys_devs:
                    sys_devs.append(sys_dev)
            for part in parts:
                if self.is_mounted(part) == '/' or self.is_mounted(part) == '/boot':
                    if withpart:
                        sys_dev = '/dev/' + part
                        if sys_dev not in sys_devs:
                            sys_devs.append(sys_dev)
                    else:
                        sys_dev = '/dev/' + dev
                        if sys_dev not in sys_devs:
                            sys_devs.append(sys_dev)
        return sys_devs

    def get_sys_dev_lvm(self, withpart = False):
        sys_devs = []
        all_parts = self.get_all_parts();
        with file('/proc/mounts', 'rb') as proc_mounts:
            for line in proc_mounts:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mounts_dev = fields[0]
                path = fields[1]
                if mounts_dev.startswith('/') and os.path.exists(mounts_dev) and (path == '/' or path == '/boot'):
                    mounts_dev = os.path.realpath(mounts_dev)
                    if mounts_dev.startswith('dm') or mounts_dev.startswith('/dev/dm'):
                        lvms = self.lvm.get_dev_bylvm(mounts_dev)
                        if withpart:
                            sys_devs.extend(lvms)
                            break
                        if lvms is None:
                            continue
                        for i in lvms:
                            for dev, parts in all_parts.iteritems():
                                sys_dev = '/dev/' + dev
                                if i == sys_dev and sys_dev not in sys_devs:
                                    sys_devs.append(sys_dev)
                                    continue
                                for part in parts:
                                    if i == '/dev/' + part and sys_dev not in sys_devs:
                                        sys_devs.append(sys_dev)

        return sys_devs

    def get_all_dm(self):
        all_dm = {}
        with file('/proc/mdstat', 'rb') as proc_mdstat:
            for line in proc_mdstat:
                if not line.startswith('md'):
                    continue
                fields = line.split()
                dm = '/dev/' + fields[0]
                if dm not in all_dm:
                    all_dm[dm] = {}
                for item in fields[3:]:
                    if '[' in item and ']' in item:
                        dev = item.split('[')[0]
                        idx = item.split('[')[1].split(']')[0]
                        all_dm[dm][idx] = '/dev/' + dev

        return all_dm

    def get_sys_dev_dm(self, devs):
        sys_devs = []
        all_dm = self.get_all_dm()
        for dev in devs:
            found = False
            for dm in all_dm:
                if dev.startswith(dm):
                    found = True
                    for idx in all_dm[dm]:
                        if all_dm[dm][idx] not in sys_devs:
                            sys_devs.append(all_dm[dm][idx])
            if not found:
                sys_devs.append(dev)

        return sys_devs

    def get_sys_dev(self, withpart = False):
        sys_devs = []
        sys_dev_mount = self.get_sys_dev_mount(withpart)
        try:
            sys_dev_lvm = self.get_sys_dev_lvm(withpart)
        except:
            sys_dev_lvm = {}

        [sys_devs.append(x) for x in sys_dev_mount if x not in sys_devs]
        [sys_devs.append(x) for x in sys_dev_lvm if x not in sys_devs]

        sys_devs = self.get_sys_dev_dm(sys_devs)

        return sys_devs

    def get_all_devs(self):
        all_parts = self.get_all_parts()

        if str(all_parts) != 0:
            return all_parts.keys()

        return []

    def get_dev_parts(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        parts = []
        all_parts = self.get_all_parts()
        if dev.split('/')[-1] in all_parts:
            parts = all_parts[dev.split('/')[-1]]

        if len(parts) == 0:
            return parts

        return parts

    def __get_all_parts(self, arg):
        dev_part_list = {}
        if not os.path.exists('/dev/disk/by-' + arg):
            return dev_part_list

        for name in os.listdir('/dev/disk/by-' + arg):
            target = os.readlink(os.path.join('/dev/disk/by-' + arg, name))
            dev = target.split('/')[-1]
            if dev.startswith('dm'):
                continue
            (baser) = re.search('(.*)-part\d+$', name)
            if baser is not None:
                basename = baser.group(1)
                base = os.readlink(os.path.join('/dev/disk/by-' + arg, basename)).split('/')[-1]
                if base not in dev_part_list:
                    dev_part_list[base] = []
                if dev not in dev_part_list[base]:
                    dev_part_list[base].append(dev)
            else:
                if dev not in dev_part_list:
                    dev_part_list[dev] = []

        return dev_part_list

    def get_all_cdrom(self):
        cdrom = []

        if os.path.exists('/proc/sys/dev/cdrom/info'):
            with file('/proc/sys/dev/cdrom/info', 'rb') as cdrom_info:
                for line in cdrom_info.read().split('\n')[2:]:
                    m = re.match('drive name:\s+(\w+)', line)
                    if m is not None :
                        cdrom.append(m.group(1))

        return cdrom

    def get_all_parts(self):
        devs = []
        parts = []
        all_parts = {}

        cdrom = self.get_all_cdrom()

        with file('/proc/partitions', 'rb') as proc_partitions:
            for line in proc_partitions.read().split('\n')[2:]:
                fields = line.split()
                if len(fields) < 4:
                    continue
                name = fields[3].split('/')[-1]
                if name.startswith('dm') or name in cdrom or name.startswith('loop'):
                    continue
                m = re.match('(\D+)(\d+)', name)
                if m is None:
                    devs.append(name)
                else:
                    parts.append(name)

        for dev in devs:
            all_parts[dev] = []

        for part in parts:
            (d, p) = re.match('(\D+)(\d+)', part).group(1, 2)
            if d not in all_parts:
                all_parts[part] = []
            else:
                all_parts[d].append(part)

        return all_parts

    def get_dev_type(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if self.is_part(dev):
            dev = re.match('(\D+)(\d*)', dev).group(1)
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev = os.path.realpath(dev)
        dev_name = dev.split('/')[-1]
        dev_type = 'UNKNOW'

        rotational = "/sys/block/"+dev_name+"/queue/rotational"
        if os.path.exists(rotational):
            type_num = _exec_pipe(["cat", rotational], 0, False)
            type_num = type_num.strip()
            if '0' == type_num:
                dev_type = 'SSD'
            elif '1' == type_num:
                dev_type = 'HDD'

        realpath = os.path.realpath('/sys/block/' + dev_name)
        m = re.match('(/sys/devices/platform/host\d+)', realpath)
        if m is not None:
            path = os.path.join(m.group(1), 'iscsi_host')
            if os.path.exists(path):
                dev_type = 'ISCSI'
            else:
                dev_type = 'UNKNOW'
            return dev_type

        pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
        if len(pci_path) == 0:
            realpath = os.path.realpath(os.path.join(realpath, "device"))
            pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
            if len(pci_path) == 0:
                return 'UNKNOW'
        if self.ls_pci is None:
            self.ls_pci = _exec_pipe(["lspci"], 0, False)
        for line in self.ls_pci.splitlines():
            if line.startswith(pci_path[-1]):
                pci_type = line.split()[1]
                if pci_type == 'USB':
                    dev_type = 'USB'
                elif pci_type == 'RAID':
                    dev_type = 'RAID'
                elif pci_type == 'Non-Volatile':
                    dev_type = 'NVMe'

        return dev_type

    def is_hba(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if self.is_part(dev):
            dev = re.match('(\D+)(\d*)', dev).group(1)
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev = os.path.realpath(dev)
        dev_name = dev.split('/')[-1]
        realpath = os.path.realpath('/sys/block/' + dev_name)

        pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
        if len(pci_path) == 0:
            realpath = os.path.realpath(os.path.join(realpath, "device"))
            pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
            if len(pci_path) == 0:
                return False
        if self.ls_pci is None:
            self.ls_pci = _exec_pipe(["lspci"], 0, False)
        for line in self.ls_pci.splitlines():
            if line.startswith(pci_path[-1]):
                if 'Serial Attached SCSI controller: Hewlett-Packard Company Smart Array' in line:
                    return True

        return False

    def get_dev_raidcard(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if self.is_part(dev):
            dev = re.match('(\D+)(\d*)', dev).group(1)
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev = os.path.realpath(dev)
        dev_name = dev.split('/')[-1]

        realpath = os.path.realpath('/sys/block/' + dev_name)

        pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
        if len(pci_path) == 0:
            realpath = os.path.realpath(os.path.join(realpath, "device"))
            pci_path = re.findall(r'[0-9a-fA-F]{4}:([0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-9a-fA-F])', realpath, re.M)
            if len(pci_path) == 0:
                return 'UNKNOW'
        if self.ls_pci is None:
            self.ls_pci = _exec_pipe(["lspci"], 0, False)
        for line in self.ls_pci.splitlines():
            if line.startswith(pci_path[-1]):
                pci_type = line.split()[1]
                if pci_type == 'RAID':
                    raidcard = re.search('RAID bus controller:([^\[(]*)', line).group(1).strip()
                    return raidcard

        raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

    def get_dev_size_sgdisk(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev_info = self.dev_check_sgdisk(dev)
        dev_size = re.findall(r'^Disk %s: \d+ sectors, (.*)' % (dev), dev_info, re.M)

        return dev_size[0].replace(' ', '').replace('i', '')

    def get_dev_size_parted(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev_info = self.dev_check_parted(dev)
        for line in dev_info.splitlines():
            m = re.search('Disk /dev', line)
            if m is not None:
                dev_size = line.strip().split(':')[1]
                return dev_size
        return None

    def get_dev_size_syscall(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        fd = open(dev)
        fd.seek(0, 2)
        dev_size = fd.tell()
        fd.close()

        return _human_readable(dev_size)

    def get_dev_size(self, dev):
        if self.disktool == 'sgdisk':
            return self.get_dev_size_sgdisk(dev)
        elif self.disktool == 'parted':
            return self.get_dev_size_parted(dev)
        elif self.disktool == 'syscall':
            return self.get_dev_size_syscall(dev)

    def get_dev_free_sgdisk(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev_info = self.dev_check_sgdisk(dev)
        dev_free = re.findall(r'^Total free space is \d+ sectors \((.*)\)', dev_info, re.M)

        return dev_free[0].replace(' ', '').replace('i', '')

    def get_dev_free_parted(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev

        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        if dev in self.dev_check:
            return self.dev_check[dev]

        try:
            (out_msg, err_msg) = _exec_pipe1(["parted", dev, "print", "-s"], 0, False, 5)
        except Exp, e:
            raise

        self.dev_check[dev] = out_msg

        lst = re.findall(r'^.+ Free Space\n', out_msg, re.M)

        free = 0
        for i in lst:
            lst1 = re.findall(r'[\d.]+[kMG]B', i, 0)
            free += _human_unreadable(lst1[2])
        return _human_readable(free)


    def get_dev_free(self, dev):
        if self.disktool == 'sgdisk':
            return self.get_dev_free_sgdisk(dev)
        elif self.disktool == 'parted':
            return self.get_dev_free_parted(dev)

    def get_dev_speed(self, dev, verbose=False):
        """ 针对移动集采，暂时不做磁盘测速 """
        return 0

        fd = os.open(dev, os.O_RDONLY)
        disk_size = os.lseek(fd, 0, os.SEEK_END)

        count = 0
        start = time.time()

        used = 0
        times = 100
        while used < 1:
            for i in range(times):
                rand = random.randint(0, disk_size - FILE_BLOCK_LEN)
                os.lseek(fd, rand, os.SEEK_SET)
                os.read(fd, FILE_BLOCK_LEN)
                count += 1

            times *= 2
            used = time.time() - start

        os.close(fd)
        speed = (count * FILE_BLOCK_LEN)/used/1024/1024
        if verbose:
            print "dev:", dev, " count:", count, " used:", used, " speed:", speed, "M/s iops:", count/used

        return int(count/used)

    def get_dev_rotation(self, dev, verbose=False):
        return self.smart.get_dev_rotation(dev)

    def get_dev_cache(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        cache = None
        (out, err) = _exec_pipe1(["hdparm", "-W", dev], 0, False)
        for line in out.splitlines():
            m = re.search('write-caching\s+=\s+(\d)\s+\(\S+\)', line)
            if m is not None:
                cache = m.group(1)

        if cache is None:
            return None
        elif cache == '1':
            return 'Enabled'
        elif cache == '0':
            return 'Disabled'
        else:
            return cache

    def set_dev_cache(self, dev, cache, force=False):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        switch = '1' if cache == 'enable' else '0'
        try:
            _exec_pipe1(["hdparm", "-W", switch, dev], 0, False)
        except Exception, e:
            if not force:
                raise Exp(errno.EPERM, "set disk %s cache faile:%s" %(dev, e.message))
            else:
                _dwarn("set disk %s cache faile, %s" %(dev, e.message))

    def get_dev_interface(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        interface = None
        try:
            res, err = _exec_pipe1(["hdparm", "-I", dev], 0, False)
            for line in res.splitlines():
                m = re.search('ATA device', line)
                if m is not None:
                    interface = 'ATA'
                m = re.search('\s*Transport:\s*Serial', line)
                if m is not None:
                    interface = 'SATA'
        except Exception, e:
            pass

        return interface

    def get_dev_serialno(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        serialno = []
        try:
            res, err = _exec_pipe1(["hdparm", "-I", dev], 0, False)
            for line in res.splitlines():
                m = re.search('Model Number:\s*(\S+)', line)
                if m is not None:
                    serialno.append(m.group(1))
                m = re.search('Serial Number:\s*(\S+)', line)
                if m is not None:
                    serialno.append(m.group(1))
                m = re.search('Firmware Revision:\s*(\S+)', line)
                if m is not None:
                    #serialno.append(m.group(1))
                    pass
        except Exception, e:
            pass

        return '_'.join(serialno)

    def get_dev_health(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_dev(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        dev_health = self.smart.get_dev_health(dev)

        return dev_health

    def get_dev(self, dev):
        if self.is_part(dev):
            (dev, partnum) = re.match('(\D+)(\d+)', dev).group(1, 2)
            return dev
        if self.is_dev(dev):
            return dev

    def get_dev_label(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev

        try:
            (out_msg, err_msg) = _exec_pipe1(["e2label",  dev], 0, False)
            return out_msg.strip()
        except Exception:
            return None

    def set_dev_label(self, dev, label, verbose=False):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        (out_msg, err_msg) = _exec_pipe1(["e2label", dev, label], 0, verbose)
        if (verbose):
            print(err_msg.strip())

    def dev_format(self, dev, verbose):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev

        fd = _lock_file("/var/run/fusionstack_disk_%s.lock" % dev.split('/')[-1], -1, False)
        res = _exec_pipe(["mkfs.ext4", dev, '-F'], 0, True)
        if verbose:
            print(res.strip())

    def dev_umount(self, dev, verbose=False):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp('not a block device', dev)

        _exec_pipe(["umount", "-lf", dev], 0, verbose)

    def dev_try_umount(self, dev, verbose=False):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not self.is_block(dev):
            raise Exp('not a block device', dev)

        _exec_pipe(["umount", dev], 0, verbose)

    def dev_mount(self, dev, path, verbose=False):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if not stat.S_ISBLK(os.lstat(os.path.realpath(dev)).st_mode):
            raise Exp('not a block device', dev)

        _exec_pipe(["mount", dev, path], 0, verbose)

    def dev_check_sgdisk(self, dev, ignore_err=True):
        if dev in self.dev_check:
            return self.dev_check[dev]

        try:
            (out_msg, err_msg) = _exec_pipe1(["sgdisk", "-p", dev], 0, False, 5)
        except Exp, e:
            out_msg = e.out
            err_msg = e.err

        if err_msg.strip() != '' and not ignore_err:
            raise Exp(errno.EPERM, '%s'%(err_msg.splitlines()[0]))

        self.dev_check[dev] = out_msg

        return out_msg

    def dev_check_parted(self, dev, ignore_err=True):
        if dev in self.dev_check:
            return self.dev_check[dev]

        try:
            (out_msg, err_msg) = _exec_pipe1(["parted", dev, "print", "-s"], 0, False, 5)
        except Exp, e:
            out_msg = e.out
            err_msg = e.err

        if err_msg.strip() != '' and not ignore_err:
            raise Exp(errno.EPERM, '%s'%(err_msg.splitlines()[0]))

        self.dev_check[dev] = out_msg

        return out_msg

    def dev_reset_sgdisk(self, dev):
        try:
            disk_info = self.dev_check_sgdisk(dev)
            if disk_info.splitlines()[0].strip() != 'Creating new GPT entries.':
                try:
                    _exec_pipe1(["sgdisk", "-z", dev], 0, False, 5)
                except Exception, e:
                    pass
        except Exception, e:
            try:
                _exec_pipe1(["sgdisk", "-z", dev], 0, False, 5)
            except Exception, e:
                pass

    def dev_reset_parted(self, dev):
        pass

    def dev_reset_free(self, dev):
        if self.disktool == 'sgdisk':
            return self.dev_reset_sgdisk(dev)
        elif self.disktool == 'parted':
            return self.dev_reset_parted(dev)

    def get_dev_fs(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part
        if not self.is_block(part):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        try:
            res = _exec_pipe(["blkid", "-s", 'TYPE', part], 0, False)
            if 'TYPE' in res:
                part_fs = res.split()[1].split('"')[1]
                return part_fs
            else:
                return None
        except Exp, e:
            return None

    def get_part_uuid(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part

        try:
            res = _exec_pipe(["blkid", "-s", 'UUID', part], 0, False)
            if 'UUID' in res:
                part_uuid = res.split()[1].split('"')[1]
                return part_uuid
            else:
                return None
        except Exp, e:
            return None

    def get_part_type(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part
        if not self.is_part(part):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        try:
            res = _exec_pipe(["file", "-s", part], 0, False)
            if 'ERROR' not in res:
                return res.split()[4]
        except Exception, e:
            return None

        return None

    def get_part_size_sgdisk(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part
        if not self.is_part(part):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        (dev, partnum) = re.match('(\D+)(\d+)', part).group(1, 2)
        dev_info = self.dev_check_sgdisk(dev)
        for line in dev_info.splitlines():
            m = re.search('\s*(\d+)\s+\d+\s+\d+\s+(\S+ \S+B)\s+\S+\s+.*', line)
            if m is not None:
                num = m.group(1)
                if num != partnum:
                    continue
                return m.group(2).replace(' ', '').replace('i', '')
        return None

    def get_part_size_parted(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part
        if not self.is_part(part):
            raise Exp(errno.EINVAL, os.strerror(errno.EINVAL))

        (dev, partnum) = re.match('(\D+)(\d+)', part).group(1, 2)
        dev_info = self.dev_check_parted(dev)
        for line in dev_info.splitlines():
            m = re.search('Disk /dev', line)
            if m is not None:
                dev_size = line.strip().split(':')[1]
                return dev_size
        return None

    def get_part_size(self, dev):
        if self.disktool == 'sgdisk':
            return self.get_part_size_sgdisk(dev)
        elif self.disktool == 'parted':
            return self.get_part_size_parted(dev)
        elif self.disktool == 'syscall':
            return self.get_dev_size_syscall(dev)

    def get_dev_info(self, dev):
        dev_info = {}
        dev_err = False

        try:
            if self.disktool == 'sgdisk':
                self.dev_check_sgdisk(dev if dev.startswith('/dev/') else '/dev/' + dev)
            elif self.disktool == 'parted':
                self.dev_check_parted(dev if dev.startswith('/dev/') else '/dev/' + dev)
        except Exception, e:
            _derror('%s:%s'%(dev, e))
            dev_err = True
        try:
            dev_type = self.get_dev_type(dev)
        except Exception, e:
            dev_type = 'UNKNOW'
        try:
            dev_raidcard = self.get_dev_raidcard(dev)
        except Exception, e:
            dev_raidcard = 'UNKNOW'
        try:
            dev_size = self.get_dev_size(dev)
        except Exception, e:
            dev_size = 'UNKNOW'
        try:
            dev_free = self.get_dev_free(dev)
        except Exception, e:
            dev_free = 'UNKNOW'
        try:
            dev_label = self.get_dev_label(dev)
        except Exception, e:
            dev_label = 'UNKNOW'
        try:
            dev_mount = self.is_mounted(dev)
        except Exception, e:
            dev_mount = 'UNKNOW'
        try:
            dev_cache = self.get_dev_cache(dev)
        except Exception, e:
            dev_cache = 'UNKNOW'
        try:
            dev_interface = self.get_dev_interface(dev)
        except Exception, e:
            dev_interface = 'UNKNOW'
        try:
            dev_fs = self.get_dev_fs(dev)
        except Exception, e:
            dev_fs = 'UNKNOW'
        #dev_health = self.get_dev_health(dev)

        dev_info['fault'] = dev_err
        dev_info['type'] = dev_type
        dev_info['media_type'] = dev_type
        dev_info['size'] = dev_size
        dev_info['free'] = dev_free
        dev_info['label'] = dev_label
        dev_info['mount'] = dev_mount
        dev_info['cache'] = dev_cache
        dev_info['interface'] = dev_interface
        dev_info['fs'] = dev_fs
        dev_info['raidcard'] = dev_raidcard
        #dev_info['health'] = dev_health

        return dev_info

    def get_part_info(self, part):
        part_info = {}

        part_type = self.get_part_type(part)
        part_size = ''
        part_fs = ''
        part_mount = ''
        if (part_type != 'extended'):
            part_size = self.get_part_size(part)
            part_fs = self.get_dev_fs(part)
            part_mount = self.is_mounted(part)

        part_info['type'] = part_type
        part_info['size'] = part_size
        part_info['fs'] = part_fs
        part_info['mount'] = part_mount

        return part_info
