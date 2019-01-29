#!/usr/bin/python

import os
import errno
import time
import sys
from StringIO import StringIO
import getopt
import copy

from utils import dwarn, derror, dmsg, Exp, _exec_shell1

def _get_value(path):
    size = os.path.getsize(path)
    fd = open(path, 'r')
    buf = fd.read(size)
    fd.close()
    return buf

def _set_value(path, buf):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    fd = open(path, 'w')
    fd.write(buf)
    fd.close()

class BcacheManage(object):
    def __init__(self):
        pass

    def create_cachedev(self, cachedev):
        cmd = 'make-bcache -C %s' % cachedev
        _exec_shell1(cmd, p=True)

    def check_and_register(self, data_dev):
        devname = data_dev.split('/')[2]
        check_path = '/sys/block/%s/bcache' % devname

        retry = 0
        while True:
            if os.path.exists(check_path):
                #using bcache disk in docker
                if not self._check_mappingdev_exists(data_dev):
                    raise Exp(errno.ENOENT, "%s mapping disk not exists, please check it !\n" % (coredev))
                else:
                    break

            if retry > 2:
                raise Exp(errno.EPERM, "%s not registered!" % (data_dev))

            self.register_dev(data_dev)
            time.sleep(1)
            retry += 1

    def clean_lich_meta(self, dev):
        # 8K + 1M
        cmd = 'dd if=/dev/zero of=%s bs=1M count=1 oflag=direct' % dev
        _exec_shell1(cmd, p=True)

    def _get_bcache_device_num(self):
        cmd = "cat /proc/devices | grep ' bcache' | awk '{print $1}'"
        out, err = _exec_shell1(cmd, p=False)
        return out.strip()

    def _check_mappingdev_exists(self, coredev):
        if coredev.startswith("/dev/bcache"):
            mappingdev = coredev
        else:
            mappingdev = self.get_mappingdev_by_coredev(coredev)

        if not os.path.exists(mappingdev):
            dev_num = self._get_bcache_device_num()
            cmd = "mknod %s b %s %s" % (mappingdev, dev_num, mappingdev[11:])
            _exec_shell1(cmd, p=False)
            if os.path.exists(mappingdev):
                return True
            else:
                return False
        else:
            return True

    def create_coredev(self, coredev):
        self._stop_coredev_bcache_service(coredev)

        # TODO clean lich metadata or not
        self.clean_lich_meta(coredev)

        #self.wipe_dev(coredev)

        cmd = 'make-bcache -B %s' % coredev
        _exec_shell1(cmd, p=True)

        self.check_and_register(coredev)


    def register_all_cachedev(self):
        cmd = "cat /proc/partitions | awk '{print $4}' | grep -v name| grep -v '^ram' | grep -v '^dm-' | grep -v '^bcache' | grep -v '^td'"
        out, err = _exec_shell1(cmd, p=False)
        for devname in out.split('\n'):
            if len(devname) == 0:
                continue
            disk = "/dev/" + devname
            self.is_cachedev(disk)

        self.all_cache_register = True

    def register_dev(self, coredev):
        register_path = '/sys/fs/bcache/register'

        if not os.path.exists(register_path):
            raise Exp(errno.EPERM, "bcache load error\n")

        _exec_register = 'echo %s > /sys/fs/bcache/register' % coredev
        try:
            _exec_shell1(_exec_register, p=False)
        except Exp as e:
            pass

    def bind_cache(self, cachedev, coredevs):
        """
        :todo if coredev is a member of ANOTHER cset

        :param cachedev:
        :param coredev:
        :return:
        """
        force = True

        self.ensure_cache_dev(cachedev, force)

        for coredev in coredevs.split(','):
            if self.is_coredev(coredev):
                if force:
                    if self.is_attached_to_cache(coredev, cachedev):
                        if not self._check_mappingdev_exists(coredev):
                            raise Exp(errno.ENOENT, "%s mapping disk not exists, please check it !\n" % (coredev))

                        dmsg("%s already attached to cache device : %s, just ignore it !" % (coredev, cachedev))
                        return
                    else:
                        dmsg("dev:%s is a core device, will force destroy it!" % coredev)
                        self.__del_coredev_no_detach(coredev)
                        time.sleep(1)
                else:
                    raise Exp(errno.EPERM, "dev:%s is already a core device, please check it" % (coredev))

            self.create_coredev(coredev)
            self._add_coredev_to_cache(coredev, cachedev)

    def is_coredev(self, dev):
        first_step = False
        if not dev.startswith('/dev/'):
            return False
        devname = dev.split('/')[2]

        cmd = "bcache-super-show %s 2>/dev/null| grep 'backing device'" % dev
        check_path = '/sys/block/%s/bcache' % devname
        try:
            out, err = _exec_shell1(cmd, p=False)
            first_step = True
        except Exp as e:
            return False

        if first_step is True:
            if os.path.exists(check_path):
                return True
            else:
                try:
                    cmd = "echo '%s' > /sys/fs/bcache/register" % (dev)
                    _exec_shell1(cmd, p=False)
                    time.sleep(1)
                    if os.path.exists(check_path):
                        return True
                    else:
                        return False
                except Exp as e:
                    return False
        else:
            return False

    def is_cachedev(self, dev):
        cmd = "bcache-super-show %s 2>/dev/null | grep 'cache device'" % dev
        try:
            out, err = _exec_shell1(cmd, p=False)
            cset_uuid = self.get_cset_uuid_by_dev(dev)
            cache_home = os.path.join("/sys/fs/bcache", cset_uuid)
            if not os.path.isdir(cache_home):
                try:
                    cmd = "echo '%s' > /sys/fs/bcache/register" % (dev)
                    _exec_shell1(cmd, p=False)
                    time.sleep(1)
                    if os.path.isdir(cache_home):
                        return True
                    else:
                        return False
                except Exp as e:
                    return False
            else:
                return True
        except Exp as e:
            return False

    def check_cache_dev(self, cachedev):
        if not self.is_cachedev(cachedev):
            return False

        cset_uuid = self.get_cset_uuid_by_dev(cachedev)
        if cset_uuid and self.is_cset_online(cset_uuid):
            cmd = "echo 0 > /sys/fs/bcache/%s/congested_write_threshold_us && echo 0 > /sys/fs/bcache/%s/congested_read_threshold_us" % (cset_uuid, cset_uuid)
            _exec_shell1(cmd, p=False)
            return True

        return False

    def ensure_cache_dev(self, cachedev, force):
        retry = 0
        while True:
            if self.check_cache_dev(cachedev):
                break

            if force and retry < 3:
                self.create_cachedev(cachedev)
                self.register_dev(cachedev)
                retry += 1
                time.sleep(1)
            else:
                raise Exp(errno.EPERM, "cache %s not ready, retry %d" % (cachedev, retry))

    def _is_deleting(self, coredev):
        cmd = "grep 'delcoredev %s' %s" % (coredev, self.cache_log)
        try:
            _exec_shell1(cmd, p=False)
            return True
        except Exp, e:
            return False

    def get_status_by_coredev(self, coredev):
        coredevname = coredev.split('/')[2]

        if self._is_deleting(coredev):
            status = "deleting_cache"
        else:
            _exec_status = 'cat /sys/block/%s/bcache/running' % coredevname
            try:
                out, err = _exec_shell1(_exec_status, p=False)
            except Exp as e:
                #  derror('dev:%s, ret: %d' % (coredev, e.errno))
                return None
            state = int(out.strip())
            if state == 1:
                status = 'running'
            else:
                status = 'stopped'

        return status


    def is_cset_online(self, cset_uuid):
        return os.path.isdir(os.path.join('/sys/fs/bcache', cset_uuid))

    def _stop_coredev_bcache_service(self, coredev):
        devname = coredev.split('/')[2]
        check_path = "/sys/block/%s/bcache" % (devname)
        if os.path.isdir(check_path):
            self.del_coredev_dangerously(coredev)
            time.sleep(1)

    def get_mappingdev_by_coredev(self, coredev):
        if not self.is_coredev(coredev):
            return None

        retry = 0
        while True:
            cmd = 'lsblk %s --raw -o NAME|grep -v NAME' % coredev
            try:
                out, err = _exec_shell1(cmd, p=False)
            except Exp as e:
                raise Exp(errno.EPERM, 'get bcachename failed\n')

            list_dev = out.strip().split('\n')

            if len(list_dev) < 2:
                if not self.all_cache_register:
                    self.register_all_cachedev()

                self.register_dev(coredev)
                #dwarn("check %s lsblk fail, will retry." % (coredev))
                retry = retry + 1
                if retry < 2:
                    time.sleep(1)
                    continue
                else:
                    return None
                    #raise Exp(errno.EPERM, 'check %s lsblk fail' % (coredev))
            else:
                break

        if coredev == '/dev/' + list_dev[0]:
            return '/dev/' + list_dev[1]

        return None

    def get_coredev_by_fastdev(self, fastdev):
        bcachename = fastdev.split('/')[2]
        abs_ln = '/sys/block/%s/bcache' % bcachename
        if os.path.islink(abs_ln):
            target = os.readlink(abs_ln)
            coredev = target.split('/')[-2]
            return '/dev/' + coredev
        else:
            return None

    def get_cachedev_by_coredev(self, coredev):
        cset_uuid = self.get_cset_uuid_by_dev(coredev)
        cachedir = os.path.join('/sys/fs/bcache', cset_uuid)
        if os.path.exists(cachedir) and os.path.isdir(cachedir):
            files = os.listdir(cachedir)
            key = 'cache'
            for f in files:
                if key in f:
                    abs_ln = os.path.join(cachedir, f)
                    if os.path.islink(abs_ln):
                        target = os.readlink(abs_ln)
                        cachedev = target.split('/')[-2]
                        return '/dev/' + cachedev
        return None

    def get_cset_uuid_by_dev(self, dev):
        _exec = "bcache-super-show %s | grep cset.uuid | awk '{print $2}'" % dev
        try:
            out, err = _exec_shell1(_exec, p=False)
        except Exp as e:
            raise Exp(errno.EPERM, 'cant get coredev uuid\n')
        return out.strip()

    def is_attached_to_cache(self, coredev, cachedev):
        core_cset_uuid = self.get_cset_uuid_by_dev(coredev)
        cache_cset_uuid = self.get_cset_uuid_by_dev(cachedev)
        if core_cset_uuid != cache_cset_uuid:
            return False
        else:
            return True

    def attach_device(self, cset_uuid, dev):
        short_name = dev.split('/')[2]
        path = '/sys/block/%s/bcache/attach' % short_name
        cmd = 'echo %s > %s' % (cset_uuid, path)
        _exec_shell1(cmd, p=True)

    def _add_coredev_to_cache(self, coredev, cachedev):
        if not self.is_coredev(coredev):
            raise Exp(errno.EINVAL, '%s not a coredev\n' % (coredev))

        cset_uuid = self.get_cset_uuid_by_dev(cachedev)

        try:
            self.attach_device(cset_uuid, coredev)
        except Exp, e:
            if self.is_attached_to_cache(coredev, cachedev):
                dmsg("%s already attached to cache device : %s, just ignore it !" % (coredev, cachedev))
                pass
            else:
                raise Exp(errno.EPERM, "add coredev:%s to %s fail, %s" % (coredev, cachedev, e.err))

    def set_cache_policy(self, coredev, cache_policy):
        if not self.is_coredev(coredev):
            raise Exp(errno.EINVAL, '%s not a coredev\n' % (coredev))

        self.set_cache_mode(coredev, cache_policy['cache_mode'])
        self.set_cache_seq_cutoff(coredev, cache_policy['sequential_cutoff'])
        self.set_cache_wb_percent(coredev, cache_policy['writeback_percent'])
        self.cacheset('writeback_delay', '5', coredev)

    def set_cache_wb_percent(self, coredev, value=None):
        if value is None or len(value) == 0:
            value = self.config.cache_wb_percent

        return self.cacheset('writeback_percent', value, coredev)

    def set_cache_seq_cutoff(self, coredev, value=None):
        if value is None or len(value) == 0:
            value = self.config.cache_seq_cutoff

        return self.cacheset('sequential_cutoff', value, coredev)

    def set_cache_mode(self, coredev, value=None):
        list_mode = ['writeback', 'writethrough', 'writearound', 'none']
        if value is None or len(value) == 0:
            value = self.config.cache_mode
        elif value not in list_mode:
            raise Exp(errno.EINVAL, "bad cache mode:%s, must be writethrough|writeback|writearound|none" % (value))

        return self.cacheset('cache_mode', value, coredev)

    def get_relate_coredev_by_cachedev(self, cachedev):
        list_coredev = []

        cset_uuid = self.get_cset_uuid_by_dev(cachedev)
        cachedir = os.path.join('/sys/fs/bcache', cset_uuid)
        if os.path.isdir(cachedir):
            files = os.listdir(cachedir)
        else:
            files = []

        key = 'bdev'
        for f in files:
            if key in f:
                abs_ln = os.path.join(cachedir, f)
                target = os.readlink(abs_ln)
                coredev = target.split('/')[-2]
                list_coredev.append(coredev)
        return list_coredev

    def list_coredevs_by_cachedev(self, cachedev):
        list_coredevs = []
        list_coredevname = self.get_relate_coredev_by_cachedev(cachedev)
        for coredevname in list_coredevname:
            coredev = '/dev/' + coredevname
            list_coredevs.append(coredev)

        return list_coredevs

    def is_clean_state(self, coredev):
        coredevname = coredev.split('/')[2]
        _exec_state = 'cat /sys/block/%s/bcache/state' % (coredevname)
        try:
            out, err = _exec_shell1(_exec_state, p=False)
        except Exp as e:
            return False
        state = out.strip()
        if 'clean' in state:
            return True
        else:
            return False

    def __del_coredev(self, coredev, cset_uuid):
        coredevname = coredev.split('/')[2]
        abs_detach = '/sys/block/' + coredevname + '/bcache/detach'
        _exec_detach = 'echo %s > %s' % (cset_uuid, abs_detach)
        _exec_shell1(_exec_detach, p=True)

        abs_stop = '/sys/block/' + coredevname + '/bcache/stop'
        _exec_stop = 'echo 1 > %s' % abs_stop
        _exec_shell1(_exec_stop, p=True)

        _exec_dd = 'dd if=/dev/zero of=%s count=1 bs=1M oflag=direct' % coredev
        _exec_shell1(_exec_dd, p=True)

    def __del_coredev_no_detach(self, coredev):
        coredevname = coredev.split('/')[2]
        mappingdev = self.get_mappingdev_by_coredev(coredev)

        try:
            abs_stop = '/sys/block/' + coredevname + '/bcache/stop'
            _exec_stop = 'echo 1 > %s' % abs_stop
            _exec_shell1(_exec_stop, p=True)
        except Exp, e:
            pass

        try:
            _exec_dd = 'dd if=/dev/zero of=%s count=1 bs=1M oflag=direct' % (coredev)
            _exec_shell1(_exec_dd, p=True)
        except Exp, e:
            pass

        #remove from docker
        if mappingdev is not None and os.path.exists(mappingdev):
            cmd = "rm -rf %s" % (mappingdev)
            _exec_shell1(cmd)

    def is_all_deleted(self, list_coredev):
        index = 0
        length = len(list_coredev)
        if length == 0:
            return True

        def __is_coredev_deleted(dev):
            _exec_check = "bcache-super-show %s 2>/dev/null| grep 'backing device'" % dev
            try:
                out, err = _exec_shell1(_exec_check, p=True)
                return False
            except Exp as e:
                #  deleted success
                return True

        while index < length:
            deleted = __is_coredev_deleted(list_coredev[index])
            if not deleted:
                print 'waiting for delete %s' % (list_coredev[index])
                time.sleep(1)
                continue
            else:
                index += 1
        return True

    def del_cachedev(self, cachedev):
        if not self.is_cachedev(cachedev):
            raise Exp(errno.EINVAL, '%s not a cachedev\n' % cachedev)

        cset_uuid = self.get_cset_uuid_by_dev(cachedev)
        coredevs = self.list_coredevs_by_cachedev(cachedev)
        for coredev in coredevs:
            if len(coredev) == 0:
                continue
            self.del_coredev_dangerously(coredev)

        deleted = self.is_all_deleted(coredevs)
        if deleted:
            _exec_stop_cache = 'echo 1 > /sys/fs/bcache/%s/stop' % cset_uuid
            _exec_shell1(_exec_stop_cache, p=True)
            time.sleep(1)

            _exec_dd = 'dd if=/dev/zero of=%s count=1 bs=1M oflag=direct' % cachedev
            _exec_shell1(_exec_dd, p=True)

    def del_coredev_dangerously(self, coredev):
        if not self.is_coredev(coredev):
            raise Exp(errno.EINVAL, '%s not a coredev\n' % (coredev))

        self.__del_coredev_no_detach(coredev)

    def del_coredev(self, coredev, is_flush):
        if not self.is_coredev(coredev):
            raise Exp(errno.EINVAL, '%s not a coredev\n' % (coredev))

        fd = _lock_file1("/var/run/cache_log.lock")
        cmd = "grep 'delcoredev %s' %s || echo 'delcoredev %s %d' >> %s" %\
                  (coredev, self.cache_log, coredev, int(is_flush), self.cache_log)
        _exec_shell1(cmd, p=False)
        _unlock_file1(fd)

        if is_flush:
            self.set_cache_wb_percent(coredev, "0")

        pid = os.fork()
        if pid == 0:
            while not self.is_clean_state(coredev):
                time.sleep(2)
            cset_uuid = self.get_cset_uuid_by_dev(coredev)
            mappingdev = self.get_mappingdev_by_coredev(coredev)
            super_8k_buff = self._get_superinfo(mappingdev, BCACHE_HEADER_LEN, 0)
            softdisk = self.get_softdisk_by_dev(mappingdev)

            self.__del_coredev(coredev, cset_uuid)

            self._resume_8k_superinfo(coredev, super_8k_buff)
            self.node.disk_manage.disk_setmeta_cset(coredev, '00000000-0000-0000-0000-000000000000')
            self._update_ln_to_hdd(mappingdev, softdisk, coredev)

            #remove from docker
            if os.path.exists(mappingdev):
                cmd = "rm -rf %s" % (mappingdev)
                _exec_shell1(cmd)

            # delete from /opt/fusionstack/log/cache.log
            fd = _lock_file1("/var/run/cache_log.lock")
            cmd = "sed -i '/delcoredev %s/d' %s" % (coredev.replace("/dev/", "\/dev\/"), self.cache_log)
            _exec_shell1(cmd, p=True)
            _unlock_file1(fd)
            sys.exit(0)
        elif pid > 0:
            return
        else:
            raise Exp(errno.EPERM, 'fork error\n')

    def flush_cachedev(self, cachedev):
        return

    def flush_coredev(self, coredev):
        return

    def is_valid_bcachedev(self, bcachedev):
        bcachename = self.get_dev_shortname(bcachedev)
        if not self.is_bcache_device_file(bcachename):
            return False

        cmd = "lsblk | grep '%s ' | wc -l" % (bcachename)
        out, err = _exec_shell1(cmd, p=False)
        count = int(out.strip('\n'))
        if count == 2: #must be 2 records, the one is in coredev, the other is in cachedev
            return True
        else:
            return False

    def is_bcache_device_file(self, name):
        if name.startswith("bcache"):
            if not os.path.isdir('/dev/%s' % name):
                return True
        return False

    def get_dev_shortname(self, dev):
        if dev.startswith("/dev/"):
            shortname = dev[5:]
        else:
            shortname = dev
        return shortname

    def _cacheset(self, key, value, cachedev_name):
        path = "/sys/block/%s/bcache/%s" % (cachedev_name, key)
        if os.path.isfile(path):
            _set_value(path, value)
        else:
            # raise Exp(errno.ENOENT, "key:%s not found in %s." % (key, cachedev_name))
            pass

    def cacheset(self, key, value, cachedev):
        fail_list = []
        succ_list = []

        if cachedev is not None:
            cachedev_name = self.get_dev_shortname(cachedev)
            self._cacheset(key, value, cachedev_name)
        else:
            for cachedev_name in os.listdir("/dev"):
                if self.is_bcache_device_file(cachedev_name):
                    try:
                        self._cacheset(key, value, cachedev_name)
                        succ_list.append(cachedev_name)
                    except Exp, e:
                        derror("%s set %s %s fail. %s" % (cachedev_name, key, value, e.err))
                        fail_list.append(cachedev_name)
                        continue

        if len(fail_list):
            print "cache set finish :"
            print "    fail list : %s" % (fail_list)
            print "    success list : %s" % (succ_list)
        else:
            # print "cache set %s value %s ok!" % (key, value)
            pass

    def _cacheget(self, key, cachedev_name):
        if key == 'all':
            keys = ['cache_mode', 'writeback_percent', 'sequential_cutoff', 'dirty_data']
        else:
            keys = [key]

        buf = StringIO()
        buf.write('%-10s' % cachedev_name)

        for key in keys:
            path = "/sys/block/%s/bcache/%s" % (cachedev_name, key)
            if not os.path.isfile(path):
                derror('key: %s not found in %s' % (key, cachedev_name))
                continue

            value = _get_value(path)
            value = value.strip(' \n')

            buf.write(' - %s: %s' % (key, value))

        dmsg(buf.getvalue())

    def cacheget(self, key, cachedev=None):
        fail_list = []
        succ_list = []

        if cachedev is not None:
            cachedev_name = self.get_dev_shortname(cachedev)
            self._cacheget(key, cachedev_name)
        else:
            devs = [dev for dev in os.listdir('/dev/') if self.is_bcache_device_file(dev)]
            devs.sort()

            for dev in devs:
                try:
                    self._cacheget(key, dev)
                    succ_list.append(dev)
                except Exp, e:
                    derror("%s get %s fail. %s" % (dev, key, e.err))
                    fail_list.append(dev)
                    continue


def main():
    force = False
    op = ""
    ext = None
    cachedev = None
    coredev = None

    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            'hvat', ['cachedev=', 'coredev=', 
                     'bind_cache', 'destroy_cache',
                     'cacheset', 'cacheget'
                     ]
        )
    except getopt.GetoptError, err:
        print str(err)
        usage()
        exit(errno.EINVAL)

    newopts = copy.copy(opts)
    for o, a in opts:
        if o in ('--cachedev'):
            cachedev = a
            newopts.remove((o, a))
        elif o in ('--coredev'):
            coredev = a
            newopts.remove((o, a))
        elif o in ('--force'):
            force = True
            newopts.remove((o, a))

    try:
        bcache_manage = BcacheManage()
    except Exp, e:
        derror(e.err)
        exit(e.errno)

    for o, a in newopts:
        if o in ('--help'):
            usage()
            exit(0)
        elif (o == '--bind_cache'):
            op = o
            if '--cachedev' in args:
                idx = args.index('--cachedev')
                if len(args) <= idx:
                    usage()
                    exit(errno.EINVAL)
                cachedev = args[idx+1]
                args.remove(cachedev)
                args.remove('--cachedev')

            if '--coredev' in args:
                idx = args.index('--coredev')
                if len(args) <= idx:
                    usage()
                    exit(errno.EINVAL)
                coredev = args[idx+1]
                args.remove(coredev)
                args.remove('--cachedev')
            ext = args
        elif (o == '--destroy_cache'):
            op = o
            if '--cachedev' in args:
                idx = args.index('--cachedev')
                if len(args) <= idx:
                    usage()
                    exit(errno.EINVAL)
                cachedev = args[idx+1]
                args.remove(cachedev)
                args.remove('--cachedev')
            ext = args
        elif (o == '--cacheset'):
            op = o
            if '--cachedev' in args:
                idx = args.index('--cachedev')
                if len(args) <= idx:
                    usage()
                    exit(errno.EINVAL)
                cachedev = args[idx+1]
                args.remove(cachedev)
                args.remove('--cachedev')
            ext = args
            if len(ext) != 2:
                derror('need enough param.')
                exit(errno.EINVAL)
        elif (o == '--cacheget'):
            op = o
            if '--c' in args:
                idx = args.index('--cachedev')
                if len(args) <= idx:
                    usage()
                    exit(errno.EINVAL)
                cachedev = args[idx+1]
                args.remove(cachedev)
                args.remove('--cachedev')
            ext = args

    if op == '--cacheset':
        try:
            bcache_manage.cacheset(ext[0], ext[1], cachedev)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            raise
    elif op == '--cacheget':
        try:
            if ext:
                bcache_manage.cacheget(ext[0], cachedev)
            else:
                bcache_manage.cacheget('all', cachedev)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            raise
    elif op == '--bind_cache':
        if cachedev is None or coredev is None:
            derror('need enough param.')
            exit(errno.EINVAL)
        try:
            bcache_manage.bind_cache(cachedev, coredev)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            raise
    elif op == '--destroy_cache':
        if cachedev is None:
            derror('need enough param.')
            exit(errno.EINVAL)
        try:
            bcache_manage.del_cachedev(cachedev)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            raise

def usage():
    print ("usage:")
    print (sys.argv[0] + " --bind_cache --cachedev <cachedev> --coredev <dev1,dev2,dev3>")
    print (sys.argv[0] + " --destroy_cache --cachedev <cachedev>")
    print (sys.argv[0] + " --cacheset <key> <value>")
    print (sys.argv[0] + " --cacheget <key>")
    print

if __name__ == '__main__':
    if len(sys.argv) == 1:
        usage()
    else:
        main()
