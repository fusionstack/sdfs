#!/usr/bin/env python

import os, sys
import time
import shutil
import hashlib
import threading
from optparse import OptionParser

def md5_file(filename):
    try:
        f = file(filename, 'rb')
    except:
        return ''

    m = hashlib.md5()
    
    bytes = f.read(2048)
    while bytes != '':
        m.update(bytes)
        bytes = f.read(2048)

    f. close()
    return m.hexdigest().upper()

def red(s):
    return '\033[0;31m%s\033[0m' % s

def green(s):
    return '\033[0;32m%s\033[0m' % s

class CheckThread(threading.Thread):
    def __init__(self, no, src, aim, mode):
        threading.Thread.__init__(self)
        self.no = no
        self.src = src
        self.md5 = '%s.md5' % src
        self.mode = mode
        self.disk = '/dev/%s' % aim
        self.mount = os.path.join('/mnt/disk_check', aim)

    def _disk_info(self, target):
        stat = os.statvfs(self.mount)
        size = stat.f_bsize * stat.f_blocks * 1.0
        free = stat.f_bsize * stat.f_bfree * 1.0
        print '+------> Thread %d : disk: %s, size: %.2fMb, use: %.2f%%, target: %s' % (self.no, self.disk, size/1024/1024, (size-free)/size, target) 

    def _disk_mount(self):
        if not os.path.exists(self.mount):
            os.makedirs(self.mount)

        if os.path.ismount(self.mount):
            for i in range(3):
                if os.system('umount -f %s' % self.mount) == 0:
                    break

        if os.system('mount %s %s' % (self.disk, self.mount)) != 0:
            print '+------> ', red('mount error, exit!')
            exit(1)

    def _log(self, e):
        log = file('/var/log/disk_check.log', 'a')
        log.write('%s\t%s\n' % (time.ctime(), e))
        log.flush()
        log.close()

    def _write(self):
        self._disk_mount()
        dir = os.path.join(self.mount, 'disk_check')

        if not os.path.exists(dir):
            os.makedirs(dir)

        f = file(os.path.join(dir, 'file_list'), 'a+')
        f.seek(0)
        buf = f.read()

        idx = buf.rfind('/')
        if idx == -1:
            idx = 0
        else:
            tmp = buf[idx + 1:].strip()
            if tmp.isdigit():
                idx = int(tmp) + 1
            else:
                idx = 0

        try:
            while True:
                target = os.path.join(dir, str(idx))
                shutil.copyfile(self.src, target)
                f.write('%s\n' % target)
                f.flush()
                self._disk_info(target)
                idx = idx + 1
            f.close()

        except IOError, e:
            print e
            self._log(e)
            return

    def _read(self):
        self._disk_mount()
        dir = os.path.join(self.mount, 'disk_check')

        if not os.path.exists(dir):
            print 'directory not exists, return'
            return

        try:
            f = file(self.md5, 'r')
            old_md5 = f.read().strip()
            f.close()

            print '+------> old md5:', green(old_md5)

            f = file(os.path.join(dir, 'file_list'), 'r')

            for line in f.readlines():
                line = line.strip()
                new_md5 = md5_file(line)
                if new_md5 != old_md5:
                    print '+------>', red('Thread : %d, check : %s') % (self.no, line)
                    self._log(line)
                    continue
                print '+------> Thread : %d, check : %s' % (self.no, line)

        except IOError, e:
            print e
            self._log(e)
            return 

    def run(self):
        if self.mode == 'read':
            self._read()
        elif self.mode == 'write':
            self._write()
        else:
            exit(1)

class DiskCheck:
    def __init__(self, disk, mode):
        self.disk = disk
        self.mode = mode 
        self.num = len(disk)
        self.size = 100 * 1024 * 1024
        self.src = '/dev/random'
        self.aim_root = '/dev/shm/disk_check'
        self.aim = os.path.join(self.aim_root, '100m')
        print '+-------------------------- Disk Check Start --------------------------+'
        print '+------> mode : %s' % self.mode

    def _rand_file(self):
        print '+------> ', green('make source file ...'),

        if os.path.exists(self.aim_root):
            print green('exist')
            return
        else:
            print

        os.makedirs(self.aim_root)

        os.system('dd if=/dev/urandom of=%s bs=%d count=1' % (self.aim, self.size))

        f = open('%s.md5' % self.aim, 'w')
        f.write(md5_file(self.aim))
        f.flush()
        f.close()

    def start(self):
        if self.mode == 'write':
            self._rand_file()

        threads = []
        for i in range(self.num):
            th = CheckThread(i, self.aim, self.disk[i], self.mode)
            th.start()
            threads.append(th)
        for th in threads:
            try:
                th.join()
            except KeyboardInterrupt:
                print 'exit'
                exit(1)

def main():
    if os.geteuid() != 0:
        print 'Need to be root!'
        exit(1)

    usage = 'Usage: %prog [options] args'
    parser = OptionParser(usage)
    parser.add_option('-m', '--mode', dest='mode', help='[read]: check md5. [write]: create file')

    (options, args) = parser.parse_args()

    if options.mode not in ['read', 'write'] or len(args) < 1:
        parser.print_help()
        exit(1)

    dc = DiskCheck(args, options.mode)
    dc.start()

if __name__ == '__main__':
    main()
