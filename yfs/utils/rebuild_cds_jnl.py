#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import struct
import zlib
import threading
import time as tm

class rebuildWorker(threading.Thread):

    op = 1
    head_magic = 0x6d12ecaf
    head_time = int(tm.time())
    head_len = 56

    def __init__(self, num, x86_64):
        self.num = num
        self.is_x86_64 = x86_64

        self.cds_chunk = '/sysy/yfs/cds/%d/ychunk' % num
        self.old_jnl_dir = '/sysy/yfs/cds/%d/ydisk/jnl' % num
        self.bk_jnl_dir = '/sysy/yfs/cds/%d/ydisk/jnlbk' % num
        self.new_jnl = '%s/0' % self.old_jnl_dir

        now = tm.strftime('%m-%d-%H:%M:%S', tm.localtime())
        self.bk_jnl_dir = '%s-%s' % (self.bk_jnl_dir, now)

        threading.Thread.__init__(self)


    def printf(self, msg):
        print 'Thread [%d]: %s' % (self.num, msg)


    def get_chkmate(self, chkpath):
        try:
            fd = open(chkpath, 'rb')
            chk_meta = fd.read(80);

            (crc, crc_ver, fileid_id, fileid_ver, __pack1__, chkno, __pack2__, \
                                chk_id, chk_ver, __pack3__, chkversion, chkoff, chklen, volid, \
                                __blank1__, __blank2__, __blank3__, __blank4__) = struct.unpack('<IIQIIIIQIIIIIIIIII', chk_meta)
        except:
            raise

        fd.close()

        return (crc, crc_ver, fileid_id, fileid_ver, __pack1__, chkno, __pack2__, \
                                chk_id, chk_ver, __pack3__, chkversion, chkoff, chklen, volid, \
                                __blank1__, __blank2__, __blank3__, __blank4__)


    def rebuild_jnl(self, path):
        for i in os.listdir(path):
            newpath = os.path.normpath('%s/%s' % (path, i))

            if os.path.isdir(newpath):
                self.printf('Enter dir %s' % newpath)
                self.rebuild_jnl(newpath)
            else:
	    	print newpath
                try:
                    (crc, crc_ver, fileid_id, fileid_ver, __pack1__, chkno, __pack2__, \
                            chk_id, chk_ver, __pack3__, chkversion, chkoff, chklen, volid, \
                            __blank1__, __blank2__, __blank3__, __blank4__) = self.get_chkmate(newpath)
                except:
                    self.printf('Error: Get ***%s*** chunk mate error' % newpath)
                    continue


                chk_meta = struct.pack('<IIQIIIIIIIIII', self.op, 0, chk_id, chk_ver, 0, chkversion, chkoff, chklen, volid, 0, 0, 0, 0)

                head_crc = zlib.crc32(chk_meta)

                if head_crc < 0:
                        head_crc = head_crc & 0xffffffff

                jnl_meta = struct.pack('<IIIIIIQIIIIIIIIII', self.head_magic, self.head_time, self.head_len, head_crc, self.op, 0, chk_id, chk_ver, 0, chkversion, chkoff, chklen, volid, 0, 0, 0, 0)
                self.jnl_fd.write(jnl_meta)

                print  self.head_magic, self.head_time, self.head_len, head_crc, self.op, 0, chk_id, chk_ver, 0, chkversion, chkoff, chklen, volid, 0, 0, 0, 0

    def backup_old_jnl(self):
        self.printf('rename %s to %s' % (self.old_jnl_dir, self.bk_jnl_dir))
        os.rename(self.old_jnl_dir, self.bk_jnl_dir)

        try:
            self.printf('mkdir %s' % self.old_jnl_dir)
            os.mkdir(self.old_jnl_dir)
        except OSError, (errno, strerror):
            self.printf('rename or mkdir Error [%d]: %s, exiting...' % (errno, strerror))
            sys.exit(1)


    def run(self):
        self.printf('running...')
        self.backup_old_jnl()

        assert(not os.path.exists(self.new_jnl))
        try:
            self.jnl_fd = open(self.new_jnl, 'ab')
        except IOError, (errno, strerror):
            self.printf('Write new jnl Error: [%d] %s' % (errno, strerror))
            sys.exit(1)

        self.printf('start rebuild journal')

        self.rebuild_jnl(self.cds_chunk)
        self.jnl_fd.close()


def usage():
    print 'Usage: %s [cds_number]' % sys.argv[0]
    sys.exit(1)

if __name__ == '__main__':
    workers = []

    if len(sys.argv) != 2:
        usage()

    try:
        cds_no = int(sys.argv[1])
    except ValueError:
        usage()

    is_x86_64 = '86_64' in os.popen('uname -a').read()
    if is_x86_64:
        print 'Notice: this computer identify as **x86_64**'
    else:
        print 'Notice this computer identify as **i386**'

    print 'All cds journal rebuild will start in 3 seconds ...'
    print '************************************************'
    tm.sleep(3)

    for i in range(1, cds_no+1):
        tmp = rebuildWorker(i, is_x86_64)
        workers.append(tmp)
        tmp.start()

    for i in workers:
        i.join()
        print 'Thread [%d]: exit' % i.num

    print '************************************************'
    print 'All cds journal rebuild finished'

