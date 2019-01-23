#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time as tm
import struct


def get_chkmate(chkpath):
    try:
        fd = open(chkpath, 'rb')
        if is_x86_64:
            chk_meta = fd.read(32)
            crc, cdsver, chkid, chkver, junk, time, chkv = \
                    struct.unpack('<IIQIIII', chk_meta)
        else:
            chk_meta = fd.read(28)
            crc, cdsver, chkid, chkver, time, chkv = \
                    struct.unpack('<IIQIII', chk_meta)
    except:
        raise
        sys.exit(1)

    fd.close()

#     print chkid, chkver
#     print tm.strftime('%Y/%m/%d-%H:%M:%S', tm.localtime(time))

    return (chkid, chkver, time, chkv)

def rebuild_jnl(path):

    for i in os.listdir(path):  
        newpath = os.path.normpath('%s/%s' % (path, i)) 

        if os.path.isdir(newpath):  
            print 'Enter dir %s' % newpath
            rebuild_jnl(newpath)  
        else:
            #print 'rebuild chk %s ' % newpath
            (chkid, chkver, time, chkv) = get_chkmate(newpath)
            jnl_fd.write(struct.pack('<IIQIII', magic, op, chkid, chkver, time, chkv))

def usage():
    print 'Usage: %s [cds_number]' % sys.argv[0]
    sys.exit(1)

if __name__ == '__main__':
    op = 1
    magic = 0xcd5fe2a0

    if len(sys.argv) != 2:
        usage()

    try:
        cds_no = int(sys.argv[1])
    except ValueError:
        usage()

    cds_jnl_dir = '/sysy/yfs/cds/%d/ychunk' % cds_no
    new_jnl_path = '/sysy/yfs/cds/%d/ydisk/jnl/jnl.new' % cds_no

    is_x86_64 = '86_64' in os.popen('uname -a').read()
    if is_x86_64:
        print 'Notice: this computer identify as **x86_64**'
    else:
        print 'Notice this computer identify as **i386**'

    print 'journal rebuild will start in 3 seconds ...'
    print '************************************************'
    tm.sleep(3)

    try:
        jnl_fd = open(new_jnl_path, 'wb')
    except IOError, (errno, strerror):
        print 'Error: [%d] %s' % (errno, strerror)
        sys.exit(1)

    rebuild_jnl(cds_jnl_dir)
    jnl_fd.close()

    print '************************************************'
    print 'New journal was generated at "%s"' % new_jnl_path
