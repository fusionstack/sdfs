#!/usr/bin/python
#-*- coding: utf-8 -*-

import os
import sys
import time
import struct
import binascii
import yfs

sys.path += ['../libs']
from daemon import *

import conf

class cds_robot:
    
    DIR_SUB_MAX = 1024
    CRCLEN = 4096
    NULL_CRC_CDS_VER = 0xface0001
    INTERVAL = 10

    def __init__(self, cds_number):
        self.cds_no = cds_number
        self.chunkdir = '%s/%d/ychunk' % (conf.CDSDIR, self.cds_no)
        self.jnldir = '%s/%d/ydisk/jnl' % (conf.CDSDIR, self.cds_no)
        self.diskinfo = '%s/%d/ydisk/diskinfo' % (conf.CDSDIR, self.cds_no)
        self.jnl_chklist = {}
        self.real_chklist = []
        self.is_x86_64 = '86_64' in os.popen('uname -a').read()

        self.cdsinfo_len = 12

        if self.is_x86_64:
            self.chkmd_len = 64
        else:
            self.chkmd_len = 56

        yfs.init()

    def get_diskinfo(self):
        try:
            fd = open(self.diskinfo, 'rb')
            cdsinfo = fd.read(self.cdsinfo_len)

            (self.disk_id, self.disk_version) = \
                    struct.unpack('<QI', cdsinfo)

            logdbg('disk id: %d, disk verison: %d' % \
                    (self.disk_id, self.disk_version))
        except:
            raise

    def cascade_id2path(self, chk_id):
        dpath = ''

        while chk_id != 0:
            dirid = chk_id % self.DIR_SUB_MAX
            chk_id /= self.DIR_SUB_MAX;

            dpath = os.path.join(dirid, dpath)

        return os.path.normpath(dpath)


    def get_jnl_chklist(self, jnldir):
        """Read a cds journal
        and get the last journal path.
        """
        journals = os.listdir(jnldir)

        self.lastjnl = str(max([int(i) for i in journals]))
        self.lastjnl = os.path.join(jnldir, self.lastjnl)

        for i in journals:
            newpath = os.path.join(jnldir, i)
            self.read_jnl(newpath)


    def read_jnl(self, jnlpath):
        """Read one journal"""
        try:
            fd = open(jnlpath, 'rb')
            logout('Open journal: %s' % jnlpath)

            while True:
                jnl_content = fd.read(28)
                if not jnl_content:
                    break

                (magic, op, id, ver, tm, version) = \
                        struct.unpack('<IIQIII', jnl_content)
                
#                 logdbg('magic %d, op %d, id %d, ver %d, tm %d, version %d' % \
#                         (magic, op, id, ver, tm, version))

                if not (self.jnl_chklist.has_key((id, ver)) and \
                        self.jnl_chklist[(id, ver)][3] >= version):
                    self.jnl_chklist[(id, ver)] = (magic, op, tm, version)  
                
                if self.jnl_chklist.has_key((id, ver)) and op == 2: #chunk del
                    del self.jnl_chklist[(id, ver)]

        except IOError, (errno, strerror):
            logerr('%d: %s' % (errno, strerror))
            raise

        fd.close()

    def jnl_chkdel(self, id, ver):
        logdbg('del %d_v%d add to journal' % (id, ver))
        jnl_content = struct.pack('<IIQIII', 0, 2, id, ver, time.time(), 0)

        try:
            fd = open(self.lastjnl, 'ab')
            fd.write(jnl_content)
        except:
            raise

        fd.close()


    def get_chkcrc(self, fd):
        self.crc_code = []
        fd.seek(self.chkmd_len)

        for i in range(256):
            buf = fd.read(16)
            (a, b, c, d) = struct.unpack('<IIII', buf)
            self.crc_code += [a, b, c, d]
        
    def verify_crc(self, fd, chkpath, need_verify):
        i = 0
        seglen = 64 * 1024

        self.get_chkcrc(fd)

        chklen = os.fstat(fd.fileno()).st_size
        left = chklen - self.chkmd_len - self.CRCLEN
        fd.seek(self.chkmd_len + self.CRCLEN)

        while left > 0:
            if left < seglen:
                needread = left
            else:
                needread = seglen

            try:
                buf = fd.read(needread)
            except IOError, (errno, strerror):
                logerr('Err: %s (%d) %s' % (chkpath, errno, strerror))
                break

            crc = binascii.crc32(buf) & 0xffffffff
            if crc != self.crc_code[i] and need_verify:
                logerr('Err: %s segment %d crcode not match' % (chkpath, i))
                break

            left -= needread
            i += 1

        if left != 0:
            return False
        else:
            return True

    def get_mdcrc(self, fd):
        fd.seek(4)

        buf = fd.read(self.chkmd_len - 4)

        return binascii.crc32(buf) & 0xffffffff

    def verify_chunk(self, chkpath, jnl_meta):
        logdbg('verify chunk: %s' % chkpath)
        logdbg('**** meta data in journal is: ****')
        logdbg('  magic  : %d' % jnl_meta[0])
        logdbg('  op     : %d' % jnl_meta[1])
        logdbg('  time   : %s' % time.strftime('%Y/%m/%d-%H:%M:%S', time.localtime(jnl_meta[2])))
        logdbg(' version: %d' % jnl_meta[3])

        try:
            fd = open(chkpath, 'rb')
            chk_meta = fd.read(self.chkmd_len) 

            if self.is_x86_64:
                (_crcode, _cds_ver, _id, _ver, _junka, \
                _tm, _chkver, _status, _chkno, _offset, \
                _count , _fileid_id, _fileid_ver, _junkb) = \
                struct.unpack('<IIQIIIIIIIIQII', chk_meta)
            else:
                (_crcode, _cds_ver, _id, _ver, \
                _tm, _chkver, _status, _chkno, _offset, \
                _count , _fileid_id, _fileid_ver,) = \
                struct.unpack('<IIQIIIIIIIQI', chk_meta)

            if _cds_ver > self.NULL_CRC_CDS_VER:
                ret = self.verify_crc(fd, chkpath, True)
            else:
                ret = self.verify_crc(fd, chkpath, False)

            mdcrc = self.get_mdcrc(fd)
            if mdcrc != _crcode:
                logerr('Err: chk %s meta data crcode verify error' % chkpath)
                ret = False

            logdbg('**** meta data in chunk is: ****')
            logdbg('  crcode : %d' % _crcode)
            logdbg('  realcrc: %d' % mdcrc)
            logdbg('  cdsver : %d' % _cds_ver)
            logdbg('  chkid  : %d' % _id)
            logdbg('  chkver : %d' % _ver)
            logdbg('  time   : %s' % time.strftime('%Y/%m/%d-%H:%M:%S',
                time.localtime(_tm)))
            logdbg('  version: %d' % _chkver)
            logdbg('  status : %d' % _status)
            logdbg('  chkno  : %d' % _chkno)
            logdbg('  offset : %d' % _offset)
            logdbg('  count  : %d' % _count)
            logdbg('  fileid : %d' % _fileid_id)
            logdbg('  filever: %d' % _fileid_ver)
            logdbg('')

        except IOError:
            ret = False

        fd.close()
        return ret

    def run_once(self):
        """main loop starts here"""
        try:
            self.get_jnl_chklist(self.jnldir)

            for k, v in self.jnl_chklist.items():
                id = k[0]
                version = k[1]

                if id < 1024:
                    subpath = ''
                else:
                    subpath = self.cascade_id2path(id)

                chkpath = os.path.normpath('%s/%s/%d_v%d' %
                        (self.chunkdir, subpath, id, version))

                if not self.verify_chunk(chkpath, v):
                    self.get_diskinfo()
                    logout('disk %d_%d, bad chunk: %s, report to mds...' % \
                            (self.disk_id, self.disk_version, chkpath))

                    yfs.mdc_badchk(id, version, self.disk_id, self.disk_version)
                    self.jnl_chkdel(id, version)
                    try:
                        logdbg('delete invalid chk %s' % chkpath)
                        os.unlink(chkpath)
                    except:
                        pass

        except:
            raise

    def start(self):
        try:
            while True:
                self.run_once()
                time.sleep(self.INTERVAL)
        except:
            self.finish()
            raise


    def finish(self):
        yfs.destroy()


if __name__ == '__main__':
    robot = cds_robot(int(sys.argv[1]))
    robot.start()

