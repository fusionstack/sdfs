#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
import struct
import getopt

def DMSG(msg):
    if verbose:
        print msg

def get_fid(path):
    """struct id_t
       x86_64 
              xxxxxxxx
              xxxx****
       i386   
              xxxxxxxx
              xxxx
    """
    try:
        fd = open(path, 'rb')
        chk_meta = fd.read(12)
        id, ver = struct.unpack('<QI', chk_meta)
    except:
        raise

    fd.close()
    return (id, ver)

def cascade_id2path(chk_id):
    dpath = ''

    while chk_id != 0:
        dirid = chk_id % 1024
        chk_id /= 1024

        dpath = os.path.join(str(dirid), dpath)

    return os.path.normpath(dpath)


def walk_mdsfiles(path):
    for i in os.listdir(path):
        newpath = os.path.normpath('%s/%s' % (path, i))

        if os.path.isdir(newpath):
            walk_mdsfiles(newpath)
        else:
            try:
                (id, ver) = get_fid(newpath)
            except IOError, (errno, strerror):
                print 'Open %s error: %s' % (newpath, strerror)
                continue

            dpath = cascade_id2path(id)
            fidpath = os.path.normpath('%s/%s_v%d' % (mdsfid_path, dpath, ver))
            DMSG('%s -> %s' % (newpath[len(mdsfile_path):], fidpath))

            if not os.path.exists(fidpath):
                print 'Error: %s -> %s fid file not exists' %  (newpath[len(mdsfile_path):], fidpath)
            else:
                fid_mustbe.append(fidpath)

def walk_mdsfidfile(path):
    for i in os.listdir(path):
        newpath = os.path.normpath('%s/%s' % (path, i))

        if os.path.isdir(newpath):
            walk_mdsfidfile(newpath)
        else:
            fid_total.append(newpath)

def help():
    print 'help'
        
if __name__ == '__main__':
    verbose = False
    mdsfile_path = '/sysy/yfs/mds/1/file'
    mdsfid_path = '/sysy/yfs/mds/1/fileid'

    fid_total = []
    fid_mustbe = []

    optlist, args = getopt.getopt(sys.argv[1:], 'vh')
    for opt, arg in optlist:
        if opt == '-v':
            verbose = True
        elif opt == '-h':
            help()
        else:
            pass

    walk_mdsfiles(mdsfile_path)
    walk_mdsfidfile(mdsfid_path)

    fidnot_exists = list(set(fid_total) - set(fid_mustbe))

    open('fidnot_exists', 'w').write('\n'.join(fidnot_exists))

