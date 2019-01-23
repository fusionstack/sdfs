#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import errno

from argparse import RawTextHelpFormatter

BLOCK_SIZE = 4096
HEADER = 4096
BLKSIZE = 592
CHUNK_SPLIT = 64*1024*1024

def human_readable(num, idx=0):
    u = ['B', 'K', 'M', 'G', 'T']
    #print ("num %u idx %u" % (num, idx))
    if (num / 1024 < 1):
        return str(int(num)) + u[idx]
    else:
        return human_readable(num / 1024, idx + 1)

def getsize(size):
    try:
        return int(size)
    except ValueError, e:
        pass

    u = {'B': 1024**0, 'K': 1024**1, 'M': 1024**2, 'G': 1024**3}
    _u = size[-1].upper()
    if _u not in u.keys():
        err = "need unit. exampe 4B 4K 4M 4G"
        sys.stderr.write("%s\n" % (err))
        sys.exit(1)
    
    return int(size[:-1])*u[_u]

def get_blocks(size):
    n = size/CHUNK_SPLIT
    if size%CHUNK_SPLIT > 0:
        n = n + 1

    n = BLKSIZE * n
    m = n/BLOCK_SIZE
    if n%BLOCK_SIZE > 0:
        m = m + 1

    return m

def mdsize(filenum, size, repnum):
    """
    key: 16 字节(取中间值)

    @@: 2 字节，自身
    container_blk_t(md_proto_t): 8 + 160 字节，自身

    name: 128 字节 (取中间值)
    dir_entry_t: 32 字节

    idx: 5 字节 (取中间值)
    container_blk_t(chkinfo_t): 8 + 72 + 16 * repnum 字节
    """
    blocks = get_blocks(size)
    total = 16 + (2 + 8 + 160) + (128 + 32) + (5 + 8 + 72 + 16 * repnum) * blocks 
    total = total * filenum

    return total

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)

    parser.add_argument("-n", "--filenum", required=True, type=int, help="the total number of files")
    parser.add_argument("-s", "--size", required=True, help="the average size(B, K, M, G) of files")
    parser.add_argument("-r", "--repnum", required=True, type=int, help="repnum of chunk")

    args = parser.parse_args()

    size = getsize(args.size)
    md_size = mdsize(args.filenum, size, args.repnum)

    print "filenum          : ", args.filenum
    print "average size     : ", args.size
    print "meta size        : ", human_readable(md_size)
