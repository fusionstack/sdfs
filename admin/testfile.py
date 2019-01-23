#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import errno
import random
import hashlib

from argparse import RawTextHelpFormatter

def human_readable(num, idx=0):
    u = ['B', 'K', 'M', 'G', 'T']
    #print ("num %u idx %u" % (num, idx))
    if (num / 1024 < 1):
        return str(int(num)) + u[idx]
    else:
        return human_readable(num / 1024, idx + 1)

def get_size(size):
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

def _get_dirs(deep, deep_idx, width, parent):
    print '--deep: %s deep_idx: %s, width: %s, parent: %s' % (deep, deep_idx, width, parent)
    dirs = []

    for w in range(width):
        width_idx = w + 1
        d = "testfile%s_%s.dir" % (deep_idx, width_idx)
        d = os.path.join(parent, d)
        dirs.append(d)

    if deep_idx >= deep:
        #print '--deep: %s deep_idx: %s, width: %s, parent: %s' % (deep, deep_idx, width, parent)
        return []

    deep_idx = deep_idx + 1
    for p in dirs:
        subdirs = _get_dirs(deep, deep_idx, width, p)
        subdirs = [os.path.join(parent, x) for x in subdirs]
        #for d in subdirs:
            #print d
        dirs.extend(subdirs)

    #print '--deep: %s deep_idx: %s, width: %s, parent: %s' % (deep, deep_idx, width, parent)
    return dirs

def get_dirs(deep, width):
    return _get_dirs(deep, 1, width, "/")



def makesure_dir(path):
    cmd = "mkdir -p %s" % (path)
    print cmd
    os.system(cmd)

def is_empty(path):
    return os.listdir(path) == []

def rmsure_f(path):
    if os.path.isfile(path):
        cmd = "rm -rf %s" % (path)
        os.system(cmd)

        p = os.path.dirname(path)
        rmsure_f(p)

    if os.path.isdir(path):
        if is_empty(path):
            cmd = "rm -rf %s" % (path)
            p = os.path.dirname(path.rstrip('/'))
            rmsure_f(p)

def write_file(path, size):
    m1 = hashlib.md5()
    with open(path, "w") as f:
        left = size
        while left > 0:
            context = get_random_str()
            u = len(context)
            if u > left:
                u = left
                context = context[:u]
            #print 'write size', u
            f.write(context)
            m1.update(context)
            left = left - u

    return m1.hexdigest()

def md5_file(path):
    m1 = hashlib.md5()
    with open(path, 'r') as f:
        for line in f:
            m1.update(line)
    return m1.hexdigest()

def get_random_str():
    return 'a' * random.randint(0, 1024*1024*10) + '\n'

def get_random_size(maxsize):
    return random.randint(0, maxsize)

def get_random_dir(deepmax):
    choices = []
    for x in range(deepmax):
        for y in range(x+1):
            choices.append(x)

    deep = random.choice(choices)
    p = []
    for i in range(deep):
        p.append(random.randint(0, 1000))
    p = [str(x) for x in p]

    return "/".join(p)

def testfile(target, deepmax, filenum, maxsize, begin, noclean):
    count = 0
    while True:
        count = count + 1
        files = {}

        i = begin
        while i < begin + filenum:
            size = get_random_size(maxsize)
            d = get_random_dir(deepmax)
            d = os.path.join(target, d)
            makesure_dir(d)

            f = os.path.join(d, 'file'+str(i))
            md5 = write_file(f, size)
            files.update({f: md5})
            print 'count: %s, write %s, %s, size: %s, md5: %s' % (count, f, i, human_readable(size), md5)
            i = i + 1

        for f in files.keys():
            md5 = md5_file(f)
            if md5 != files[f]:
                err = "f: %s, except md5: %s" % (f, md5)
                raise Exception(err)
            print 'count: %s, check %s ok' % (count, f)
            rmsure_f(f)
            print 'count: %s, rm %s ok' % (count, f)

        if clean:
            cmd = "rm -rf %s/*" % (target)
            os.system(cmd)
        else:
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)

    parser.add_argument("-t", "--target", required=True, help="targe dir")
    parser.add_argument("-d", "--deep", required=True, type=int, help="")
    parser.add_argument("-f", "--filenum", required=True, type=int, help="filenum")
    parser.add_argument("-s", "--maxsize", default='100M', help="file size B|K|M|G")
    parser.add_argument("-b", "--begin", default='0', type=int, help="")
    parser.add_argument("-c", "--clean", default=1, type=int, help="")
    #parser.add_argument("-b", "--obs", default=1024*1024*100, type=int, help="BYTES bytes at a time")

    args = parser.parse_args()

    deep = args.deep
    target = args.target
    filenum = args.filenum
    maxsize = get_size(args.maxsize)
    begin = args.begin
    clean = args.clean

    testfile(target, deep, filenum, maxsize, begin, clean)

    #print "filenum          : ", args.filenum
    #print "average size     : ", args.size
    #print "meta size in disk: ", human_readable(md_size)
