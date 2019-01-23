#!/usr/bin/python

import os
import sys
import getopt

flist = '/sysy/yfs/app/bin/junk_chunk.log'
errchunk = []

def origin2backup(chunk):
    return '%s.bk' % chunk

def rename_chunk(op):
    fd = open(flist, 'r')
    while True:
        chunk = fd.readline()
        if not chunk:
            break

        chunk = chunk.strip().split()[0]
        bak_chunk = origin2backup(chunk)

        try:
            if op == 'backup':
#                 print 'rename %s to %s' % (chunk, bak_chunk)
                os.rename(chunk, bak_chunk)
            elif op == 'debackup':
#                 print 'rename %s to %s' % (bak_chunk, chunk)
                os.rename(bak_chunk, chunk)
            else:
                print 'wrong op, exit...'
                EXIT(1)
        except OSError, (errno, strerror):
            errchunk.append('%s %s %s %d %s' % (
                op, chunk, bak_chunk, errno, strerror
                )
                )
            print '%s : chunk:%s bak_chunk:%s error: %s' % (
                    op, chunk, bak_chunk, strerror
                    )
    fd.close()

def ori2bak():
    rename_chunk('backup')

def bak2ori():
    rename_chunk('debackup')

def del_chunk():
    fd = open(flist, 'r')
    while True:
        chunk = fd.readline()
        if not chunk:
            break

        chunk = chunk.strip().split()[0]
        bak_chunk = origin2backup(chunk)
#         print 'delete %s' % bak_chunk
        try:
            os.unlink(bak_chunk)
        except OSError, (errno, strerror):
            errchunk.append(
                    'delete %s %d:%s' % (
                bak_chunk, errno, strerror
                )
                )
            print 'delete chunk:%s error: %s' % (
                    bak_chunk, strerror
                    )
    fd.close()

def help():
    print '%s [-b|-d|-r]' % sys.argv[0]
    print '    -b backup chunk'
    print '    -d debackup chunk'
    print '    -r remore backup chunk'
    EXIT(1)

def main():
    backup = False
    debackup = False
    deletechk = False

    if len(sys.argv) < 2:
        help()

    optlist, args = getopt.getopt(sys.argv[1:], 'bdr')
    for opt, arg in optlist:
        if opt == '-b':
            backup = True
        elif opt == '-d':
            debackup = True
        elif opt == '-r':
            deletechk = True
        else:
            help()

    if backup:
        ori2bak()
	exit(0)

    if debackup:
        bak2ori()
	exit(0)

    if deletechk:
        del_chunk()
	exit(0)

    open('delchunk.err', 'w').write('\n'.join(errchunk))

if __name__ == '__main__':
    main()
