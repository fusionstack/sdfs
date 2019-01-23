#!/usr/bin/python

import os
import sys
import getopt
import threading

class gen_data(threading.Thread):
    FILLINGDATA = 'ABCDEFGH' * 128 * 1024# length 1024
    DIR_SUB_MAX = 1024

    def __init__(self, filesize, filenum, rootpath):
        self.filesize = filesize
        self.rootpath = rootpath
        self.filenum = filenum
        self.logpath = os.path.normpath('%s/%d.log' % (rootpath, filesize))

        threading.Thread.__init__(self)


    def _num2path(self, num):
        dpath = ''

        while num != 0:
            dirid = num % self.DIR_SUB_MAX;
            num /= self.DIR_SUB_MAX;
            dpath = os.path.join(str(dirid), dpath)
        
        return os.path.normpath(dpath)


    def gen(self, filepath, filesize):
        if not os.path.exists(filepath):
            try:
                fd = open(filepath, 'w')
            except IOError, (errno, strerror):
                print 'open %s error (%d): %s' % (filepath, errno, strerror)

            while filesize > 0:
                try:
                    fd.write(self.FILLINGDATA)
                    fd.flush()
                except IOError:
                    print 'write data to %s error' % filepath
                filesize -= len(self.FILLINGDATA)

            fd.close()
        else:
            filesize = 0

        if filesize <= 0:
            try:
                self.logfd.write('%s\n' % filepath)
                self.logfd.flush()
            except IOError, (errno, strerror):
                print 'write log %s error (%d): %s' % \
                        (self.logpath, errno, strerror)


    def run(self):
        if os.path.exists(self.logpath):
            print 'logpath exists, exiting...'
            EXIT(0)

        try:
            self.logfd = open(self.logpath, 'w')
        except IOError, (errno, strerror):
            print 'open log %s error (%d): %s' % \
                    (self.logpath, errno, strerror)

        for i in xrange(self.filenum):
            if i < 1024:
                relapath = ''
            else:
                relapath = self._num2path(i)

            filedir = os.path.normpath('%s/%d/%s' % \
                    (self.rootpath, self.filesize, relapath))

            if not os.path.exists(filedir):
                os.system('mkdir -p %s' % filedir)

            filepath = os.path.normpath('%s/%d.file' % (filedir, i))
            self.gen(filepath, self.filesize)

        self.logfd.close()

def help():
    print '%s [OPTION] ...' % sys.argv[0]
    print '  -b size MB      generate size MB data'
    print '  -c conurrent'
    print '  -d directory'
    print '  -s file min size, default is 10MB'
    EXIT(0)

def main():
    totalsize = 0
    con = 0
    rootpath = ''
    start_size = 10 # MB

    if len(sys.argv) == 1:
        help()

    optlist, args = getopt.getopt(sys.argv[1:], 'b:c:d:s:h')

    for opt, arg in optlist:
        print opt, arg
        if opt == '-b':
            totalsize = int(arg)
        elif opt == '-c':
            con = int(arg)
        elif opt == '-d':
            rootpath = arg
        elif opt == '-s':
            start_size = int(arg)
        elif opt == '-h':
            help()
        else:
            help()

    sizelist = xrange(start_size, con + start_size)

    num = totalsize / con * sum(sizelist)

    if not os.path.exists(rootpath):
        os.mkdir(rootpath)

    workers = []
    for i in sizelist:
        tmp = gen_data(i * 1024 * 1024, num, rootpath)
        workers.append(tmp)
        tmp.start()

    for i in workers:
        i.join()

if __name__ == '__main__':
    main()
