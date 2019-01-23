#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import signal
import threading;
import random
import time
import hashlib
from optparse import OptionParser

done = False

class WriteThread(threading.Thread):
    def __init__(self, id, dir):
        threading.Thread.__init__(self)
        self.id  = id
        self.dir = dir
    def run(self):
        os.system('mkdir -p %s/%d' % (self.dir, self.id))
        js = open("%s/files_%d" % (self.dir, self.id), 'w')

        while not done:
            file_size = random.randint(16*1024, options.mbytes*1024*1024)
            fname = '%s/%d/testfile_s%d_t%d_%d' % (self.dir, self.id, file_size, int(time.time()), random.randint(1,10000))
            print '%s: writing to %s' % (self.getName(), fname)

            f = open(fname, 'w')
            m = hashlib.md5()

            count = 0
            while count < file_size:
                try:
                    buflen = random.randint(20000, 40000)
                    if file_size - count < buflen:
                        buflen = file_size - count
                    input_str = random.choice('abcdefghijklmnopqrstuvwxyz0123456789') * buflen
                    count += buflen

                    f.write(input_str)
                    m.update(input_str)
                except KeyboardInterrupt, e:
                    break
            f.close()
            open('%s.md5' % fname, 'w').write(m.hexdigest())
            js.write('%s\n' % fname)
            js.flush()
        js.close()

class NfsTestCase:
    def __init__(self, dir):
        print 'begin test in dir %s' % dir
        print '-----------------------------------------------------------'
        self.dir = dir
    def testWrite(self):
        os.system('rm -rf %s/*' % self.dir)

        threads = []
        for i in range(options.threads):
            th = WriteThread(i, self.dir)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()
    def checkMd5__(self, path):
        md5_len = 32
        for line in open(path):
            line = line.strip(' \n')
            print "------------------------------------------%s" % line
            f = open(line)
            m = hashlib.md5()
            while True:
                out = f.read(412528)
                if not out: break
                m.update(out)
            md5 = open('%s.md5' % line).read(md5_len)
            if m.hexdigest() != md5:
                print "raw", md5
                print "now", m.hexdigest()
    def testMd5(self):
        for f in os.listdir(self.dir):
            if os.path.isfile('%s/%s' % (self.dir, f)) and f[:6] == 'files_':
                self.checkMd5__('%s/%s' % (self.dir, f))


def handler(signum, frame):
    print 'Done'
    done = True

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", "--dir",     type="string", dest="dir",     default='/mnt/yfs')
    parser.add_option("-t", "--test",    type="int",    dest="test",    default=0)
    parser.add_option("-c", "--threads", type="int",    dest="threads", default=10)
    parser.add_option("-m", "--mbytes",  type="int",    dest="mbytes",  default=256)
    (options, args) = parser.parse_args()

    signal.signal(signal.SIGABRT, handler)

    testcase = NfsTestCase(options.dir)
    if options.test == 0:
        testcase.testWrite()
    elif options.test == 1:
        testcase.testMd5()
    else:
        print "Error test code: ", options.test
