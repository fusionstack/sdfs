#!/usr/bin/python

import sys
sys.path.append('../libyfs')
import yfs

def test_node():
    yfs.mkdir('test', 777)
    print 'mkdir test'

    yfs.rmdir('test')
    print 'rmdir test'

def main():
    ret = yfs.init()
    print ret
    if ret != 0:
        print 'init failed'
        EXIT(1)

    print 'yfs init'
    test_node()
    yfs.destroy()
    print 'yfs_destroy'


if __name__ == '__main__':
    main()
