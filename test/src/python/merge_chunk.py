#!/usr/bin/python
import os
import sys

def is_x86_64():
    return '86_64' in os.popen('uname -a').read()

def merge_chk(from_chk, to_fd):
    try:
        fr = open(from_chk, 'r')
        fr.seek(4160)
    except:
        raise

    while True:
        buf = fr.read(524288)
        if buf:
            to_fd.write(buf)
        else:
            break

    fr.close()

def main():
    if not is_x86_64():
        print 'System is i386, exit'
        exit(0)

    to_fd = open(sys.argv[-1], 'w')
    for i in sys.argv[1:-1]:
            print 'merge chk %s' % i 
            merge_chk(i, to_fd)

    to_fd.close()

if __name__ == '__main__':
    main()
