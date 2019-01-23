#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from optparse import OptionParser

def main(file, offset, length):

    f = open('%s' % file)
    f.seek(offset)
    content = f.read(length)
    sys.stdout.write ("%s" % content) 
    f.seek(0)
    f.close()



if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-f", "--file",    dest="file",     type='string', help="file to open")
    parser.add_option("-o", "--offset",  dest="offset", type='int', help="offset position",   default="0")
    parser.add_option("-l", "--length",  action="store", dest="length", type='int', help="read length", )
    (options, args) = parser.parse_args()
    try:
        main(options.file, options.offset, options.length)
    except:
        print "Usage: ocat.py -f file -o offset -l length"
