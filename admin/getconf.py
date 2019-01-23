#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import subprocess
import argparse
import sys

def exec_cmd(cmd):
    r = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)    
    stdout,stderr = r.communicate()
    if int(r.returncode) == 0:
        return stdout    

def replace_org_file(args):
        if not os.path.exists(args.dest):
            raise Exception("%s is not exist" % args.dest)
        if not os.path.exists(args.source):
            raise Exception("%s is not exist" % args.source)
        if os.path.exists('%s.bak' % args.dest):
            exec_cmd("rm -rf %s.bak" % args.dest)
        exec_cmd("cp %s %s.bak" % (args.dest, args.dest))
        exec_cmd("cp %s %s" % (args.source, args.dest))
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        #prog ='genconf',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument('-s', '--source', metavar='<source file>', required=True, help='use source file replace dest file')
    parser.add_argument('-d', '--dest', metavar='<dest file>', required=True, help='replaces by source file')
    if len(sys.argv) == 1 or len(sys.argv) > 5:
       parser.print_help()
       exit(1)
    args = parser.parse_args(sys.argv[1:])
    rtn =  replace_org_file(args)
    if rtn:
        print "gengration %s file success!" % args.dest
