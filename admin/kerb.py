#!/usr/bin/env python
# -*- coding:utf-8 -*-
import subprocess
import sys
import os
import argparse

content = '''
# Configuration snippets may be placed in this directory as well
includedir /etc/krb5.conf.d/

[logging]
default = FILE:/var/log/krb5libs.log
kdc = FILE:/var/log/krb5kdc.log
admin_server = FILE:/var/log/kadmind.log
[libdefaults]
dns_lookup_realm = false
dns_lookup_kdc = true
ticket_lifetime = 24h
renew_lifetime = 7d
forwardable = true 
#rdns = false 
default_realm = %(upper_domain)s
[realms]
%(upper_domain)s = {
  kdc = %(lower_domain)s:88
  admin_server = %(lower_domain)s:749
  default_domain = %(upper_domain)s
}
[domain_realm]
 .%(lower_domain)s = %(upper_domain)s
  %(lower_domain)s = %(upper_domain)s
[kdc]
 profile = /var/kerberos/krb5kdc/kdc.conf
[appdefaults]
 pam = {
    debug = false
    tick_lifetime = 36000
    renew_lifetime = 36000
    forwardable = true
    drb4_convert = false
}
'''

def exec_cmd(cmd):
    r = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout,stderr = r.communicate()
    if int(r.returncode) == 0:
        return stdout,False
    else:
        return stderr,True
def get_domain():
    cmd = "uss.attr -g ad /system"
    rtn,err = exec_cmd(cmd)
    if not err:
        domain = rtn.split(';')[1]
        return domain
    raise Exception(rtn)

def replace_org_file(args):
        if not os.path.exists(args.dest):
            raise Exception("%s is not exist" % args.dest)
        if not os.path.exists(args.source):
            raise Exception("%s is not exist" % args.source)
        if not os.path.exists('%s.bak' % args.dest):
            exec_cmd("cp %s %s.bak" % (args.dest, args.dest))
            #exec_cmd("rm -rf %s.bak" % args.dest)
        exec_cmd("cp %s %s" % (args.source, args.dest))
        return True

def genconf(content=None, filename=None):
    if os.path.exists(filename):
        raise Exception("the file %s already exist!" % filename)
    domain = get_domain()
    content = content %{'upper_domain':domain.upper(),'lower_domain':domain}
    with open(filename,'w') as f:
        f.write(content)

def main():
    #fun_dic = {'genconf':kerb.genconf, 'rfile':kerb.rfile}
    parser = argparse.ArgumentParser(
        #prog ='genconf',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    subparsers = parser.add_subparsers(dest='subcommand',metavar='<subcommand>')
    genconf_parser = subparsers.add_parser('genconf', help='general krbd5 config file')
    genconf_parser.add_argument('-f', '--file', metavar='<reconf file>', required=True, help='reconf krbd5 configure file')
    replace_parser = subparsers.add_parser('replfile', help='replace krbd5 config file')
    replace_parser.add_argument('-s', '--source', metavar='<source file>', required=True, help='use source file replace dest file')
    replace_parser.add_argument('-d', '--dest', metavar='<dest file>', required=True, help='replaces by source file')
    if len(sys.argv) == 1:
       parser.print_help()
       exit(1)
    args = parser.parse_args(sys.argv[1:])
    if len(sys.argv) >1 and len(sys.argv) == 4:
        genconf(content, args.file)
    if len(sys.argv) > 1 and len(sys.argv) == 6:
        replace_org_file(args)

    #rtn = replace_org_file(args)
    #if rtn:
    #    print "gengration %s file success!" % args.dest
if __name__ == "__main__":
    main()
