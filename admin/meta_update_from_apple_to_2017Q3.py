#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import os
import sys
import string

from config import Config
from utils import mutil_exec, Exp, derror, dwarn, dmsg, exec_remote
from color_output import darkyellow


def get_hosts():
    host_list = []
    conf = Config()
    filename = "%s/etc/cluster.conf" % (conf.home)

    if(os.path.isfile(filename) is False):
        dwarn("%s not found" % (filename))
        return host_list

    #192.168.120.84 |cds.0|cds.1|cds.2
    #192.168.251.245 mds.0|cds.0|cds.1
    with open(filename, 'r') as fp:
        for line in fp.readlines():
            host = line.split(' ')[0]
            host_list.append((host, 'ftp', ''))
            roles = line.split(' ')[1].strip().split('|')
            for role in roles:
                if role == "":
                    continue
                r,num = role.split('.')
                host_list.append((host, r, num))
    return host_list


# all host in cluster should be updated
def meta_update(version, hosts=None):
    def _update_version(k):
        update_cmd = "echo %s > /opt/sdfs/%s/%s/status/version " % (version, k[1], k[2])
        try:
            dmsg('update meta %s %s.%s version to %s' % (k[0], k[1], k[2], version))
            exec_remote(k[0], update_cmd)
        except Exp, e:
            derror("%s : %s" % (k, e))

    if hosts != []:
        args = [[k] for (k) in hosts]
        mutil_exec(_update_version, args)


if __name__ == "__main__":
    hosts = []
    version = "meta \(2017Q3\)"

    try:
        verify = raw_input(darkyellow("Are you sure to update version from apple to %s? (y/n) " % (version))).lower()
        if verify == 'y':
            hosts = get_hosts()
            meta_update(version, hosts)
        else:
            print "not update version."
            sys.exit(1)
    except KeyboardInterrupt:
        print
        sys.exit()
