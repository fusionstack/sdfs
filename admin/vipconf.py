#!/usr/bin/env python2.7
# -*- coding:utf-8 -*-

import argparse
import json
import os
import sys
from config import Config
from utils import dwarn, dmsg, json_store, json_load


class Vipconf(object):
    def __init__(self, config=None):
        self.config = config
        if self.config is None:
            self.config = Config()
        self.json_file = self.config.vip_conf


    def list(self):
        if os.path.exists(self.json_file):
            data = json_load(self.json_file)

        if len(data) != 0:
            group_list = [(k, data[k]) for k in sorted(data.keys())] 

        for (k, v) in group_list:
            print ("%s:" % k)
            for (key, val) in v.items():
                print ("\t%s:%s" % (key,val))
                

    def add(self, args):
        #check args
        update = 1
        count = 0
        data = {}
        group_list = []

        if os.path.exists(self.json_file):
            data = json_load(self.json_file)

        if len(data) != 0:
            group_list = [(k, data[k]) for k in sorted(data.keys())] 
            gname, v = group_list[len(group_list) - 1]
            count = filter(str.isdigit, json.dumps(gname))

        if args:
            gname = "group" + str(int(count)+1) 
            data[gname] = {}
            group = data[gname]

            #generate vipconf json
            group['nodes'] = args.host
            group['type'] = args.type
            group['vip'] = args.vip
        else:
            update = 0

        #data = sorted(data.items(), key=lambda d: d[0])
        #modify vipconf about vip or nodes

        if update:
            json_store(data, self.config.vip_conf)
            dmsg('add vipconf sucessfully')
        else:
            dwarn('vipconf was updated.')


    def delete(self, args):
        print "delete vipconf"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    def _list(args, vipconf):
        vipconf.list()
    parser_list = subparser.add_parser('list', help='list all vipconf')
    parser_list.set_defaults(func=_list)

    def _add(args, vipconf):
        vipconf.add(args)
    parser_add = subparser.add_parser('add', help='add a vipconf')
    parser_add.add_argument('-t', '--type', help='user or dns')
    parser_add.add_argument('-m', '--mask', help='ip mask')
    parser_add.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.100.1')
    parser_add.add_argument('-v', '--vip', help='vip ip eg:192.168.120.1')
    parser_add.set_defaults(func=_add)

    def _del(args, vipconf):
        vipconf.delete(args)
    parser_del = subparser.add_parser('del', help='delete a vipconf')
    parser_del.add_argument('-t', '--type', help='user or dns')
    parser_del.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.100.1')
    parser_del.add_argument('-v', '--vip', help='vip ip eg:192.168.120.1')
    parser_del.set_defaults(func=_del)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(-1)

    args = parser.parse_args()
    vipconf = Vipconf()
    args.func(args, vipconf)
