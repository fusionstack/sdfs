#!/usr/bin/env python2.7
# -*- coding:utf-8 -*-

import argparse
import json
import os
import sys
from config import Config
from utils import dwarn, dmsg

USER_LEN = 32
PASS_LEN = 128
PATH_LEN = 4*1024
PERMISSION_LEN = 512

class Ftp(object):
    def __init__(self, config=None):
        self.config = config
        if self.config is None:
            self.config = Config()
        self.user_list = []
        self.ftp_conf = self.config.ftp_conf

    def _read_json(self, filename):
        with open(filename, 'r') as fp:
            contents = json.load(fp)
            return contents

    def _write_json(self, filename, user_list):
        with open(filename, 'w') as fp:
            json.dump(user_list, fp, indent=4)

    def get_user_list(self, filename):
        if os.path.exists(filename):
            self.user_list = self._read_json(filename)
            return True
        else:
            return False

    def _format(self, user_record):
        print "user:%s  password:%s  path:%s  permision:%s" % (user_record['user'],
                        user_record['password'], user_record['path'],
                        user_record['permision'])

    def list_user(self):
        if self.get_user_list(self.ftp_conf):
            if len(self.user_list): #//ftp.conf中已有用户配置
                map(self._format, self.user_list)
            else:
                dwarn('empty record')
            return True
        else:
            return False

    def del_user(self, args):
        found = False
        if not self.get_user_list(self.ftp_conf):
            return False

        if len(self.user_list) == 0:
            dwarn('empty record')
            return False

        for record in self.user_list:
            if record['user'] == args.user:
                found = True
                self.user_list.remove(record)
                break

        if found:
            self._write_json(self.ftp_conf, self.user_list)
            dmsg('operation sucessfully')
        else:
            dwarn('given user is not exist')

        return True


    def is_valid_arg(self, args):
        retval = True
        if not args:
            return False
        if len(args.user) >= USER_LEN:
            dwarn('username is too long')
            retval = False
        elif len(args.password) >= PASS_LEN:
            dwarn('password is too long')
            retval = False
        elif len(args.path) >= PATH_LEN:
            dwarn('path is too long')
            retval = False
        elif len(args.permision) >= PERMISSION_LEN:
            dwarn('permision is too long')
            retval = False
        elif args.permision != 'rw' and args.permision != 'RW' and args.permision != 'ro' and args.permision != 'RO':
            dwarn('unsupported permision value')
            retval = False
        return retval

    def add_user(self, args):
        found = False
        if not self.is_valid_arg(args):
            return False
        if not self.get_user_list(self.ftp_conf):
            return False
        for record in self.user_list:
            if record['user'] == args.user:
                found = True
                break

        if not found:
            self.user_list.append({'user':args.user, 'password':args.password,
                    'path':args.path, 'permision':args.permision})
            self._write_json(self.ftp_conf, self.user_list)
            dmsg('operation sucessfully')
        else:
            dwarn('given user is already exist')

        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparser = parser.add_subparsers()

    def _list(args, ftp):
        ftp.list_user()
    parser_list = subparser.add_parser('list', help='list all ftp users')
    parser_list.set_defaults(func=_list)

    def _add(args, ftp):
        ftp.add_user(args)
    parser_add = subparser.add_parser('add', help='add a ftp user')
    parser_add.add_argument('-u', '--user', required=True, help='user name')
    parser_add.add_argument('-p', '--password', required=True, help='password')
    parser_add.add_argument('-d', '--path', required=True, help='path(dir)')
    parser_add.add_argument('-x', '--permision', required=True, help='permision')
    parser_add.set_defaults(func=_add)

    def _del(args, ftp):
        ftp.del_user(args)
    parser_del = subparser.add_parser('delete', help='delete a ftp user')
    parser_del.add_argument('-u', '--user', required=True, help='user name')
    parser_del.set_defaults(func=_del)

    if(len(sys.argv) == 1):
        parser.print_help()
        sys.exit(-1)

    args = parser.parse_args()
    ftp = Ftp()
    args.func(args, ftp)
