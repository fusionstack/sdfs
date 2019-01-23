#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json

from utils import Exp, _dwarn, _exec_pipe, _exec_remote, _str2dict


DEFAULT_PROTO = 'iscsi'


class LichBase(object):
    _CMD = None

    def __init__(self, lich_home='/opt/fusionstack/lich', protocol=DEFAULT_PROTO, debug=False):
        self.lich_home = lich_home
        self.protocol = protocol
        self.debug = debug

    def log(self, msg):
        if self.debug:
            print msg

    def _update_args(self, cmd, args, protocol):
        assert cmd is not None
        args.insert(0, os.path.join(self.lich_home, cmd))
        if protocol in ['iscsi', 'nbd', 'lichbd']:
            args.extend(['-p', protocol])

    def _run(self, args, protocol=DEFAULT_PROTO, timeout=0):
        self._update_args(self._CMD, args, protocol)

        self.log(' '.join(args))
        res = _exec_pipe(args, timeout, False)
        self.log(res)
        return res.strip()

    def _remote_run(self, host, args, protocol=DEFAULT_PROTO, timeout=0):
        self._update_args(self._CMD, args, protocol)
        try:
            self.log(host + ': ' + ' '.join(args))
            out, err = _exec_remote(host, ' '.join(args))
            self.log(out)
            return out.strip('\n')
        except Exp, e:
            _dwarn("%s:%s" % (host, e.err))
            return ''


class Lichbd(LichBase):
    _CMD = 'libexec/lichbd'

    def __init__(self, lich_home='/opt/fusionstack/lich', protocol=DEFAULT_PROTO, debug=False):
        super(Lichbd, self).__init__(lich_home, protocol=protocol, debug=debug)

    def run(self, args, protocol=DEFAULT_PROTO):
        return self._run(args, protocol=protocol)

    def list_storagearea(self):
        return self.run(['liststoragearea'], protocol='')

    def list_all_pools(self, protocol=DEFAULT_PROTO):
        return self.run(['pool', 'ls'], protocol=protocol)

    def pool_create(self, pool, protocol=DEFAULT_PROTO):
        return self.run(['pool', 'create', pool], protocol=protocol)

    def pool_rm(self, pool, protocol=DEFAULT_PROTO):
        return self.run(['pool', 'rm', pool], protocol=protocol)

    def pool_list(self, pool, protocol=DEFAULT_PROTO):
        return self.run(['vol', 'ls', pool], protocol=protocol)

    def vol_create(self, vol, size, protocol=DEFAULT_PROTO):
        return self.run(['vol', 'create', vol, '--size', size], protocol=protocol)

    def vol_resize(self, vol, size, protocol=DEFAULT_PROTO):
        return self.run(['vol', 'resize', vol, '--size', size], protocol=protocol)


class LichAdmin(LichBase):
    _CMD = 'libexec/lich.admin'

    def __init__(self, lich_home='/opt/fusionstack/lich', protocol=DEFAULT_PROTO, debug=False):
        super(LichAdmin, self).__init__(lich_home, protocol=protocol, debug=debug)

    def run(self, args, protocol='', timeout=3):
        return self._run(args, protocol=protocol, timeout=timeout)

    def list_nodes(self):
        res = self.run(['--listnode', '-v'], protocol='')
        return _str2dict(res)


class LichNode(LichBase):
    _CMD = 'admin/node.py'

    def __init__(self, lich_home='/opt/fusionstack/lich', protocol=DEFAULT_PROTO, debug=False):
        super(LichNode, self).__init__(lich_home, protocol=protocol, debug=debug)

    def run(self, args, protocol='', timeout=10):
        return self._run(args, protocol=protocol, timeout=timeout)

    def health(self, host, ext=[]):
        """

        :param host:
        :param ext: [], ['scan'], or ['clean']
        :return:
        """
        return self._remote_run(host, ['--health', ' '.join(ext), '--json'], protocol='')

    def recover(self, host):
        return self._remote_run(host, ['--recover'], protocol='')

    def metabalance(self, host):
        return self._remote_run(host, ['--metabalance'], protocol='')

    def chunkbalance(self, host):
        return self._remote_run(host, ['--chunkbalance'], protocol='')


if __name__ == '__main__':
    lichbd = Lichbd(debug=True)
    lichbd.run(['pool', 'ls'])

    lich_admin = LichAdmin(debug=True)
    print lich_admin.list_nodes()

    lich_node = LichNode(debug=True)
    print lich_node.health('v1')
