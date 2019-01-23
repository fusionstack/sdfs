#!/usr/bin/env python2
#coding=utf-8
import errno
import socket
import argparse
import sys
import os
import time

from utils import _dmsg, _exec_pipe
from utils import _dwarn
from utils import _derror

from utils import _exec_remote, mutil_exec, _exec_shell1, _exec_pipe1, _sysinfo, _syserror, _syswarn

from utils import Exp, _check_config
from config import Config
from argparse import RawTextHelpFormatter

ETCD_NOT_RUNNING = 0
ETCD_RUN_AS_PROXY = 1
ETCD_RUN_AS_META = 2
ETCD_MIN_MEMBER_COUNT = 3


class Etcd_manage(object):
    def __init__(self):
        self.config = Config()
        pass

    def member_add(self, hosts):
        origin_cluster_list = self._etcd_get_init_cluster().keys()
        new_cluster_list = origin_cluster_list
        add_list = []

        for host in hosts.split(','):
            if host in new_cluster_list:
                _dwarn("host:%s is already in etcd cluster, please check it!" % (host))
                continue
            else:
                # add etcd member one by one
                self._etcd_health_wait()

                self._etcd_add_member(host)
                new_cluster_list.append(host)

                try:
                    cmd = "python %s/app/admin/node.py etcd --state %s --hosts %s" % (self.config.home, "existing", ','.join(new_cluster_list))
                    self._exec_node(host, cmd)
                except Exp, e:
                    self._etcd_del_member(host)
                    _syserror("etcd set conf on host:%s fail, errmsg:%s" % (host, e.err))
                    raise Exp(e.errno, "etcd set conf on host:%s fail, errmsg:%s" % (host, e.err))

        _sysinfo("etcd member add hosts:%s ok !" % (host))
        print "member add hosts:%s ok !" % (hosts)

    def _exec_node(self, h, cmd):
        (out, err, stat) = _exec_remote(h, cmd)
        if (out):
            _dmsg(h + ":\n" + out)
        if (err):
            _dwarn(h + ":\n" + err)

    def _etcd_health_wait(self):
        retry = 0

        while True:
            if self.etcd_is_health():
                break
            else:
                retry = retry + 1
                if retry > 30:
                    raise Exp(errno.EPERM, "etcd cluster unhealth, pelease check it !")

            time.sleep(1)

    def member_del(self, hosts, proxy=False):
        origin_cluster_list = self._etcd_get_init_cluster().keys()
        del_list = hosts.split(',')
        new_cluster_list = origin_cluster_list
        new_del_list = []

        for host in del_list:
            if host not in new_cluster_list:
                _dwarn("host:%s is not in etcd cluster, please check it!" % (host))
                del_list.remove(host)
                continue
            else:
                #etcd member remove host
                new_cluster_list.remove(host)

        for host in del_list:
            if len(host) == 0:
                continue

            _dmsg("wait etcd for health, host:%s" % (host))

            self._etcd_health_wait()
            _dmsg("etcd is health, begin check is permit to remove... host:%s" % (host))

            if not self.etcd_check_member_remove_permition(host):
                _dwarn("host:%s not permit to remove. pelease check it!\n" % (host))
                new_cluster_list.append(host)
                continue

            _dmsg("host:%s permit to remove, begin remove..." % (host))

            try:
                cmd = "etcdctl member list | grep %s | awk '{print $1}' | awk -F':' '{print $1}'| awk -F'[' '{print $1}'| xargs etcdctl member remove" % (host)
                self._exec_node(new_cluster_list[0], cmd)
                _sysinfo("etcd member remove %s ok !" % (host))
                new_del_list.append(host)
            except Exp, e:
                _syserror("etcd member remove %s fail ! errmsg:%s" % (host, e.err))
                raise Exp(errno.EPERM, "etcd member remove %s fail, %s" % (host, e.err))

        if proxy and len(new_del_list) > 0:
            #set hosts run as proxy mode
            cmd = "python %s/app/admin/node.py etcd --state %s --hosts %s" % (self.config.home, "existing", ','.join(new_cluster_list))
            args = [[x, cmd] for x in del_list]
            mutil_exec(self._exec_node, args)
            _sysinfo("etcd member remove %s ok , new_cluster_list:%s!" % (del_list, new_cluster_list))

        print "member del hosts:%s ok !" % (new_del_list)

    def member_list(self):
        _etcd_cluster = self._etcd_get_init_cluster()
        for host in _etcd_cluster.keys():
            if _etcd_cluster[host] == "true":
                print "%s : leader" % (host)
            else:
                print "%s" % (host)

    def create_cluster(self, hosts, proxy_nodes=None):
        cluster_list = hosts.split(',')
        proxy_list = []

        cmd = "python %s/app/admin/node.py etcd --state %s --hosts %s" % (self.config.home, "new", hosts)
        args = [[x, cmd] for x in cluster_list]
        mutil_exec(self._exec_node, args)

        if proxy_nodes is not None:
            proxy_list = proxy_nodes.split(',')
            for node in proxy_list:
                if node in cluster_list:
                    _dwarn("host:%s is in etcd cluster, not permit run as proxy mode!" % (node))
                    proxy_list.remove(node)

            if len(proxy_list):
                args = [[x, cmd] for x in proxy_list]
                mutil_exec(self._exec_node, args)

    @classmethod
    def get_admin(cls):
        """
        :return: app admin host
        """
        cmd = u"etcdctl get /sdfs/mond/master | awk -F',' '{print $1}'"
        (host, err) = _exec_shell1(cmd, p=False)
        if err.strip() != '':
            raise Exp(errno.EPERM, "%s:%s" % (host.strip(), err.strip()))
        return host

    @classmethod
    def etcd_is_health(cls):
        """
        :return:
            return 1: etcd cluster health
            return 0: etcd cluster unhealth
        """
        host = cls.get_admin()
        cmd = u"etcdctl cluster-health"
        #print host.strip()
        (out, err, stat) = _exec_remote(host.strip(), cmd)
        if err.strip() != '':
            raise Exp(errno.EPERM, "%s:%s" % (host.strip(), err.strip()))
        if "cluster is healthy" in out:
            return 1
        else:
            _dwarn("Etcd status is unhealth, please check etcd status")
            return 0

    @classmethod
    def _etcd_get_member_healthy_count(cls, admin):
        cmd = u"etcdctl cluster-health | grep 'is healthy:' | wc -l"
        (out, err, stat) = _exec_remote(admin, cmd)
        if err.strip() != '':
            raise Exp(errno.EPERM, "%s:%s" % (host.strip(), err.strip()))

        return out.strip()

    @classmethod
    def _etcd_member_is_healthy(cls, admin, host):
        cmd = u"etcdctl cluster-health | grep 'name=%s ' | grep 'is healthy:' | wc -l" % (host)
        (out, err, stat) = _exec_remote(admin, cmd)
        if err.strip() != '':
            raise Exp(errno.EPERM, "%s:%s" % (host.strip(), err.strip()))

        if out.strip() == "1":
            return True
        else:
            return False

    @classmethod
    def etcd_check_member_remove_permition(cls, host):
        """
        :return:
              return True : permit to remove
              return False: not permit to remove
        """
        admin_node = cls.get_admin()
        if not cls._etcd_member_is_healthy(admin_node.strip(), host):
            return True

        member_count = cls._etcd_get_member_healthy_count(admin_node.strip())
        if member_count <= ETCD_MIN_MEMBER_COUNT:
            return False
        else:
            return True

    @classmethod
    def etcd_get_role(cls):
        config = Config()
        cmd = u"ps -ef |grep 'etcd'"
        (out, err) = _exec_shell1(cmd, p=False)
        if err.strip() != '':
            raise Exp(errno.EPERM, "%s:%s" % (host.strip(), err.strip()))

        out = out.strip()
        if out == "":
            return ETCD_NOT_RUNNING

        if os.path.isdir(os.path.join(config.etcd_data_path, "proxy")):
            return ETCD_RUN_AS_PROXY
        else:
            return ETCD_RUN_AS_META

    @classmethod
    def etcd_set_config(cls, init_cluster, state, proxy):
        config = Config()
        ip = socket.gethostbyname(config.hostname)

        os.system("echo  > /etc/etcd/etcd.conf")
        _check_config("/etc/etcd/etcd.conf", "ETCD_NAME", "=", '"' + "%s" % (config.hostname) + '"', True)
        _check_config("/etc/etcd/etcd.conf", "ETCD_DATA_DIR", "=", '"' + config.etcd_data_path + '"', True)
        _check_config("/etc/etcd/etcd.conf", "ETCD_ADVERTISE_CLIENT_URLS", "=", '"' + "http://%s:2379" % (config.hostname) + '"', True)
        _check_config("/etc/etcd/etcd.conf", "ETCD_LISTEN_CLIENT_URLS", "=", '"' + "http://%s:2379,http://127.0.0.1:2379" % (ip) + '"', True)
        _check_config("/etc/etcd/etcd.conf", "ETCD_INITIAL_CLUSTER", "=", '"' + "%s" % (init_cluster) + '"', True)
        if proxy:
            # set etcd conf as proxy mode
            _check_config("/etc/etcd/etcd.conf", "ETCD_PROXY", "=", '"' + "on" + '"', True)
        else:
            _check_config("/etc/etcd/etcd.conf", "ETCD_LISTEN_PEER_URLS", "=", '"' + "http://0.0.0.0:2380" + '"', True)
            _check_config("/etc/etcd/etcd.conf", "ETCD_INITIAL_CLUSTER_STATE", "=", '"' + "%s" % (state) + '"', True)
            _check_config("/etc/etcd/etcd.conf", "ETCD_INITIAL_ADVERTISE_PEER_URLS", "=", '"' + "http://%s:2380" % (config.hostname) + '"', True)

    @classmethod
    def _etcd_service_trystart(cls):
        try:
            cmd = "systemctl start etcd"
            _exec_shell1(cmd, p=False)
        except Exp, e:
            pass

    @classmethod
    def _etcd_check_running(cls):
        cmd = "systemctl status etcd | grep 'Active'"
        out, err = _exec_shell1(cmd, p=False)
        if "running" in out:
            return True
        else:
            return False

    @classmethod
    def _etcd_check_removed(cls):
        i = 0

        while i < 10:
            cls._etcd_service_trystart()
            time.sleep(1)
            try:
                cmd = "systemctl status etcd | grep -E 'the member has been permanently removed from the cluster|the data-dir used by this member must be removed'"
                out, err = _exec_shell1(cmd, p=False)
                out = out.strip()
                if len(out) > 0:
                    return True
            except Exp, e:
                pass

            i = i + 1

        return False

    @classmethod
    def _get_members_from_conf(cls):
        member_list = []

        cmd = "cat /etc/etcd/etcd.conf | grep '^ETCD_INITIAL_CLUSTER[ =]' | awk -F'\"' '{print $2}'"
        out, err = _exec_shell1(cmd, p=True)
        if len(out.strip()) == 0:
            raise Exp(errno.EPERM, "not find init cluster, please check it !")

        for member in out.strip().split(','):
            if len(member) == 0:
                continue

            host = member.split('=')[0]
            member_list.append(host)

        return member_list

    @classmethod
    def _etcd_try_start_as_proxy(cls):
        is_running = False

        member_list = cls._get_members_from_conf()
        if len(member_list) == 0:
            raise Exp(errno.EPERM, "etcd service start fail, not find init cluster!")

        for host in member_list:
            meta_list = []
            meta_list.append(host)
            cls.etcd_set(meta_list, "existing", proxy=True)
            if cls._etcd_check_running():
                is_running = True
                break
            else:
                continue

        if is_running:
            _dmsg("etcd run as proxy ok !")
        else:
            _syserror("start as proxy mode fail .")
            raise Exp(errno.EPERM, "etcd try to run as proxy fail, please check it !")

    @classmethod
    def etcd_service_start_force(cls):
        cls._etcd_service_trystart()

        if cls._etcd_check_running():
            _sysinfo("etcd service start ok!")
            return

        if cls._etcd_check_removed():
            #try to run as proxy mode
            _syswarn("etcd was removed, try start as proxy mode !")
            cls._etcd_try_start_as_proxy()
            _syswarn("etcd was removed, start as proxy mode ok !")
        else:
            raise Exp(errno.EPERM, "etcd service start fail, please check it!")

    @classmethod
    def _etcd_get_init_cluster(cls):
        init_cluster_list = {}

        #cmd = "etcdctl member list | awk '{print $2}'| awk -F'=' '{print $2}'"
        cmd = """etcdctl member list | awk '{print $2" "$5}'"""
        out, err = _exec_shell1(cmd, p=False)
        for line in out.strip().split('\n'):
            if len(line) == 0:
                continue

            hostname = line.split(' ')[0].split('=')[1]
            if "isLeader" in line:
                is_leader = line.split(' ')[1].split('=')[1]

            init_cluster_list[hostname] = is_leader

        #print init_cluster_list
        return init_cluster_list

    @classmethod
    def _etcd_add_member(cls, host):
        cmd = "etcdctl member add %s http://%s:2380" % (host, host)
        _exec_shell1(cmd, p=True)

    @classmethod
    def _get_uuid(cls, host):
        cmd = "etcdctl member list | grep 'name=%s ' | awk '{print $1}'" % (host)
        out, err = _exec_shell1(cmd, p=False)
        return out.strip()[:-1]

    @classmethod
    def _etcd_del_member(cls, host):
        member_uuid = cls._get_uuid(host)
        if member_uuid == "":
            raise Exp(errno.ENOENT, "get host:%s uuid fail" % (host))

        cmd = "etcdctl member remove %s " % (member_uuid)
        _exec_shell1(cmd, p=True)

    def etcd_member_update(self, new_meta_list):
        add_list = []
        del_list = []

        add_list, del_list = self.etcd_meta_list_get_changed(new_meta_list)
        if len(add_list):
            self.member_add(','.join(add_list))
        if len(del_list):
            self.member_del(','.join(del_list), proxy=True)

    @classmethod
    def etcd_meta_list_is_changed(cls, new_meta_list):
        old_meta_list = []

        old_meta_list = cls._etcd_get_init_cluster().keys()
        del_list = set(old_meta_list) - set(new_meta_list)
        add_list = set(new_meta_list) - set(old_meta_list)

        #print (new_meta_list, old_meta_list)
        if len(del_list) or len(add_list):
            return True

        return False

    @classmethod
    def etcd_meta_list_get_changed(cls, new_meta_list):
        old_meta_list = []

        old_meta_list = cls._etcd_get_init_cluster().keys()
        del_list = set(old_meta_list) - set(new_meta_list)
        add_list = set(new_meta_list) - set(old_meta_list)

        return add_list, del_list

    @classmethod
    def etcd_role_is_changed(cls, proxy):
        role = cls.etcd_get_role()
        if role == ETCD_NOT_RUNNING:
            return True

        if (role == ETCD_RUN_AS_PROXY and proxy) or (role == ETCD_RUN_AS_META and not proxy):
            return False

        return True

    @classmethod
    def etcd_set(cls, meta_list, state, proxy=False):
        cluster = ""
        config = Config()

        if not cls.etcd_role_is_changed(proxy) and not cls.etcd_meta_list_is_changed(meta_list):
            _dwarn("etcd cluster not change, no need set!")
            return

        for i in meta_list:
            cluster = cluster + "%s=http://%s:2380," % (i, i)

        cls.etcd_set_config(cluster[:-1], state, proxy)

        if not os.path.isdir(config.etcd_data_path):
            os.system("mkdir -p %s" % (config.etcd_data_path))
            os.system("chown etcd.etcd %s" % (config.etcd_data_path))
        else:
            os.system("systemctl stop etcd")
            os.system("rm -rf %s/*" % (config.etcd_data_path))

        #if cls.etcd_get_role() == ETCD_RUN_AS_PROXY and not proxy:
        #    cmd = "systemctl stop etcd && rm -rf %s/proxy" % (config.etcd_data_path)
         #   _exec_shell1(cmd, p=False)

        s = _exec_pipe1(["systemctl", 'restart', 'etcd'], 1, False)

def main():
    etcd_manage = Etcd_manage()


if __name__ == "__main__":
    main()

    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    subparsers = parser.add_subparsers()

    def _member_add(args):
        etcd_manage = Etcd_manage()
        etcd_manage.member_add(args.hosts)
    parser_member_add = subparsers.add_parser('member_add', help='add a member into etcd cluster')
    parser_member_add.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_member_add.set_defaults(func=_member_add)

    def _member_del(args):
        etcd_manage = Etcd_manage()
        etcd_manage.member_del(args.hosts, args.proxy)
    parser_member_del = subparsers.add_parser('member_del', help='remove a member from etcd cluster')
    parser_member_del.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_member_del.add_argument("--proxy", action='store_true', help="node run as proxy mode")
    parser_member_del.set_defaults(func=_member_del)

    def _member_list(args):
        etcd_manage = Etcd_manage()
        etcd_manage.member_list()
    parser_member_list = subparsers.add_parser('member_list', help='list etcd cluster members')
    parser_member_list.set_defaults(func=_member_list)

    def _create_cluster(args):
        etcd_manage = Etcd_manage()
        etcd_manage.create_cluster(args.hosts, args.proxy_nodes)
    parser_create_cluster = subparsers.add_parser('create_cluster', help='create an etcd cluster')
    parser_create_cluster.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_create_cluster.add_argument("--proxy_nodes", default=None, help="host3,host4,...")
    parser_create_cluster.set_defaults(func=_create_cluster)

    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
