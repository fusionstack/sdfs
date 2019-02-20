#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import string
import uuid
import json

from config import Config
from cluster_conf import ClusterConf
from instence import Instence
from utils import mutil_exec, check_crontab, unset_crontab, Exp, \
                  exec_shell, check_sysctl, lock_file, derror, dwarn,\
                  human_readable, dev_mountpoints, dev_lsblks, \
                  dev_mkfs_ext4, dev_uuid, dev_childs, fstab_del_mount, \
                  dev_clean, ssh_set_nopassword, put_remote, dmsg, \
                  exec_remote, lsb_release, check_ip_valid, mutil_exec_futures, \
                  get_mask_by_addr
from vip import vip_loadconf

class Cluster(object):
    def __init__(self, config=None):
        self.config = config
        if self.config is None:
            self.config = Config()

    def _get_src(self, op):
        src_tar = None
        if op not in ["etc", "app", "samba"]:
            raise Exp(errno.EINVAL, "not support %s" %(op))

        if op == "samba":
            src_file = "/usr/local/samba"
        else:
            src_file = os.path.join(self.config.home, op)

        src_tar = "/tmp/uss_%s.tar.gz" % (op)

        cmd = "rm -rf %s" % (src_tar)
        cmd2 = "cd %s" % (src_file)
        cmd3 = "tar czf %s *" % (src_tar)
        if op == "samba":
            cmd3 = cmd3 + " --exclude=./private/* --exclude=./var/* >/dev/null 2>&1"
        else:
            cmd3 = cmd3

        cmd = " && ".join([cmd, cmd2, cmd3])
        exec_shell(cmd)
        return src_tar

    def sshkey(self, hosts, password):
        print 'sshkey', hosts
        ssh_set_nopassword(hosts, password)

    def _update(self, src, hosts=None):
        now = time.strftime('%H:%M:%S')

        src_tar = self._get_src(src)
        if src == "samba":
            dist = "/usr/local/samba"
        else:
            dist = os.path.join(self.config.home, src)
        backup_dir = os.path.join(self.config.home, "backup")
        backup = os.path.join(backup_dir, "%s-%s.tar.gz" % (src, now))

        cmd_backup = "mkdir -p %s" % (backup_dir)
        cmd_backup = cmd_backup + " && " + \
                "mkdir -p %s && cd %s && tar czf %s *" % (dist, dist, backup)

        tmp_update = '/tmp/%s-tmp-%s.tar.gz' % (src, now)
        cmd_update = "mkdir -p %s && tar xf %s -C %s > /dev/null && rm %s " % (dist, tmp_update, dist, tmp_update)

        def _put_remote_warp(k):
            # backup and update
            try:
                dmsg('backup %s to %s' % (dist, backup))
                exec_remote(k, cmd_backup)
            except Exp, e:
                derror("%s : %s" % (k, e))

            try:
                dmsg('update %s to %s' % (src, k))
                put_remote(k, src_tar, tmp_update)
                exec_remote(k, cmd_update)
            except Exp, e:
                derror("%s : %s" % (k, e))

        args = [[k] for (k) in hosts]
        mutil_exec(_put_remote_warp, args)

    def _update_nfs(self, nfs_tar, force, hosts=None):
        nfs_temp = "/tmp/nfs"
        nfs_etc = "/etc/ganesha"
        ntirpc_inc = "/usr/include/ntirpc"
        common_lib = "/usr/lib64"
        fsal_lib = "/usr/lib64/ganesha"

        if os.path.exists(nfs_tar) == False:
            dwarn("nfs package is not found")
            sys.exit(-2)

        nfs_pack = os.path.basename(nfs_tar)

        for host in hosts:
            # 首先创建临时目录
            _exec_mkdir_nfs_temp = "mkdir -p %s" % (nfs_temp)
            exec_remote(host, _exec_mkdir_nfs_temp)

            # 把安装包分发到所有节点
            dmsg('update %s to %s' % (nfs_tar, host))
            remote_path = os.path.join(nfs_temp, nfs_pack)
            put_remote(host, nfs_tar, remote_path)

        for host in hosts:
            _exec_all_cmd = ""
            # 创建etc目录
            _exec_mkdir_nfs_etc = "mkdir -p %s" % (nfs_etc)
            _exec_all_cmd = _exec_mkdir_nfs_etc + " && "

            # 解压安装包到临时目录
            _exec_tar_zx = "tar zxvf %s/%s -C %s > /dev/null" % (nfs_temp, nfs_pack, nfs_temp)
            _exec_all_cmd = _exec_all_cmd + _exec_tar_zx + " && "

            # 安装nfs-bin
            _exec_install_bin = "cp -f %s/ganesha.nfsd /usr/bin" % (nfs_temp)
            _exec_all_cmd = _exec_all_cmd + _exec_install_bin + " && "

            # 安装依赖库
            _exec_install_lib = "mkdir -p %s && tar zxvf %s/ntirpc-include.tar.gz -C %s > /dev/null" % (ntirpc_inc, nfs_temp, ntirpc_inc)
            _exec_all_cmd = _exec_all_cmd + _exec_install_lib + " && "

            _exec_install_lib = "tar zxvf %s/ntirpc-lib.tar.gz -C %s > /dev/null" % (nfs_temp, common_lib)
            _exec_all_cmd = _exec_all_cmd + _exec_install_lib + " && "

            _exec_install_lib ="mkdir -p %s && tar zxvf %s/fsal-lib.tar.gz -C %s > /dev/null" % (fsal_lib, nfs_temp, fsal_lib)
            _exec_all_cmd = _exec_all_cmd + _exec_install_lib + " && "

            if force:
                # 安装etc-conf
                _exec_install_nfs_conf = "cp -f %s/common.conf %s/ganesha.conf %s" % (nfs_temp, nfs_temp, nfs_etc)
                _exec_all_cmd = _exec_all_cmd + _exec_install_nfs_conf + " && "

                # 安装dbus-conf
                _exec_install_dbus_conf = "cp -f %s/org.ganesha.nfsd.conf /etc/dbus-1/system.d/" % (nfs_temp)
                _exec_all_cmd = _exec_all_cmd + _exec_install_dbus_conf + " && "

                # 安装start-conf
                _exec_install_start_conf = "cp -f %s/*.service /usr/lib/systemd/system" % (nfs_temp)
                _exec_install_start_conf = _exec_install_start_conf + " && " + "cp -f %s/nfs-ganesha /etc/sysconfig" % (nfs_temp)
                _exec_install_start_conf = _exec_install_start_conf + " && " + "mkdir -p /usr/libexec/ganesha/"
                _exec_install_start_conf = _exec_install_start_conf + " && " + "cp -f %s/nfs-ganesha-config.sh /usr/libexec/ganesha" % (nfs_temp)
                _exec_all_cmd = _exec_all_cmd + _exec_install_start_conf + " && "

                # to load *.service file
                #reload dameon
                _exec_reload = "systemctl daemon-reload"
                _exec_all_cmd = _exec_all_cmd + _exec_reload + ' && '

                """
                # to load dbus config file
                #start message-bus
                _exec_dbus = "systemctl restart messagebus"
                _exec_all_cmd = _exec_all_cmd + _exec_dbus + ' && '

                # restart systemd-logind
                _exec_login = "systemctl restart systemd-logind"
                _exec_all_cmd = _exec_all_cmd + _exec_login + ' && '
                """

            _exec_rmdir = "rm -rf %s/*" % (nfs_temp)
            _exec_all_cmd = _exec_all_cmd + _exec_rmdir
            dmsg("install %s to %s" % (nfs_tar, host))
            exec_remote(host, _exec_all_cmd)

    def _update_tar(self, tar, hosts=None):
        src_tar = tar
        if 'app' in src_tar:
            relative_path = 'app'
        elif 'etc' in src_tar:
            relative_path = 'etc'
        elif 'samba' in src_tar:
            relative_path = 'samba'
        else:
            print 'this package is not supported'
            return

        if relative_path == 'samba':
            abs_path = "/usr/local/samba"
            if not os.path.exists(abs_path):
                os.makedirs(abs_path)
        else:
            abs_path = os.path.join(self.config.home, relative_path)

        file_name = os.path.split(src_tar)[1]
        remote_tmp_path = os.path.join('/tmp', file_name)
        cmd_update = "mkdir -p %s && tar xf %s -C %s > /dev/null && rm %s " % (abs_path, remote_tmp_path, abs_path, remote_tmp_path)

        def _put_remote_warp(k):
            try:
                dmsg('update %s to %s' % (src_tar, k))
                put_remote(k, src_tar, remote_tmp_path)
                dmsg('install %s to %s' % (src_tar, k))
                exec_remote(k, cmd_update)
            except Exp, e:
                derror("%s : %s" % (k, e))

        args = [[k] for (k) in hosts]
        mutil_exec(_put_remote_warp, args)

    def update(self, src=None, tar=None, nfs=None, force=False, hosts=None):
        if hosts is None:
            hosts = self.hosts()

        #print (src, tar, nfs, force, hosts)
        if tar is not None:
            self._update_tar(tar, hosts)
        elif src is not None:
            self._update(src, hosts)
        elif nfs is not None:
            self._update_nfs(nfs, force, hosts)
        else:
            self._update("etc", hosts)
            self._update("app", hosts)

    def deploy(self, src, dist, hosts):
        for h in hosts:
            put_remote(h, src, dist)

    def _add_check_env(self, hosts):
        (distro, release, codename) = lsb_release()

        cmd = '''
        if [ -e '$home_t/$role_t' ]; then
            has_child=0

            for disk in `ls $home_t/$role_t`; do
                if [ $disk == '.' -o $disk == '..' ]; then
                    continue
                fi

                is_num=`expr match $disk "[0-9]*$"`
                if [ ${is_num} -gt 0 ]; then
                    has_child=1

                    if [ "$role_t" == "cds" ]; then
                        if ! mount | grep $home_t/$role_t/$disk 1>/dev/null && ! [ -e $home_t/$role_t/$disk/fake ]; then
                            echo "$home_t/$role_t/$disk/ not mount" 1>&2
                            exit 1
                        fi
                    fi

                    for disk_child in `ls $home_t/$role_t/$disk`; do
                        if [ $disk_child == '.' -o $disk_child == '..' -o $disk_child == 'core' -o $disk_child == 'log' -o $disk_child == 'lost+found' -o $disk_child == 'fake' -o $disk_child == 'tier' ]; then
                            continue
                        else
                            echo "$home_t/$role_t/$disk/ has data!" 1>&2
                            exit 1
                        fi
                    done
                elif [ "$role_t" == "cds" -o "$role_t" == "mond" ]; then
                    echo "$home_t/$role_t/$disk not a number!" 1>&2
                    exit 1
                else
                    if [ $disk == "status" ]; then
                        for disk_child in `ls $home_t/$role_t/$disk`; do
                            if [ $disk_child == '.' -o $disk_child == '..' -o $disk_child == 'lost+found' -o $disk_child == 'fake' ]; then
                                continue
                            else
                                echo "$home_t/$role_t/$disk/ has data!" 1>&2
                                exit 1
                            fi
                        done
                    else
                        echo "$home_t/$role_t/ has data!" 1>&2
                        exit 1
                    fi
                fi
            done
        else
            if [ $role_t == "nfs" -o $role_t == "ftp" ]; then
                mkdir -p $home_t/$role_t
            fi
        fi

        for f in `ls /dev/shm/sdfs/`; do
            if [ $f == '.' -o $f == '..' -o $f == 'lost+found' -o $f == 'nodeid']; then
                continue
            else
                echo "dev/shm/sdfs/ has data!" 1>&2
                exit 1
            fi
        done

        release=`python -c "import platform;print platform.dist()[0]"`
        if [ ${release} != '$distro_t' ]; then
            echo "can not add ${release} node, this system is $release_t" 1>&2
            exit 1
        fi

        if ps -ef | awk '{print $8}' | grep '$home_t/app/sbin/uss_$role_t' 1>/dev/null; then
            echo "uss process already running" 1>&2
            exit 1
        fi

        compare=$now_t
        datetime=`date +%%s`
        if [ $datetime -gt $((compare+60)) -o $datetime -lt $((compare-60)) ]; then
            echo "time is not synchronized" 1>&2
            exit 1
        fi
        '''

        for host in hosts:
            for role in self.config.roles:
                myvars = {"home_t": self.config.workdir, "role_t": role,
                    "now_t": int(time.time()), "iscsi_port_t": 3260,
                    "distro_t": distro}
                cmd2 = string.Template(cmd)
                cmd3 = cmd2.safe_substitute(myvars)
                exec_remote(host, cmd3)

    def _get_services(self, host):
        #return services = {"cds": [], "mond": []}
        services = {}
        for role in self.config.roles:
            path = os.path.join(self.config.workdir, role)
            cmd = "mkdir -p %s;ls %s" % (path, path)
            stdout, _ = exec_remote(host, cmd)
            if (stdout != ""):
                services.update({role: stdout.split()})

        return services

    def _add_check_cluster_conf(self, cluster):
        mond = 0
        for h in cluster.keys():
            mond = mond + len(cluster[h]["mond"])
        if mond == 0:
            raise Exp(1, "not found mond %s" % (cluster))

        for h in cluster.keys():
            if (len(cluster[h]['mond']) + len(cluster[h]["cds"])) == 0:
                raise Exp(1, "not found service in %s,\n %s" % (h, cluster))

    def _add_update_cluster_conf(self, hosts):
        for h in hosts:
            if not self.config.cluster.has_key(h):
                services = self._get_services(h)
                if not services.has_key("mond"):
                    services["mond"] = [0]
                
                self.config.cluster.update({h: services})

        #self._add_check_cluster_conf(cluster)
        #self.config.cluster = cluster
        dumpto = "/tmp/uss_cluster.conf"
        self.config.dump_cluster(dumpto=dumpto)
        self.deploy(dumpto, self.config.cluster_conf, hosts)

    def _drop_update_cluster_conf(self, hosts):
        for h in hosts:
            del self.config.cluster[h]

        self._add_check_cluster_conf(self.config.cluster)
        dumpto = "/tmp/uss_cluster.conf"
        self.config.dump_cluster(dumpto=dumpto)
        self.deploy(dumpto, self.config.cluster_conf, hosts)

    def start(self):
        def _warp(h):
            cmd = "python2 %s start" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "start host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

        cmd = "python %s/app/admin/check_domain.py check" % (self.config.home)
        exec_shell(cmd)

    def stop(self):

        def _cluster_stop(module_py, host):
            cmd = "python2 %s stop" % (module_py)
            x, y = exec_remote(host, cmd)
            print "stop host: %s \n%s" % (host, x)
            if y:
                print y

        #stop all node minio service first
        #args = [[self.config.uss_minio, x] for x in self.config.cluster.keys()]
        #mutil_exec(_cluster_stop, args)

        args = [[self.config.uss_node, x] for x in self.config.cluster.keys()]
        mutil_exec(_cluster_stop, args)

    def restart(self):
        def _warp(h):
            cmd = "python2 %s restart" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "restart host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

        cmd = "python %s/app/admin/check_domain.py check" % (self.config.home)
        exec_shell(cmd)
        
    def stat(self):
        def _warp(h):
            cmd = "python2 %s stat" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "stat host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def _init_root(self):
        cmd = "%s init_root" % (self.config.uss_admin)
        exec_shell(cmd)

    def _init_env(self, hosts):
        def _init_env_warp(h):
            cmd = "python2 %s env_init" % (self.config.uss_node)
            (out, err) = exec_remote(h, cmd)
            print out
            print err
        mutil_exec(_init_env_warp, [[h] for h in hosts])

    def _init_mond(self, hosts):
        def _init_warp(h, index):
            cmd = "python2 %s mond_init --service %s" % (
                        self.config.uss_node, index)
            exec_remote(h, cmd)

        init_args = []
        for h in hosts:
            for i in self.config.cluster[h]['mond']:
                print 'init mond', h, i
                _init_warp(h, i)
                #init_args.append([h, i])

        #mutil_exec(_init_warp, init_args)

        def _start_warp(h):
            cmd = "python2 %s start --op simple" % (self.config.uss_node)
            exec_remote(h, cmd)
        mutil_exec(_start_warp, [[h] for h in hosts])

    def _start_service(self, hosts):
        def _start_srv_warp(h):
            cmd = "python2 %s startsrv" % (self.config.uss_node)
            dmsg(cmd)
            exec_remote(h, cmd)

        mutil_exec(_start_srv_warp, [[h] for h in hosts])

    def _init_system(self):
        cmd = "sdfs.mkdir /system"
        exec_shell(cmd)
        '''
        移动集采 暂时关闭
        cmd = "sdfs.mkdir /nfs_minio"
        exec_shell(cmd)
        '''

        #test nfs-ganesha
        if self.config.testing:
            cmd = "sdfs.mkdir /nfs-ganesha"
            exec_shell(cmd)

        '''
        移动集采 暂时关闭
        cmd = "sdfs.mkdir /small -g leveldb"
        exec_shell(cmd)
        '''

        derror("worm disabled")
        #cmd = "sdfs.worm --init"
        #exec_shell(cmd)

    def _init_secret_key(self):
        uuid_str = str(uuid.uuid1())
        cmd = "%s -s secret_key -V %s /system" % (self.config.uss_attr, uuid_str)
        exec_shell(cmd)

    def _init_create_time(self):
        #todo
        now = long(time.time())
        cmd = "sdfs.attr -s create_time -V %ld /system" % (now)
        exec_shell(cmd)

    def _init_etcd(self, hosts):
        def _init_etcd_(self, host, statue, lst):
            s = ''
            for i in lst:
                s = s + "%s," % (i)
                cmd = "%s etcd --state %s --hosts %s" % (self.config.uss_node, statue, s[:-1])
            dmsg(cmd)
            (out, err) = exec_remote(host, cmd)
            if (out):
                dmsg(host + ":\n" + out)
            if (err):
                dwarn(host + ":\n" + err)

        args = [[self, k, 'new', hosts] for (k) in hosts]
        mutil_exec(_init_etcd_, args)
            
    def create(self, hosts):
        self._add_check_env(hosts)
        '''
        if self.config.use_redis():
        '''
        self.update(hosts=hosts)
        self._init_etcd(hosts=hosts)
        #self._init_redis()
        #self._start_redis()

        conf = ClusterConf(config=self.config, lock=False)
        self.config = conf.add_node(hosts)
        
        #self._add_update_cluster_conf(hosts)
        #self._init_atomicid()
        self._init_env(hosts)
        self._init_root()
        self._init_mond(hosts)
        self._init_system()
        self._init_create_time()
        self._init_secret_key()
        self._start_service(hosts)

    def _add_etcd(self, hosts):
        cmd = "%s/app/admin/etcd_manage.py member_add --hosts " % (self.config.home)  + ','.join(hosts)
        os.system(cmd)
        
    def addnode(self, hosts):
        old_hosts = self.hosts()
        new_hosts = hosts

        self._add_check_env(new_hosts)
        self.update(hosts=new_hosts)

        conf = ClusterConf(lock=True)
        self.config = conf.add_node(old_hosts+new_hosts)
        
        #self._add_update_cluster_conf(old_hosts+new_hosts)

        self._add_etcd(new_hosts);
        self._init_env(new_hosts)
        self._init_mond(hosts)
        self._start_service(new_hosts)

    def dropnode(self, hosts, force=False):
        if not force:
            raise Exp(1, "now only support with --force")

        old_hosts = self.hosts()
        del_hosts = hosts
        for host in del_hosts:
            if host not in old_hosts:
                raise Exp(errno.ENOENT, "not found %s" % host)

        self._drop_update_cluster_conf(del_hosts)

    def _ucarp_conf_group(self, hosts, vip, master_vip):
        master_skew = 5
        slave_skew = 10
        print "hosts %s, vip:%s, master_vip:%s" % (hosts, vip, master_vip)
        def _warp(h):
            if h == master_vip:
                cmd = "python2 %s ucarpconf --srcip %s --addr %s --skew %s" % (self.config.uss_node, h.strip(), vip.strip(), master_skew)
            else:
                cmd = "python2 %s ucarpconf --srcip %s --addr %s --skew %s" % (self.config.uss_node, h.strip(), vip.strip(), slave_skew)

            x, y = exec_remote(h, cmd)
            print "ucarp conf host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in hosts]
        mutil_exec(_warp, args)

    def ucarpstart(self):
        def _warp(h):
            cmd = "python2 %s ucarpstart" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "start ucarp host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def ucarpstatus(self):
        def _warp(h):
            cmd = "python2 %s ucarpstatus" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "host: %s, status : %s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def ucarpstop(self):
        def _warp(h):
            cmd = "python2 %s ucarpstop" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "stop ucarp host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def ucarpdestroy(self):
        def _warp(h):
            cmd = "python2 %s ucarpdestroy" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "destroy ucarp host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def ucarpconf(self, groups):
        local_clusterid = ""
        dst_clusterid = ""

        try:
            cmd = "%s | grep 'cluster id' | awk -F':' '{print $2}'| awk '{print $1}'" % (self.config.uss_mdstat)
            out, err = exec_shell(cmd, need_return=True, p=False, timeout=30)
            local_clusterid = out.split("\n")[0]
        except Exception as e:
            raise Exp(1, "get local clusterid fail!")

        for group in groups:
            hosts = group.split(":")[0].split(",")
            vip_info = group.split(":")[1].strip()
            vip = vip_info.split("/")[0]
            master_vip = vip_info.split("/")[1]
            master_vip_found = 0

            check_ip_valid(vip)

            for host in hosts:
                try:
                    cmd = "%s | grep 'cluster id' | awk -F':' '{print $2}'| awk '{print $1}'" % (self.config.uss_mdstat)
                    x, y = exec_remote(host, cmd)
                    dst_clusterid = x.split('\n')[0]
                except Exception as e:
                    raise Exp(1, "get host %s cluster id fail!" % (host))

                if dst_clusterid != local_clusterid:
                    raise Exp(errno.EINVAL, "host %s not in cluster !" % (host))

                if master_vip == host:
                    master_vip_found = 1

            if master_vip_found == 0:
                raise Exp(errno.EINVAL, "master vip %s not avaliable, please check it !" % (master_vip))

            self._ucarp_conf_group(hosts, vip, master_vip)

    def _exec_cmd_on_cluster_nodes(self, cmd, is_raise=1):
        def _warp(h):
            x, y = exec_remote(h, cmd)
            if y:
                print "\thost:%s, errmsg:%s" % (h, y)

        mutil_exec_futures(_warp, self.config.cluster.keys(), is_raise)

    def _useradd_to_db(self, username, password, useradd_args):
        cmd = "%s --set %s --password %s %s" % (self.config.uss_user, username, password, useradd_args)
        exec_shell(cmd)
        print "useradd to db cmd : %s" % cmd

    def _userdel_from_db(self, username):
        cmd = "%s --remove %s" % (self.config.uss_user, username)
        exec_shell(cmd)
        print "userdel from db cmd : %s" % cmd

    def _usermod_by_db(self, username, usermod_args):
        cmd = "%s --set %s %s" % (self.config.uss_user, username, usermod_args)
        exec_shell(cmd)
        print "usermod from db cmd : %s" % cmd

    def useradd(self, username, password, uid="", gid=""):
        _args = ""

        if uid is not None:
            _args = "--uid %s" % (uid)

        if gid is not None:
            _args = "%s --gid %s" % (_args, gid)

        cmd = "python2 %s useradd --name %s --password %s %s" % (self.config.uss_node, username, password, _args)
        self._exec_cmd_on_cluster_nodes(cmd)
        self._useradd_to_db(username, password, _args)

    def userdel(self, username):
        self._userdel_from_db(username)
        cmd = "python2 %s userdel --name %s" % (self.config.uss_node, username)
        self._exec_cmd_on_cluster_nodes(cmd)

    def usermod(self, username, password="", uid="", gid=""):
        _args = ""

        if uid is not None:
            _args = "--uid %s" % (uid)

        if gid is not None:
            _args = "%s --gid %s" % (_args, gid)

        if password is not None:
            _args = "%s --password %s" % (_args, password)

        cmd = "python2 %s usermod --name %s %s" % (self.config.uss_node, username, _args)
        self._exec_cmd_on_cluster_nodes(cmd)
        self._usermod_by_db(username, _args)

    def _groupadd_to_db(self, groupname, groupadd_args):
        cmd = "%s --set %s %s" % (self.config.uss_group, groupname, groupadd_args)
        exec_shell(cmd)
        print "groupadd to db cmd : %s" % cmd

    def _groupdel_from_db(self, groupname):
        cmd = "%s --remove %s" % (self.config.uss_group, groupname)
        exec_shell(cmd)
        print "groupdel from db cmd : %s" % cmd

    def groupadd(self, groupname, gid=""):
        _args = ""

        if gid is not None:
            _args = "--gid %s" % (gid)

        cmd = "python2 %s groupadd --name %s %s" % (self.config.uss_node, groupname, _args)
        self._exec_cmd_on_cluster_nodes(cmd)
        self._groupadd_to_db(groupname, _args)

    def groupdel(self, groupname):
        self._groupdel_from_db(groupname)
        cmd = "python2 %s groupdel --name %s" % (self.config.uss_node, groupname)
        self._exec_cmd_on_cluster_nodes(cmd)

    def _ad_enable(self, domain, user, passwd):
        dmsg("enable start")
        cmd = "/opt/sdfs/app/admin/samba.py reconf"
        cmd = cmd + " ;pkill -9 winbindd"
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)

        #切换到AD域模式下获取域信息更新krb5文件
        cmd = "rm -f /etc/krb5.conf && python /opt/sdfs/app/admin/kerb.py genconf -f /etc/krb5.conf"
        #配置nsswitch适配winbind
        cmd = cmd + " ;python /opt/sdfs/app/admin/nsswitch-conf-update.py"
        #创建winbind相关的软链接
        cmd = cmd + " ;python /opt/sdfs/app/admin/create_access_relation.py"
        #更新/etc/pam.d/password-auth-ac文件
        cmd = cmd + " ;python /opt/sdfs/app/admin/password-auth-ac-config-update.py"
        #更新/usr/local/samba/etc/ctdb/event.d/50.samba文件
        cmd = cmd + " ;python /opt/sdfs/app/admin/samba_config_update.py"
        #更新/usr/local/samba/etc/ctdb/event.d/00.ctdb文件
        cmd = cmd + " ;python /opt/sdfs/app/admin/ctdb_config_update.py"
        cmd = cmd + " ;ntpdate -b %s" % (domain)
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)
        dmsg("join AD")

        #集群节点加入AD域
        _exec_join = "/usr/local/samba/bin/net ads join -U '%s%%%s'" % (user, passwd)
        _exec_check = "/usr/local/samba/bin/net ads testjoin"
        for host in self.hosts():
            out = ""
            err = ""
            try:
                out, err = exec_remote(host, _exec_join)
            except Exp, e:
                pass
            print '%s join ad, out : %s' % (host, out)

            try:
                out, err = exec_remote(host, _exec_check)
            except Exp, e:
                pass

            print '%s test join, out : %s' % (host, out)
            time.sleep(2)


        #重启集群各个节点samba服务
        cmd = "systemctl restart uss_samba"
        cmd = cmd + " ;/usr/local/samba/sbin/winbindd -D"
        cmd = cmd + " ;/usr/local/samba/bin/wbinfo -t"
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)

        _exec_attr_set = "%s -s mode -V ad /system" % (self.config.uss_attr)
        exec_shell(_exec_attr_set)

    def _ad_disable(self, domain, user, passwd):
        dmsg("leave AD")
        out = ""
        err = ""
        #集群节点离开AD域
        _exec_leave = "/usr/local/samba/bin/net ads leave -U '%s%%%s'" % (user, passwd)
        for host in self.hosts():
            out, err = exec_remote(host, _exec_leave)
            print "%s leave AD, out : %s, err : %s" % (host, out, err)
            time.sleep(2)

        #重启集群各个节点samba服务
        cmd = "pkill -9 winbindd"
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)

        _exec_attr_set = "%s -s mode -V user /system" % (self.config.uss_attr)
        exec_shell(_exec_attr_set)

    def _ad_get(self):
        #ip;域名;用户名;密码
        domain = None
        user = None
        passwd = None

        cmd = "%s -g ad /system" % (self.config.uss_attr)
        out, err = exec_shell(cmd, p=True, need_return=True)
        domain = out.strip().split(";")[1]
        user = out.strip().split(";")[2]
        passwd = out.strip().split(";")[3]

        return (domain, user, passwd)

    def ad(self, able=True):
        derror("ad disabled")
        return
        assert(type(able) == type(True))

        try:
            domain, user, passwd  = self._ad_get()
        except Exp, e:
            if (e.errno == 126):
                domain = None
                user = None
                passwd = None
            else:
                raise

        if able:
            if None in [domain, user, passwd]:
                raise Exp(1, "domain %s user %s passwd %s invalid" % (domain, user, passwd))
            self._ad_enable(domain, user, passwd)
        else:
            if None in [domain, user, passwd]:
                dmsg("domain %s user %s passwd %s, skip disable" % (domain, user, passwd))
                return None
            self._ad_disable(domain, user, passwd)

    def _ldap_enable(self, server, dn):
        cmd = "pkill -9 winbindd;pkill -9 nscd"
        cmd = cmd + ' ;authconfig --disablewinbind --disablewins --disablesssd --disablenis --enableldap --enableldapauth --ldapserver="%s" --ldapbasedn="%s" --enablemkhomedir --update' % (server, dn)
        cmd = cmd + " ;systemctl start nslcd"
        print cmd
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)

        _exec_attr_set = "%s -s mode -V ldap /system" % (self.config.uss_attr)
        exec_shell(_exec_attr_set)

    def _ldap_disable(self, server, dn):
        cmd = 'authconfig --disableldap --disableldapauth --ldapserver="%s" --ldapbasedn="%s" --enablemkhomedir --update' % (server, dn)
        print cmd
        self._exec_cmd_on_cluster_nodes(cmd, is_raise=0)

        _exec_attr_set = "%s -s mode -V user /system" % (self.config.uss_attr)
        exec_shell(_exec_attr_set)

    def _ldap_get(self):
        server = None
        dn = None

        cmd = "%s -g ldap /system" % (self.config.uss_attr)
        out, err = exec_shell(cmd, p=True, need_return=True)
        server = out.strip().split(";")[0]
        dn = out.strip().split(";")[1]

        return (server, dn)

    def ldap(self, able=True):
        assert(type(able) == type(True))

        try:
            server, dn = self._ldap_get()
        except Exp, e:
            if (e.errno == 126):
                server = None
                dn = None
            else:
                raise

        if able:
            if (server is None or dn is None):
                raise Exp(1, "server %s dn %s invalid" % (server, dn))
            self._ldap_enable(server, dn)
        else:
            if (server is None or dn is None):
                dmsg("server %s dn %s, skip disable" % (server, dn))
                return None
            self._ldap_disable(server, dn)

    def viplist(self, json_show=False):
        if not os.path.isfile(self.config.vip_conf):
            dwarn("cluster vip service not configured, please check it !")
            sys.exit(errno.EPERM)

        group_dic = {}

        data = vip_loadconf(self.config.vip_conf)
        for group in data.keys():
            host_dic = {}
            if not json_show:
                print "-----------------------------------------------------"
                print "%s(%s):" % (group, data[group]["type"])
            else:
                host_dic["type"] = data[group]["type"]

            def _warp(h):
                try:
                    cmd = "python2 %s getvip --group %s" % (self.config.uss_node, group)
                    x, y = exec_remote(h, cmd)
                    vips = x.strip()
                    if vips == "":
                        vips = "None"

                    if not json_show:
                        print "        host:%s --> vip:%s\n" % (h, vips)
                    else:
                        host_dic[h] = vips
                except Exp, e:
                    if not json_show:
                        print "        host:%s --> vip:None\n" % (h)
                    else:
                        host_dic[h] = "None"
                group_dic[group] = host_dic

            args = [[x] for x in data[group]["nodes"].split(',')]
            mutil_exec(_warp, args)

        if json_show:
            print json.dumps(group_dic)

    def vipstart(self):
        def _warp(h):
            cmd = "python2 %s vipstart" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def vipstop(self):
        def _warp(h):
            cmd = "python2 %s vipstop" % (self.config.uss_node)
            x, y = exec_remote(h, cmd)
            print "host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def hosts(self):
        return self.config.cluster.keys()

    def vipconfadd(self, hosts, ops, vips):
        vip = vips.split(',')[0]
        mask = get_mask_by_addr(vip)

        cmd = "python2 %s addconf -t %s -v %s -H %s -m %s" % (self.config.uss_vip, ops, vips, hosts, mask)
        self._exec_cmd_on_cluster_nodes(cmd)

    def vipadd(self, group, host, vip):
        if host == None and vip == None:
            derror("vip and host can not be null at the same time.")
            sys.exit(1)

        cmd = "python2 %s add -g %s" % (self.config.uss_vip, group)

        if vip is not None:
            cmd = cmd + " -v %s" % (vip)

        if host is not None:
            cmd = cmd + " -H %s" % (host)

        #print 'cmd',cmd

        def _warp(h):
            x, y = exec_remote(h, cmd)
            print "stat host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def vipdel(self, group, host, vip):
        if host == None and vip == None:
            derror("vip and host can not be null at the same time.")
            sys.exit(1)

        cmd = "python2 %s del -g %s" % (self.config.uss_vip, group)

        if vip is not None:
            cmd = cmd + " -v %s" % (vip)

        if host is not None:
            cmd = cmd + " -H %s" % (host)

        #print 'cmd',cmd

        def _warp(h):
            x, y = exec_remote(h, cmd)
            #print "stat host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def vipdestroy(self):
        self.vipstop()
        def _warp(h):
            cmd = "rm -rf %s" % (self.config.vip_conf)
            x, y = exec_remote(h, cmd)
            print "host: %s vip service destroy ok\n" % (h)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def dnsconf(self, name, dnsip, hosts):
        cmd = "python2 %s dnsconf --name %s --dnsip %s --hosts %s" % (self.config.uss_node, name, dnsip, hosts)
        self._exec_cmd_on_cluster_nodes(cmd)

    def dns_list(self, name=None):
        if name is not None:
            domain_conf = "/var/named/%s.zone" % (name)
            if not os.path.isfile(domain_conf):
                raise Exp(errno.ENOENT, "not such domain name : %s" % (name))
            cmd =  "grep 'www' %s | awk '{print $4}'" % (domain_conf)
            out, err = exec_shell(cmd, p=False, need_return=True)
            print out.strip()
            return

        cmd = "grep -n '0.in-addr.arpa' /etc/named.rfc1912.zones | awk -F':' '{print $1}'"
        out, err = exec_shell(cmd, p=False, need_return=True)
        line_num = out.strip()

        cmd = "tail -n +%s /etc/named.rfc1912.zones| grep 'zone ' | awk 'NR > 1 {print $2}' | awk -F'\"' '{print $2}'" % (line_num)
        out, err = exec_shell(cmd, p=False, need_return=True)
        for name in out.strip().split('\n'):
            print "%s" % (name)

    def dns_del(self, name):
        def _warp(h):
            cmd = "python2 %s dnsdel --name %s" % (self.config.uss_node, name)
            x, y = exec_remote(h, cmd)
            print "host: %s \n%s" % (h, x)
            if y:
                print y

        args = [[x] for x in self.config.cluster.keys()]
        mutil_exec(_warp, args)

    def vipconfdel(self, group):
        cmd = "python2 %s delconf -g %s" % (self.config.uss_vip, group)
        self._exec_cmd_on_cluster_nodes(cmd)

def handle(cluster, args):
    if args.option == 'sshkey':
        password = raw_input("input password:")
        cluster.sshkey(args.hosts.split(","), password)
    elif args.option == 'start':
        cluster.start()
    elif args.option == 'stop':
        cluster.stop()
    elif args.option == 'list':
        cluster.stat()
    elif args.option == 'update':
        hosts = None
        if args.hosts is not None:
            hosts = args.hosts.split(",")
        cluster.update(args.src, hosts)
    elif args.option == 'create':
        if args.hosts is None:
            raise Exp(1, "need --hosts")
        hosts = args.hosts.split(",")
        dmsg("create cluster %s" % (hosts))
        cluster.create(hosts)
    else:
        raise Exp(1, "unknown option %s" % (args.option))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    #cluster operation
    def _start(args, cluster):
        cluster.start()
    parser_start = subparsers.add_parser('start', help='start services')
    parser_start.set_defaults(func=_start)

    def _stop(args, cluster):
        cluster.stop()
    parser_stop = subparsers.add_parser('stop', help='stop services')
    parser_stop.set_defaults(func=_stop)

    def _restart(args, cluster):
        cluster.restart()
    parser_restart = subparsers.add_parser('restart', help='restart services')
    parser_restart.set_defaults(func=_restart)

    def _stat(args, cluster):
        cluster.stat()
    parser_stat = subparsers.add_parser('list', help='list the service of cluster')
    parser_stat.set_defaults(func=_stat)

    def _update(args, cluster):
        hosts = None
        if args.hosts is not None:
            hosts = args.hosts.split(",")
        cluster.update(args.src, args.tar, args.nfs, args.f, hosts)

    parser_update = subparsers.add_parser('update', help='update app or etc or samba')
    parser_update.add_argument("--hosts", default=None, help="host1,host2,...")
    parser_update.add_argument("--src", default=None, choices=["app", "etc", "samba"])
    parser_update.add_argument("--tar", action='store', help="update a tar package")
    parser_update.add_argument("--nfs", action='store', help="update nfs package")
    parser_update.add_argument("-f", action='store_true', default=False, help="force update")
    parser_update.set_defaults(func=_update)

    def _sshkey(args, cluster):
        password = raw_input("input password:")
        cluster.sshkey(args.hosts.split(","), password)
    parser_sshkey = subparsers.add_parser('sshkey', help='make nopassword')
    parser_sshkey.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_sshkey.set_defaults(func=_sshkey)

    def _create(args, cluster):
        hosts = args.hosts.split(",")
        dmsg("create cluster %s" % (hosts))
        cluster.create(hosts)
    parser_create = subparsers.add_parser('create', help='create cluster')
    parser_create.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_create.set_defaults(func=_create)

    def _addnode(args, cluster):
        hosts = args.hosts.split(",")
        dmsg("add hosts %s" % (hosts))
        cluster.addnode(hosts)
    parser_addnode = subparsers.add_parser('addnode', help='add hosts')
    parser_addnode.add_argument("--hosts", required=True, help="host1,host2,...")
    parser_addnode.set_defaults(func=_addnode)

    #ucarp operation
    def _ucarp_conf(args, cluster):
        groups = args.hosts.split(";")
        dmsg("ucarp groups %s" % (groups))
        cluster.ucarpconf(groups)
    parser_ucarpconf = subparsers.add_parser('ucarpconf', help='ucarp configure')
    parser_ucarpconf.add_argument("--hosts", required=True, help='likes "host1,host2,host3:vip1/host1;host1,host2,host3:vip2/host2;host1,host2,host3:vip2/host3"')
    parser_ucarpconf.set_defaults(func=_ucarp_conf)

    def _ucarp_start(args, cluster):
        cluster.ucarpstart()
    parser_ucarpstart = subparsers.add_parser('ucarpstart', help='start ucarp services')
    parser_ucarpstart.set_defaults(func=_ucarp_start)

    def _ucarp_status(args, cluster):
        cluster.ucarpstatus()
    parser_ucarpstatus = subparsers.add_parser('ucarpstatus', help='list cluster ucarp status')
    parser_ucarpstatus.set_defaults(func=_ucarp_status)

    def _ucarp_stop(args, cluster):
        cluster.ucarpstop()
    parser_ucarp_stop = subparsers.add_parser('ucarpstop', help='stop ucarp services')
    parser_ucarp_stop.set_defaults(func=_ucarp_stop)

    def _ucarp_destroy(args, cluster):
        cluster.ucarpdestroy()
    parser_ucarp_destory = subparsers.add_parser('ucarpdestroy', help='destroy ucarp services')
    parser_ucarp_destory.set_defaults(func=_ucarp_destroy)

    #user operation
    def _useradd(args, cluster):
        cluster.useradd(args.name, args.password, args.uid, args.gid)
    parser_useradd = subparsers.add_parser('useradd', help='add a user')
    parser_useradd.add_argument("--name", required=True, help="user name")
    parser_useradd.add_argument("--password", required=True, help="user password")
    parser_useradd.add_argument("--uid", default=None, help="user ID")
    parser_useradd.add_argument("--gid", default=None, help="user group ID")
    parser_useradd.set_defaults(func=_useradd)

    def _userdel(args, cluster):
        cluster.userdel(args.name)
    parser_userdel = subparsers.add_parser('userdel', help='del a user')
    parser_userdel.add_argument("--name", required=True, help="user name")
    parser_userdel.set_defaults(func=_userdel)

    def _usermod(args, cluster):
        cluster.usermod(args.name, args.password, args.uid, args.gid)
    parser_usermod = subparsers.add_parser('usermod', help='modify user info')
    parser_usermod.add_argument("--name", required=True, help="user name")
    parser_usermod.add_argument("--password", default=None, help="user password")
    parser_usermod.add_argument("--uid", default=None, help="user ID")
    parser_usermod.add_argument("--gid", default=None, help="user group ID")
    parser_usermod.set_defaults(func=_usermod)

    #group operation
    def _groupadd(args, cluster):
        cluster.groupadd(args.name, args.gid)
    parser_groupadd = subparsers.add_parser('groupadd', help='add a group')
    parser_groupadd.add_argument("--name", required=True, help="group name")
    parser_groupadd.add_argument("--gid", default=None, help="group ID")
    parser_groupadd.set_defaults(func=_groupadd)

    def _groupdel(args, cluster):
        cluster.groupdel(args.name)
    parser_groupdel = subparsers.add_parser('groupdel', help='del a group')
    parser_groupdel.add_argument("--name", required=True, help="group name")
    parser_groupdel.set_defaults(func=_groupdel)

    def _ldap(args, cluster):
        able = None
        if args.config == "enable":
            able = True
        elif args.config == "disable":
            able = False
        else:
            raise Exp(errno.EINVAL, "invalid, need enable or disable")
        cluster.ldap(able)
    parser_ldap = subparsers.add_parser('ldap', help='enable or disable ldap')
    parser_ldap.add_argument("--config", required=True, help="enable or disable")
    parser_ldap.set_defaults(func=_ldap)

    def _ad(args, cluster):
        able = None
        if args.config == "enable":
            able = True
        elif args.config == "disable":
            able = False
        else:
            raise Exp(errno.EINVAL, "invalid, need enable or disable")
        cluster.ad(able)
    parser_ad = subparsers.add_parser('ad', help='enable or disable ad')
    parser_ad.add_argument("--config", required=True, help="enable or disable")
    parser_ad.set_defaults(func=_ad)

    def _vip_list(args, cluster):
        cluster.viplist(args.json)
    parser_viplist = subparsers.add_parser('viplist', help='list cluster vips')
    parser_viplist.add_argument('-j', '--json', default=False, help="enable or disable")
    parser_viplist.set_defaults(func=_vip_list)

    def _vip_start(args, cluster):
        cluster.vipstart()
    parser_vipstart = subparsers.add_parser('vipstart', help='start cluster vip service')
    parser_vipstart.set_defaults(func=_vip_start)

    def _vip_stop(args, cluster):
        cluster.vipstop()
    parser_vipstop = subparsers.add_parser('vipstop', help='stop cluster vip service')
    parser_vipstop.set_defaults(func=_vip_stop)

    #vipconf add
    def _vipconf_add(args, cluster):
        cluster.vipconfadd(args.host, args.type, args.vip)
    parser_add = subparsers.add_parser('vipconfadd', help='create a new vipconf')
    parser_add.add_argument('-t', '--type', required=True,help='user or dns')
    parser_add.add_argument('-H', '--host', required=True,help='cluser nodes ip eg:192.168.1.10,192.168.1.11,192.168.1.12')
    parser_add.add_argument('-v', '--vip', required=True,help='vip address eg:192.168.1.100')
    parser_add.set_defaults(func=_vipconf_add)

    #vipconf delete
    def _vipconf_del(args, cluster):
        cluster.vipconfdel(args.group)
    parser_delete = subparsers.add_parser('vipconfdel', help='delete a group in vipconf')
    parser_delete.add_argument('-g', '--group', required=True, help='group eg: group1')
    parser_delete.set_defaults(func=_vipconf_del)

    #vipconf addnodes
    def _vip_add(args, cluster):
        cluster.vipadd(args.group, args.host, args.vip)
    parser_addnodes = subparsers.add_parser('vipadd', help='vipconf add host or vip')
    parser_addnodes.add_argument('-g', '--group', required=True, help='group eg: group1')
    parser_addnodes.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.1.10,192.168.1.11,192.168.1.12')
    parser_addnodes.add_argument('-v', '--vip', help='vip address eg:192.168.1.100')
    parser_addnodes.set_defaults(func=_vip_add)

    #vipconf delnodes
    def _vip_del(args, cluster):
        cluster.vipdel(args.group, args.host, args.vip)
    parser_delnodes = subparsers.add_parser('vipdel', help='vipconf del host or vip')
    parser_delnodes.add_argument('-g', '--group', required=True, help='group eg: group1')
    parser_delnodes.add_argument('-H', '--host', help='cluser nodes ip eg:192.168.1.10,192.168.1.11,192.168.1.12')
    parser_delnodes.add_argument('-v', '--vip', help='vip address eg:192.168.1.100')
    parser_delnodes.set_defaults(func=_vip_del)

    def _vip_destroy(args, cluster):
        cluster.vipdestroy()
    parser_vipdestroy = subparsers.add_parser('vipdestroy', help='destroy cluster vip service')
    parser_vipdestroy.set_defaults(func=_vip_destroy)

    def _dns_conf(args, cluster):
        cluster.dnsconf(args.name, args.dnsip, args.hosts)
    parser_dnsconf = subparsers.add_parser('dnsconf', help='set cluster dns')
    parser_dnsconf.add_argument("--name", required=True, help="domain name")
    parser_dnsconf.add_argument("--dnsip", required=True, help="dns server ip address")
    parser_dnsconf.add_argument("--hosts", required=True, help="domain name corresponds to the IP addresses, split by ','")
    parser_dnsconf.set_defaults(func=_dns_conf)

    def _dns_list(args, cluster):
        cluster.dns_list(args.name)
    parser_dnslist = subparsers.add_parser('dnslist', help='list domain name')
    parser_dnslist.add_argument("--name", default=None, help="domain name")
    parser_dnslist.set_defaults(func=_dns_list)

    def _dns_del(args, cluster):
        cluster.dns_del(args.name)
    parser_dnsdel = subparsers.add_parser('dnsdel', help='del domain name')
    parser_dnsdel.add_argument("--name", required=True, help="domain name")
    parser_dnsdel.set_defaults(func=_dns_del)

    #main func
    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    cluster = Cluster()
    args.func(args, cluster)
