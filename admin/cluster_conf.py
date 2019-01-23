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
import etcd

from config import Config
from instence import Instence
from utils import mutil_exec, check_crontab, unset_crontab, Exp, \
                  exec_shell, check_sysctl, lock_file, derror, dwarn,\
                  human_readable, dev_mountpoints, dev_lsblks, \
                  dev_mkfs_ext4, dev_uuid, dev_childs, fstab_del_mount, \
                  dev_clean, ssh_set_nopassword, put_remote, dmsg, \
                  exec_remote, lsb_release, check_ip_valid, mutil_exec_futures, \
                  get_mask_by_addr

class ClusterConf:
    def __init__(self, config=None, lock=False):
        if lock:
            self.config = None
            self.etcd = etcd.Client(host='127.0.0.1', port=2379)
            self.lock = etcd.Lock(self.etcd, "clusterconf")
        else:
            self.config = config
            self.etcd = None
            self.lock = None

    def _lock(self):
        if (self.etcd == None):
            return

        dmsg("lock clusterconf")
        self.lock.acquire(blocking=True, lock_ttl=30)
        
    def _unlock(self):
        if (self.etcd == None):
            return

        dmsg("unlock clusterconf")
        self.lock.release()
        
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

    def add_node(self, hosts):
        self._lock();

        if (self.config == None):
            self.config = Config()
        
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

        self._unlock()

        return self.config

    def add_disk(self, disks):
        self._lock();

        if (self.config == None):
            self.config = Config()

        for d in disks:
            if not self.config.service.has_key(d):
                self.config.service["cds"].append(d)

        self.config.cluster.update({self.config.hostname: self.config.service})

        #self._add_check_cluster_conf(cluster)
        #self.config.cluster = cluster
        dumpto = "/tmp/uss_cluster.conf"
        self.config.dump_cluster(dumpto=dumpto)
        self.deploy(dumpto, self.config.cluster_conf, self.config.cluster.keys())

        self._unlock()
        return self.config
        
    def deploy(self, src, dist, hosts):
        for h in hosts:
            put_remote(h, src, dist)
        
    def drop_node(self, hosts):
        self._lock();

        if (self.config == None):
            self.config = Config()
        
        for h in hosts:
            del self.config.cluster[h]

        self._add_check_cluster_conf(self.config.cluster)
        dumpto = "/tmp/uss_cluster.conf"
        self.config.dump_cluster(dumpto=dumpto)
        self.deploy(dumpto, self.config.cluster_conf, hosts)
 
        self._unlock()

        return self.config
