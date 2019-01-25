#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import errno
import argparse
import os
import time
import sys
import errno
import uuid
import re
import random
import shutil

from argparse import RawTextHelpFormatter

from config import Config
from instence import Instence
from disk_manage import DiskManage
from utils import mutil_exec, check_crontab, unset_crontab, Exp, \
                  exec_shell, check_sysctl, lock_file, derror, dwarn, dmsg,\
                  human_readable, dev_mountpoints, dev_lsblks, \
                  dev_mkfs_ext4, dev_uuid, dev_childs, fstab_del_mount, \
                  dev_clean, install_init, check_ip_valid, exec_remote, \
                  put_remote, set_value, check_process_exists
from vip import vip_unset, vip_set, del_local_vips, get_local_vips, \
                set_vips, vip_isexist, vip_del
from samba import samba_start, samba_stop
from etcd_manage import Etcd_manage
from redisd import Redisd
from cluster_conf import ClusterConf

class Node:
    def __init__(self, config=None):
        self.config = config
        if self.config is None:
            self.config = Config()
        self.ttyonly = False

    def _get_instences(self):
        instences = []
        for role in self.config.roles:
            #print (role, self.config.roles)
            role_dir = os.path.join(self.config.workdir, role)
            ls = sorted(os.listdir(role_dir))
            for i in ls:
                try:
                    num = int(i)
                except ValueError, e:
                    derror("bad path %s/%s" % (role_dir, i))
                    raise Exp(errno.EIO, '%s' % (i))
                    continue
                instences.append(Instence(role, i, self.config))

        #instences.append(Instence("uss_nfs", -1, self.config))
        #instences.append(Instence("uss_iscsi", -1, self.config))
        #instences.append(Instence("uss_ftp", -1, self.config))
        return instences

    def _get_services(self, host):
        #return services = {"cds": [], "mond": []}
        services = {}
        for role in self.config.roles:
            path = os.path.join(self.config.workdir, role)
            cmd = "mkdir -p %s;ls %s" % (path, path)
            stdout, _ = exec_remote(host, cmd)
            #print (role, stdout.split())
            services.update({role: stdout.split()})
        return services

    def _start_service(self):
        #derror("samba, nfs-ganesha start unimplemented")
        return
        #start nfs service
        _exec_nfs = "systemctl start nfs-ganesha"
        exec_shell(_exec_nfs, p=True)

        #start samba service
        _exec_samba = "systemctl start uss_samba"
        exec_shell(_exec_samba, p=True)

    def _init_redis(self):
        redis = self.config.service["redis"]
        if (redis == None):
            return
        
        #print (self.config.redis_dir, redis)

        for i in redis:
            cmd = "mkdir -p %s/%s" % (self.config.redis_dir, i)
            os.system(cmd)

        lst = os.listdir(self.config.redis_dir)

        for i in lst:
            #path = os.path.join(self.config.redis_dir, str(i))
            #dmsg("init redis " + path)
            #redisd = Redisd(path)
            #redisd.init()
            
            cmd = "python2 %s --init %s/%s > %s/log/redis_disk_%s.log 2>&1" % (self.config.uss_redisd, self.config.redis_dir, i, self.config.home, i)
            (out, err) = exec_shell(cmd, p=False, need_return=True)

    def _start_redis(self):
        lst = os.listdir(self.config.redis_dir)

        for i in lst:
            cmd = "python2 %s --start %s/%s" % (self.config.uss_redisd, self.config.redis_dir, i)
            os.system(cmd)

    def _stop_redis(self):
        lst = os.listdir(self.config.redis_dir)

        for i in lst:
            cmd = "python2 %s --stop %s/%s" % (self.config.uss_redisd, self.config.redis_dir, i)
            os.system(cmd)
            
    def start(self, role=None, service=None, op="all"):
        lfile = "/var/run/uss.start.lock"
        lock = lock_file(lfile)

        check_sysctl(self.config, fix = True)

        cmd = "iptables -F"
        os.system(cmd)

        cmd = "systemctl start rpcbind"
        os.system(cmd)

        cmd = "systemctl start etcd"
        os.system(cmd)

        self._start_redis()
        
        if (service is not None) and (role is not None):
            i = Instence(role, service, self.config)
            return i.start(self.ttyonly)

        def instance_start_warp(i):
            i.start(self.ttyonly)

        instences = self._get_instences()
        args = [[x] for x in instences]
        mutil_exec(instance_start_warp, args, timeout=30, timeout_args=[])
        check_crontab(self.config)

        if op == "all":
            self._start_service()

        os.system("rm " + lfile)

    def stop(self, role=None, service=None):
        lfile = "/var/run/uss.stop.lock"
        lock = lock_file(lfile)

        if (service is not None) and (role is not None):
            i = Instence(role, service, self.config)
            return i.stop(self.ttyonly)

        #first stop minio srevice
        #minio = Minio()
        #minio.stop()

        def instance_stop_warp(i):
            i.stop(self.ttyonly)

        instences = self._get_instences()
        args = [[x] for x in instences]
        mutil_exec(instance_stop_warp, args)
        unset_crontab()

        derror("samba, nfs stop unimplemented")
        """
        _exec_samba = "systemctl stop uss_samba"
        exec_shell(_exec_samba)

        _exec_nfs = "systemctl stop nfs-ganesha"
        exec_shell(_exec_nfs)
        """
        self._stop_redis()

        os.system("rm " + lfile)
        
    def _construct_dict(self, i):
        disk_total = i.get_total()
        disk_used = i.get_used()

        if disk_total == 0:
            disk_used_rate = "0%"
        else:
            disk_used_rate = str(int(float(disk_used)/disk_total*100)) + "%"

        s = {"name": i.name, "running": i.running(), "nid": i.nid,
            "disk_status": i.disk_status, "skiped": i.skiped,
            "deleting": i.deleting,
            "total": human_readable(disk_total, 1),
            "used": human_readable(disk_used, 1),
            "used_rate": disk_used_rate
            }

        return s

    def stat(self):
        lfile = "/var/run/uss.stat.lock"
        lock = lock_file(lfile, p=False)

        stats = []
        instences = self._get_instences()
        for i in instences:
            s = self._construct_dict(i)
            stats.append(s)
        return stats

    def _stat_show(self):
        stats = self.stat()
        for stat in stats:
            if stat["running"]:
                s = "running"
            else:
                s = "stopped"

            if stat["skiped"]:
                s = s + ",skiped"

            if stat["deleting"]:
                s = s + ",deleting"

            if stat["disk_status"]:
                s = s + ",disk error"

            print "%s %s %s %s/%s %s" % (stat["name"], s, stat["nid"],
                                      stat["used"], stat["total"], stat["used_rate"])
    def objck_nolock(self, check_flag=None):
        if (check_flag == None):
            cmd = "%s" % self.config.uss_lobjck
        else:
            cmd = "%s -c" % self.config.uss_lobjck
        exec_shell(cmd)

    def is_master(self):
        cmd = """etcdctl get /sdfs/mond/master | awk -F',' '{print $1}'"""
        master, _ = exec_shell(cmd, p=False, need_return=True, timeout=7)
        master = master.strip()

        try:
            #dmsg("master: %s, self: %s" % (master, nid))
            if (self.config.hostname == master):
                return True
        except IOError, e:
            if e.errno != errno.ENOENT:
                raise

        return False

    def _objck_leveldb(self, check_flag=None):
        #先杀死前一个执行, 避免重复执行
        n = os.path.basename(self.config.uss_lobjck)
        cmd = "pkill -9 %s" % (n)
        try:
            exec_shell(cmd)
        except Exp, e:
            pass

        if self.is_master():
            self.objck_nolock(check_flag)
        else:
            dwarn("need run with master!")



    def _objck_redis(self, check_flag=None):
        #先杀死前一个执行, 避免重复执行
        n = os.path.basename(self.config.uss_robjck)
        cmd = "pkill -9 %s" % (n)
        try:
            exec_shell(cmd)
        except Exp, e:
            pass

        if self.is_master():
            self.objck_nolock(check_flag)
        else:
            dwarn("need run with master!")

    def _get_cluster_disk_info(self):
        cluster_disk_info = []
        node_disk_info = ()

        for h in self.config.cluster.keys():
            try:
                cmd = "python2.7 %s stat" % (self.config.uss_node)
                x, y = exec_remote(h, cmd, timeout=600, exectimeout=288000)
                if y:
                    print y
                for line in x.split('\n'):
                    if "cds" in line and "running" in line:
                        nid = line.split(' ')[2]
                        rate = int(line.split(' ')[4].split('%')[0])
                        node_disk_info = (h, nid, rate)
                        cluster_disk_info.append(node_disk_info)
            except Exception as e:
                derror("host:%s cmd:%s error" % (h, cmd))
                return []

        return sorted(cluster_disk_info, key=lambda item:item[2])

    def _get_fileid_from_logfile(self, logfile):
        try:
            cmd = "grep 'file ' %s | awk '{print $2}' | awk -F'[' '{print $1}'" % (logfile)
            out, err = exec_shell(cmd, p=True, need_return=True)
            return str(out.split('\n')[0])
        except Exception as e:
            raise Exp(e.errno, "get fileid fail from %s" % (logfile))

    def _get_replicas_info(self, logfile, idx):
        repnum = 0
        master = 0

        try:
            cmd = "grep '    chk\[%d]' %s" % (idx, logfile)
            out, err = exec_shell(cmd, p=True, need_return=True)
            chkinfo = str(out.split('\n')[0])
            repnum = int(chkinfo.split()[2])
            master = int(chkinfo.split()[6])
        except Exception as e:
            raise Exp(e.errno, "get replica info fail, i:%d" % (idx))

        cmd = "grep 'chk\[%d]' %s -A%d | grep -v 'chk\[%d]'" % (idx, logfile, repnum, idx)
        out, err = exec_shell(cmd, p=True, need_return=True)

        return out.split('\n')

    def _deal_file_for_chunks_move(self, filename, from_host, from_nid, to_host, to_nid, _count):
        succ_count = 0
        u = uuid.uuid1()
        tmp_file = "/tmp/%s.dump" % (str(u))
        chkid = ""

        try:
            cmd = "uss.stat %s -v > %s" % (filename, tmp_file)
            exec_shell(cmd, p=True)
        except Exception as e:
            #remove tmp log if exist
            if os.path.isfile(tmp_file):
                os.remove(tmp_file)
            return 0

        fileid = self._get_fileid_from_logfile(tmp_file)

        i = 0;
        while True: #parse tmp_file
            replicas_info = []
            src_find = 0
            permit = 1

            chkid = fileid + '[' + str(i) + ']'
            try:
                replicas_info = self._get_replicas_info(tmp_file, i)
            except Exception as e:
                break

            for replica_info in replicas_info:
                if len(replica_info) == 0:
                    continue

                if to_host in replica_info:
                    if from_host == to_host and from_nid != to_nid:
                        permit = 1
                    else:
                        permit = 0
                        break

            if permit == 0:
                i = i + 1
                dmsg("host %s already has one replica, not permited to move it" % to_host)
                continue

            for replica_info in replicas_info:
                if from_nid in replica_info:
                    src_find = 1
                    cmd = "%s/app/bin/uss.objmv --chkid %s -f %s -t %s" % (self.config.workdir, chkid, from_nid, to_nid)
                    try:
                        exec_shell(cmd)
                        dmsg("move chunk %s from %s to %s success!" % (chkid, from_nid, to_nid))

                        succ_count = succ_count + 1
                        if succ_count >= _count:
                            os.remove(tmp_file)
                            return succ_count
                    except Exception as e:
                        dwarn("move chunk %s from %s to %s fail!" % (chkid, from_nid, to_nid))
                        pass

            if src_find == 0:
                dmsg("not find %s in %s" % (from_nid, chkid))

            i = i + 1

        os.remove(tmp_file)
        return succ_count

    def _scan_dir_for_chunks_move(self, dir_path, from_host, from_nid, to_host, to_nid, _count):
        lines = ()
        succ_count = 0
        filename = ""
        abs_path = ""

        try:
            cmd = "uss.ls %s" % dir_path
            out, err = exec_shell(cmd, p=True, need_return=True, timeout=3600)
            lines = out.split('\n')
        except Exception as e:
            raise Exp(e.errno, "uss.ls %s error, errno %d" % (dir_path, e.errno))

        for line in lines:
            if (len(line) == 0):
                continue

            filename = line.split(' ')[-1]
            abs_path = os.path.join(dir_path, filename)

            if line[0] == 'd':
                #dir
                succ_count += self._scan_dir_for_chunks_move(abs_path, from_host, from_nid, to_host, to_nid, _count)
                if succ_count >= _count:
                    return succ_count
            elif line[0] == '-':
                #file
                dmsg("scan file : %s...\n" % abs_path)
                succ_count += self._deal_file_for_chunks_move(abs_path, from_host, from_nid, to_host, to_nid, _count)
                if succ_count >= _count:
                    return succ_count
            else:
                continue

        return succ_count

    def _chunk_balance(self, _times = 0):
        from_disks = {}
        succ_count = 0

        disk_info = self._get_cluster_disk_info()
        info_len = len(disk_info)
        if info_len == 0:
            return 0

        #disk_info sorted already, 0~100
        if disk_info[info_len-1][2] - disk_info[0][2] > 5:
            to_host = disk_info[0][0]
            to_nid = disk_info[0][1]
            min_rate = disk_info[0][2]

            i = info_len - 1
            last_rate = 0
            while True:
                from_host = disk_info[i][0]
                from_nid = disk_info[i][1]
                max_rate = disk_info[i][2]

                if i == (info_len -1) or last_rate == max_rate:
                    from_disks[from_nid] = from_host
                    last_rate = max_rate
                    i = i - 1
                    continue
                else:
                    break


            dmsg("chunk balance %d times begin, from %s to %s:%s count 16" % (_times, from_disks, str(to_host), str(to_nid)))
            for fnid in from_disks.keys():
                fhost = from_disks[fnid]
                succ_count = self._scan_dir_for_chunks_move("/", fhost, fnid, to_host, to_nid, 16)
                if (succ_count >= 16):
                    dmsg("chunk balance %d times finish, from %s to %s:%s count %d" % (_times, from_disks, str(to_host), str(to_nid), succ_count))
                    break;

            return 0
        else:
            dmsg("chunk already balanced.")
            return 1

    def chunkbalance(self):
        lfile = "/var/run/chunkbalance.lock"
        lock = lock_file(lfile)
        is_balanced = 0

        times = 1
        while True:
            if self.is_master():
                    is_balanced = self._chunk_balance(times)
                    if is_balanced:
                        break

                    times = times + 1
            else:
                dwarn("need run with master!")
                break

    def objck(self, check_flag=None):
        lfile = "/var/run/uss.objck.lock"
        lock = lock_file(lfile)

        if self.config.use_redis():
            self._objck_redis(check_flag)
        elif self.config.use_leveldb():
            self._objck_leveldb(check_flag)
        else:
            raise Exp(1, "unsupport")

    def ln_old(self, force=True):
        if not os.path.exists("/opt/fusionnas"):
            os.system("ln -sf /opt/sdfs /opt/fusionnas")
        
        link_path = "/usr/local/bin"

        try:
            cmd = "rm -rf %s/uss.*" % (link_path)
            exec_shell(cmd, p=False)
        except Exception as e:
            pass

        tasks = [("app/admin/node.py", "app/bin/uss.node"),
                 ("app/admin/cluster.py", "app/bin/uss.cluster"),
                 ("app/admin/minio.py", "app/bin/uss.minio")]
        for t in tasks:
            target = os.path.join(self.config.home, t[0])
            link_name = os.path.join(self.config.home, t[1])
            exec_shell("chmod +x %s" % (target), p=False)
            exec_shell("ln -sf %s %s" % (target, link_name), p=False)

        bin_path = os.path.join(self.config.home, 'app/bin')
        tasks = os.listdir(bin_path)
        for t in tasks:
            target = os.path.join(bin_path, t)
            link_name = os.path.join(link_path, t)
            link_name = link_name.replace("sdfs", "uss")
            #print link_name
            exec_shell("ln -sf %s %s" % (target, link_name), p=False)
        
        
    def ln(self, force=True):
        self.ln_old()
        
        link_path = "/usr/local/bin"

        try:
            cmd = "rm -rf %s/sdfs.*" % (link_path)
            exec_shell(cmd, p=False)
        except Exception as e:
            pass

        tasks = [("app/admin/node.py", "app/bin/sdfs.node"),
                 ("app/admin/sdfs.py", "app/bin/sdfs"),
                 ("app/admin/cluster.py", "app/bin/sdfs.cluster"),
                 ("app/admin/minio.py", "app/bin/sdfs.minio")]
        for t in tasks:
            target = os.path.join(self.config.home, t[0])
            link_name = os.path.join(self.config.home, t[1])
            exec_shell("chmod +x %s" % (target), p=False)
            exec_shell("ln -sf %s %s" % (target, link_name), p=False)

        bin_path = os.path.join(self.config.home, 'app/bin')
        tasks = os.listdir(bin_path)
        for t in tasks:
            target = os.path.join(bin_path, t)
            link_name = os.path.join(link_path, t)
            exec_shell("ln -sf %s %s" % (target, link_name), p=False)

    def _init_cds(self):
        cds = self.config.service["cds"]
        if (cds == None):
            return

        for i in cds:
            cmd = "mkdir -p %s/data/cds/%s" % (self.config.home, i)
            os.system(cmd)

    def _init_gateway(self, gateway):
        for i in gateway:
            try:
                #print (i, self.config.service)
                s = self.config.service[i]
                cmd = "mkdir -p %s/data/%s/0" % (self.config.home, i)
                os.system(cmd)
            except:
                pass

    
            
    def env_init(self):
        self.ln()
        install_init(self.config.home)

        os.system("iptables -F")
        os.system("mkdir -p %s/log" % (self.config.home))
        self._init_redis()
        self._init_cds()
        self._init_gateway(["nfs", "ftp"])
        self._start_redis()

    def _check_disk_available(self, dev, force=False):
        devs = dev_lsblks()
        if dev not in devs:
            raise Exp(1, "%s not found" % (dev))

        ms = dev_mountpoints(dev)
        if ms:
            raise Exp(1, "%s was mounted with %s" % (dev, ms))

        childs = dev_childs(dev)
        for x in childs:
            if x.startswith("/dev/mapper"):
                raise Exp(1, "%s was used with lvm %s" % (dev, x))
        if childs:
            if (not force):
                msg = "%s has parted %s, --force" % (dev, childs)
                raise Exp(1, msg)
            else:
                dev_clean(dev)

    def _check_fstab(self):
        cmd = "mount -a"
        exec_shell(cmd)

    def _get_release_info(self):
        distributor = ""
        version = 0.0

        cmd = "lsb_release -a | grep \"Distributor ID\" | awk '{print $3}'"
        out, err = exec_shell(cmd, p=False, need_return=True)
        distributor = out.split('\n')[0]

        cmd = "lsb_release -a | grep \"Release\" | awk '{print $2}' | cut -d'.' -f1-2"
        out, err = exec_shell(cmd, p=False, need_return=True)
        release = out.split('\n')[0]
        version = float(release)

        return (distributor, version)

    def __mkfs_and_mount(self, dev, idx, mountpoint):
        old_uuid = dev_uuid(dev)
        new_uuid = old_uuid
        dev_mkfs_ext4(dev)
        for i in range(10):
            new_uuid = dev_uuid(dev)
            if new_uuid != old_uuid:
                break
            time.sleep(1)

        dev_info = "UUID=%s %s" % (new_uuid, mountpoint)
        distributor, release = self._get_release_info()
        if distributor == "Ubuntu"  and release < 16.04: #nobootwait is no longer a valid option in Ubuntu 16.04
            dev_info = dev_info + " ext4 user_xattr,noatime,defaults,nobootwait 0 0"
        else:
            dev_info = dev_info + " ext4 user_xattr,noatime,defaults 0 0"
        cmds = []
        cmds.append('echo ' + dev_info + ' >> /etc/fstab')
        cmds.append('mkdir -p %s' % (mountpoint))
        cmds.append('mount -a')
        [exec_shell(cmd) for cmd in cmds]

    def _mkfs_and_mount(self, dev, idx, mountpoint):
        try:
            self.__mkfs_and_mount(dev, idx, mountpoint)
        except Exception as e:
            fstab_del_mount(mountpoint)
            raise

    def mond_init(self, idx):
        cmd = '%s --init -n %s' % (self.config.uss_mond, idx)
        exec_shell(cmd)

    def _get_new_idx(self, role):
        role_dir = os.path.join(self.config.workdir, role)
        try:
            idx = [int(x) for x in os.listdir(role_dir)]
        except OSError as e:
            if e.errno == errno.ENOENT:
                return 0

        for i in range(self.config.max_num):
            if i not in idx:
                return i

        err = "too many %s, max_num: %s" % (role, self.config.max_num)
        raise Exp(1, err)

    def deploy(self, src, dist, hosts):
        for h in hosts:
            put_remote(h, src, dist)

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
        cluster = {}
        for h in hosts:
            services = self._get_services(h)
            cluster.update({h: services})

        #self._add_check_cluster_conf(cluster)
        self.config.cluster = cluster
        dumpto = "/tmp/uss_cluster.conf"
        self.config.dump_cluster(dumpto=dumpto)
        self.deploy(dumpto, self.config.cluster_conf, hosts)

    def is_block(self, dev):
        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        try:
            if not stat.S_ISBLK(os.lstat(os.path.realpath(dev)).st_mode):
                return False
        except Exception, etier:
            return False

        return True

    def get_all_cdrom(self):
        cdrom = []

        if os.path.exists('/proc/sys/dev/cdrom/info'):
            with file('/proc/sys/dev/cdrom/info', 'rb') as cdrom_info:
                for line in cdrom_info.read().split('\n')[2:]:
                    m = re.match('drive name:\s+(\w+)', line)
                    if m is not None :
                        cdrom.append(m.group(1))

        return cdrom

    def get_all_parts(self):
        devs = []
        parts = []
        all_parts = {}

        cdrom = self.get_all_cdrom()

        with file('/proc/partitions', 'rb') as proc_partitions:
            for line in proc_partitions.read().split('\n')[2:]:
                fields = line.split()
                if len(fields) < 4:
                    continue
                name = fields[3].split('/')[-1]
                if name.startswith('dm') or name in cdrom or name.startswith('loop'):
                    continue
                m = re.match('(\D+)(\d+)', name)
                if m is None:
                    devs.append(name)
                else:
                    parts.append(name)

        for dev in devs:
            all_parts[dev] = []

        for part in parts:
            (d, p) = re.match('(\D+)(\d+)', part).group(1, 2)
            if d not in all_parts:
                all_parts[part] = []
            else:
                all_parts[d].append(part)

        return all_parts

    def is_part(self, part):
        if not part.startswith("/dev/"):
            part = "/dev/" + part

        part = part.split('/')[-1]
        all_parts = self.get_all_parts()

        for dev, parts in all_parts.iteritems():
            if part in parts:
                return True

        return False

    def _get_dev_type_num(self, dev):
        type_num = 1

        if not dev.startswith("/dev/"):
            dev = "/dev/" + dev
        if self.is_part(dev):
            dev = re.match('(\D+)(\d*)', dev).group(1)

        dev = os.path.realpath(dev)
        dev_name = dev.split('/')[-1]

        rotational = "/sys/block/"+dev_name+"/queue/rotational"
        if os.path.exists(rotational):
            cmd = "cat %s" % (rotational)
            type_num, err = exec_shell(cmd, p=False, need_return=True)
            type_num = type_num.strip()

        return type_num

    def _set_disk_tier(self, dev, mountpoint):
        tier_num = self._get_dev_type_num(dev)
        tier_file = os.path.join(mountpoint, 'tier')

        with open(tier_file, "w") as f:
            f.write("%s\n" % (tier_num))

    def disk_add(self, dev, role, idx=None, force=False):
        #UUID=xxx /sysy/yfs/cds/0 ext4 user_xattr,noatime,defaults 0 0
        idx_list = os.listdir(os.path.join(self.config.workdir, role))
        if idx is None:
            idx = self._get_new_idx(role)
        elif str(idx) in idx_list:
            dwarn("%s %s is already exist\n" % (role, idx))
            sys.exit(errno.EEXIST)

        self._check_disk_available(dev, force)
        self._check_fstab()
        mountpoint = "%s/%s/%s" %  (self.config.workdir, role, idx)
        self._mkfs_and_mount(dev, idx, mountpoint)
        self._set_disk_tier(dev, mountpoint)

        if os.path.isfile(self.config.cluster_conf):
            conf = ClusterConf(lock=True)
            self.config = conf.add_disk([str(idx)])
            #self._add_update_cluster_conf(self.config.cluster.keys())
            i = Instence(role, int(idx))
            i.start(self.ttyonly)

    def _disk_iswriteable(self, role, idx):
        role_path = os.path.join(self.config.workdir, role)
        idx_path = os.path.join(role_path, idx)

        try:
            tmp = idx_path + '/check_' + str(random.random())
            set_value(tmp, "test")
            os.unlink(tmp)
            return True
        except Exception, e:
            return False

    def _vip_start(self):
        if not os.path.isfile(self.config.vip_conf):
            print "vip service not configured, will exit!"
            return

        if check_process_exists("node.py vipstart") > 1:
            print "vip service is running!"
            return

        lfile = "/var/run/uss.vipstart.lock"
        lock = lock_file(lfile)

        pid = os.fork()
        if pid == 0:
            while True:
                try:
                    if self.is_master():
                        cmd = "python %s/app/admin/vip.py balance >> %s/log/vipbalance.log 2>&1" % (self.config.home, self.config.home)
                        exec_shell(cmd, p=False)
                    time.sleep(3)
                except Exception as e:
                    pass
        elif pid > 0:
            dmsg("vip service start ok!")
        else:
            derror("vip service start fail, fork error!")

    def _vip_stop(self):
        if not os.path.isfile(self.config.vip_conf):
            dwarn("node vip service not configured, please check it !")
            return

        lfile = "/var/run/uss.vipstop.lock"
        lock = lock_file(lfile)

        del_local_vips(self.config.vip_conf)
        cmd = "ps -ef | grep vipstart | grep -v grep | awk '{print $2}' | xargs -r kill -9 >/dev/null 2>&1"
        exec_shell(cmd, p=False)

        print "vip service stop ok!"

    def _get_vips(self, group):
        if not check_process_exists("vipstart"):
            raise Exp(errno.EPERM, "vip service is stopped!")

        local_vips =  get_local_vips(self.config.vip_conf, group)
        print local_vips

    def _set_vips(self, vips, mask):
        return set_vips(vips, mask)

    def _vip_isexist(self, vip):
        if not vip_isexist(vip):
            raise Exp(errno.ENOENT, "vip:%s not exist" % (vip))
        else:
            dmsg("vip %s exist!" % (vip))

    def _vip_del(self, vips, mask):
        for vip in vips.split(","):
            if vip == "":
                continue
            if vip_isexist(vip):
                vip_del(vip, mask)

    def _set_dns(self, name, dnsip, hosts):
        dns_rfc_zones = "/etc/named.rfc1912.zones"
        dns_rfc_zones_tmp = "%s.tmp" % (dns_rfc_zones)
        zone_file = "/var/named/%s.zone" % (name)
        zone_file_tmp = "%s.tmp" % (zone_file)

        #修改/etc/named.rfc1912.zones
        shutil.copyfile(dns_rfc_zones, dns_rfc_zones_tmp)
        fd = open(dns_rfc_zones_tmp, 'a')
        fd.write("\n")
        fd.write("zone \"%s\" IN {\n" % (name))
        fd.write("	type master;\n")
        fd.write("	file \"%s\";\n" % (zone_file))
        fd.write("};\n")
        fd.flush()
        fd.close()
        os.rename(dns_rfc_zones_tmp, dns_rfc_zones)

        #添加/var/named/目录下对应的域名文件
        fd = open(zone_file_tmp, 'w')
        fd.write("$TTL 1D\n")
        fd.write("@	IN SOA	ns1.%s.	root.localhost. (\n" % (name))
        fd.write("					0	; serial\n")
        fd.write("					1D	; refresh\n")
        fd.write("					1H	; retry\n")
        fd.write("					1W	; expire\n")
        fd.write("					3H )	; minimun\n")
        fd.write("	NS	ns1.%s.\n" % (name))
        fd.write("	A	%s\n" % (dnsip))
        for host in hosts.split(','):
            fd.write("www	IN	A	%s\n" % (host))

        fd.write("ns1	IN	A	%s\n" % (dnsip))
        fd.flush()
        fd.close()
        os.rename(zone_file_tmp, zone_file)
        
        #修改dns的ip地址
        cmd = "chown root:named %s ; grep '%s' /etc/resolv.conf > /dev/null || sed -i '1a nameserver %s' /etc/resolv.conf" % (zone_file, dnsip, dnsip)
        exec_shell(cmd, p=False)

    def _check_domain_isexist(self, name):
        try:
            cmd = """grep 'zone \"%s\"' /etc/named.rfc1912.zones""" % (name)
            exec_shell(cmd, p=False)
            return True
        except Exp, e:
            return False

    def _dns_conf(self, name, dnsip, hosts):
        dnsconf_tpl = "%s/etc/named.conf" % (self.config.home)
        if not os.path.exists(dnsconf_tpl):
            raise Exp(errno.ENOENT, "not find dns config file %s", dnsconf_tpl)
        
        if self._check_domain_isexist(name):
            raise Exp(errno.EPERM, "domain name %s already exists, please check it!", name)

        check_ip_valid(dnsip)
        for host in hosts.split(","):
            check_ip_valid(host)

        cmd = "cp -f %s /etc/named.conf" % (dnsconf_tpl)
        exec_shell(cmd, p=False)

        self._set_dns(name, dnsip, hosts)

        cmd = "service named restart"
        exec_shell(cmd, p=False)

        print "dns config ok!"

    def _dns_del(self, name):
        dns_conf = "/etc/named.rfc1912.zones"
        if not os.path.isfile(dns_conf):
            raise Exp(errno.EPERM, "not install dns service")

        cmd = "cp %s %s.tmp && sed -i '/zone \"%s/,+4d' %s.tmp && cp %s.tmp %s && sync" % (dns_conf, dns_conf, name, dns_conf, dns_conf, dns_conf)
        exec_shell(cmd, p=False)

        cmd = "rm -f /var/named/%s.zone && service named restart > /dev/null 2>&1" % (name)
        exec_shell(cmd, p=False)

        print "delete domain name : %s ok !" % (name)

    def disk_list(self):
        cmd = "lsblk -pr|grep disk"
        out, err = exec_shell(cmd, p=False, need_return=True)
        disk_info = out.split('\n')
        print 'disk_info:', disk_info
        disk_available = []
        for line in disk_info:
            if line:
                disk_available.append(line.split(' ')[0])

        disk_json={}
        print 'disk_available:', disk_available
        for dev in disk_available:
            cmd = "lsblk -pPno NAME,FSTYPE,SIZE,MOUNTPOINT,TYPE" + " " + dev + "|grep -v disk|grep -v lvm"
            out, err = exec_shell(cmd, p=False, need_return=True)
            list_out = out.split('\n')
            print 'list_out:', list_out
            disk = {dev:list_out}
            print 'disk:', disk
            disk_json.update(disk)

        print 'disk_json:', disk_json
        return disk_json

    def _ucarp_is_running(self, addr=""):
        try:
            if addr != "":
                cmd = """ps -ef | grep 'ucarp ' | grep 'interface'| grep '%s'|grep -v grep""" % (addr)
            else:
                cmd = """ps -ef | grep 'ucarp ' | grep 'interface'| grep -v grep"""

            out,err = exec_shell(cmd, p=False, need_return=True)
            if len(out) > 0:
                return 1
            else:
                return 0
        except Exception as e:
                if (e.errno == 1):
                    return 0
                else:
                     derror("check ucarp error, errno:%d" % (e.errno))
                     sys.exit(e.errno)

    def _ucarp_is_master_vip(self, addr):
        cmd = """ip addr show | grep %s >/dev/null""" % (addr)
        try:
            exec_shell(cmd, p=False, need_return=False)
        except Exception as e:
            return False

        return True


    def ucarp_status(self):
        ucarp_conf_file = os.path.join(self.config.home, "ucarp/ucarp.conf")

        if not os.path.exists(ucarp_conf_file):
            print "ucarp service not configured"
            sys.exit(0)

        if self._ucarp_is_running():
            addr = ""
            master_vip = ""
            with open(ucarp_conf_file, "r") as f:
                for line in f.readlines():
                    addr = line.split(' ')[1]
                    if self._ucarp_is_master_vip(addr):
                        if len(master_vip) > 0:
                            master_vip = master_vip + "," + addr
                        else:
                            master_vip = addr

            if len(master_vip) > 0:
                print "running, vip:%s" % (master_vip)
            else:
                print "running"
        else:
            print "stopped"

    def _ucarp_start_all(self):
        ucarp_conf_file = os.path.join(self.config.home, "ucarp/ucarp.conf")

        with open(ucarp_conf_file, "r") as f:
            for line in f.readlines():
                srcip = line.split(' ')[0]
                addr = line.split(' ')[1]
                dev = line.split(' ')[2]
                skew = line.split(' ')[3]
                vhid = addr.split('.')[3]

                if self._ucarp_is_running(addr):
                    continue

                vip_up_script = "/etc/vip-%s-up.sh" % (addr)
                vip_down_script = "/etc/vip-%s-down.sh" % (addr)

                cmd = """ucarp --interface=%s --srcip=%s --addr=%s --vhid=%s --pass=mdsmds --upscript=%s --downscript=%s -P -B -k %s""" \
                        % (dev, srcip, addr, vhid, vip_up_script, vip_down_script, skew)

                try:
                    exec_shell(cmd, p=False)
                except Exception as e:
                    dwarn("ucarp start vip:%s fail.\n", addr)
                    pass

    def ucarp_start(self):
        ucarp_conf_file = os.path.join(self.config.home, "ucarp/ucarp.conf")

        if not os.path.exists(ucarp_conf_file):
            print "ucarp service is not configured, just ignore it."
            sys.exit(0)

        pid = os.fork()
        if pid == 0: #child process
            while True:
                time.sleep(5)
                self._ucarp_start_all()
        else:
            self._ucarp_start_all()
            print "ucarp start ok !"

    def ucarp_stop(self):
        vip_down_script = "/etc/vip-down.sh"

        cmd = """ps -ef | grep -E "ucarp --|ucarpstart" | grep -v grep | awk '{print $2}'"""
        pids,err = exec_shell(cmd, p=False, need_return=True)
        if len(pids) == 0:
            print "ucarp service is not alive, just ignore it."
        else:
            pid_list = pids.split('\n')
            for pid in pid_list:
                if len(pid):
                    cmd = """kill -9 %s""" % pid
                    exec_shell(cmd, p=False)

            try:
                time.sleep(1)
                self._vip_down_all()
            except Exception as e:
                pass

            print "ucarp stop ok !"

    def _vip_down_all(self):
        cmd = "for f in `ls /etc/vip-*-down.sh`; do sh $f >/dev/null 2>&1; done"
        exec_shell(cmd, p=False)

    def ucarp_destroy(self):
        ucarp_conf_file = os.path.join(self.config.home, "ucarp/ucarp.conf")
        vip_up_script = "/etc/vip-*-up.sh"
        vip_down_script = "/etc/vip-*-down.sh"

        cmd = "rm -rf %s 2>/dev/null" % (ucarp_conf_file)
        exec_shell(cmd, p=False)

        self.ucarp_stop()

        try:
            time.sleep(1)
            self._vip_down_all()
        except Exception as e:
            pass

        cmd = "rm -rf %s %s 2>/dev/null" % (vip_up_script, vip_down_script)
        exec_shell(cmd, p=False)

        print "ucarp destroy ok !"

    def ucarp_conf(self, srcip, addr, skew):
        ucarp_conf_file = os.path.join(self.config.home, "ucarp/ucarp.conf")
        ucarp_conf_dir = os.path.dirname(ucarp_conf_file)

        if not os.path.isdir(ucarp_conf_dir):
            os.makedirs(ucarp_conf_dir)

        if not os.path.isfile(ucarp_conf_file):
            cmd = "touch %s" % (ucarp_conf_file)
            exec_shell(cmd, p=False)

        check_ip_valid(addr)

        #get dev by ip
        cmd = """ifconfig | grep -B1 '%s' | head -n1 | awk -F':' '{print $1}'""" % (srcip)
        out, err = exec_shell(cmd, p=False, need_return=True)
        dev = out.split("\n")[0]
        if len(dev) == 0:
            print "%s device not found." % srcip
            sys.exit(errno.ENOENT)

        #create vip-up.sh
        vip_up_script = "/etc/vip-%s-up.sh" % (addr)
        vip_up_ctx = '#!/bin/bash\nip addr add %s/24 dev %s\narping  -c 5 -U -I %s %s' % (addr, dev, dev, addr)
        cmd = """echo '%s' > %s.tmp && mv %s.tmp %s && chmod +x %s""" % (vip_up_ctx, vip_up_script, vip_up_script, vip_up_script, vip_up_script)
        exec_shell(cmd, p=False)

        #create vip-down.sh
        vip_down_script = "/etc/vip-%s-down.sh" % (addr)
        vip_down_ctx = """#!/bin/bash\nip addr del %s/24 dev %s""" % (addr, dev)
        cmd = """echo '%s' > %s.tmp && mv %s.tmp %s && chmod +x %s""" % (vip_down_ctx, vip_down_script, vip_down_script, vip_down_script, vip_down_script)
        exec_shell(cmd, p=False)

        #create ucarp.conf
        ucarp_conf_ctx = "%s %s %s %s" % (srcip, addr, dev, skew)
        cmd = """cp %s %s.tmp && echo '%s' >> %s.tmp && mv %s.tmp %s""" % (ucarp_conf_file, ucarp_conf_file, ucarp_conf_ctx, ucarp_conf_file, ucarp_conf_file, ucarp_conf_file)
        exec_shell(cmd, p=False)

        print "ucarp config ok!"

    def _useradd_local(self, username, password, useradd_args):
        retry = 0
        max_retry = 1

        while True:
            try:
                cmd = "useradd %s %s" % (username, useradd_args)
                exec_shell(cmd, need_return=True)
                break
            except Exception as e:
                if e.errno == errno.EBADF:
                    retry = retry + 1
                    if retry > max_retry:
                        raise Exp(e.errno, "add user %s fail, errmsg:(%s)" % (username, e.err))

                    self._userdel_local(username)
                    continue
                else:
                    raise Exp(e.errno, "add user %s fail, errmsg:(%s)" % (username, e.err))

        cmd = "echo \"%s:%s\" | chpasswd" % (username, password)
        exec_shell(cmd)

    def _useradd_samba(self, username, password):
        smbpasswd = "/usr/local/samba/bin/smbpasswd"

        cmd = "passwd=%s && (echo $passwd;echo $passwd) |%s -a %s -s >/dev/null 2>&1" % (password, smbpasswd, username)
        exec_shell(cmd)
        cmd = "%s -e %s >/dev/null 2>&1" % (smbpasswd, username)
        exec_shell(cmd)

    def useradd(self, username, password, uid="", gid=""):
        _args = ""

        if uid is not None:
            _args = "--uid %s" % (uid)

        if gid is not None:
            _args = "%s --gid %s" % (_args, gid)

        self._useradd_local(username, password, _args)
        self._useradd_samba(username, password)
        dmsg("user %s add ok !" % username)

    def _userdel_local(self, username):
        try:
            cmd = "userdel -r %s" % username
            exec_shell(cmd, need_return=True)
        except Exception as e:
            if e.errno == errno.ENXIO:
                pass
            else:
                raise Exp(e.errno, "del user %s fail, errmsg:(%s)" % (username, e.err))

    def _userdel_samba(self, username):
        smbpasswd = "/usr/local/samba/bin/smbpasswd"

        cmd = "%s -d %s > /dev/null 2>&1" % (smbpasswd, username)
        exec_shell(cmd)

    def userdel(self, username):
        self._userdel_local(username)
        self._userdel_samba(username)
        dmsg("user %s delete ok !" % username)

    def _usermod_local(self, username, password="", uid="", gid=""):
        _args = ""

        if uid is not None:
            _args = "--uid %s" % (uid)

        if gid is not None:
            _args = "%s --gid %s" % (_args, gid)

        if len(_args) > 0:
            cmd = "usermod %s %s" % (username, _args)
            exec_shell(cmd)

        if password is not None:
            cmd = "echo \"%s:%s\" | chpasswd" % (username, password)
            exec_shell(cmd)

    def _usermod_samba(self, username, password=""):
        if password is not None:
            self._useradd_samba(username, password)


    def usermod(self, username, password="", uid="", gid=""):
        self._usermod_local(username, password, uid, gid)
        self._usermod_samba(username, password)

    def group_exists(self, groupname="", gid=""):
        _args = ""
        both_check = False

        _groupname = "%s:" % (groupname)
        _gid = ":%s:" % (gid)

        if groupname is not None and gid is not None:
            both_check = True
        elif groupname is not None:
            _args = _groupname
        elif gid is not None:
            _args = _gid

        try:
            if both_check:
                cmd = "cat /etc/group | grep '%s' | grep '%s' >/dev/null 2>&1" % (_groupname, _gid)
            else:
                cmd = "cat /etc/group | grep '%s' >/dev/null 2>&1" % (_args)

            exec_shell(cmd, p=False, need_return=True)
            return True
        except Exception as e:
            if e.errno == errno.EPERM:
                return False
            else:
                raise Exp(e.errno, "group check %s %s fail, errmsg:(%s)" % (groupname, gid, e.err))

    def groupadd(self, groupname, gid=""):
        _args = ""

        if self.group_exists(groupname, gid):
            return

        if self.group_exists(groupname, None):
            raise Exp(errno.EEXIST, "group name:%s already exists" % (groupname))

        if self.group_exists(None, gid):
            raise Exp(errno.EEXIST, "group ID:%s already exists" % (gid))

        if gid is not None:
            _args = "--gid %s" % (gid)

        cmd = "groupadd %s %s" % (groupname, _args)
        exec_shell(cmd)

    def groupdel(self, groupname):
        try:
            cmd = "groupdel %s" % (groupname)
            exec_shell(cmd, need_return=True)
        except Exception as e:
            if e.errno == errno.ENXIO:
                pass
            else:
                raise Exp(e.errno, "del group %s fail, errmsg:(%s)" % (groupname, e.err))

    def worm(self, update=None):
        if self.is_master():
            if update:
                cmd = "uss.worm --update"
                exec_shell(cmd)
        else:
            dwarn("need run with master!")

    def raidlist(self):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.disk_list(True, False, False)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid list fail. %s" % str(e.args))
            exit(e.args[0])

    def raidadd(self, devs, force=False):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.raid_add(devs, force)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid delete fail. %s" % str(e.args))
            exit(e.args[0])

    def raiddel(self, devs, force=False):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.raid_del(devs, force)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid delete fail. %s" % str(e.args))
            exit(e.args[0])

    def raidcache(self, op, devs, policy):
        self.disk_manage = DiskManage(self)
        try:
            self.disk_manage.raid_cache(op, devs, policy)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid cache %s fail. %s" % (op, str(e.args)))
            exit(e.args[0])

    def raidlight(self, op, devs):
        self.disk_manage = DiskManage(self)
        try:
            self.disk_manage.disk_light(op, devs)
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid light %s fail. %s" % (op, str(e.args)))
            exit(e.args[0])

    def raidmiss(self):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.raid_miss()
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid miss fail. %s" % str(e.args))
            exit(e.args[0])

    def raidload(self, r=False):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.raid_load()
            if r:
                role = "cds"
                for idx in os.listdir(os.path.join(self.config.workdir, role)):
                    if not self._disk_iswriteable(role, idx):
                        cmd = "ps -ef | grep 'uss_cds -n %s$' | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null" % (idx)
                        cmd = cmd + "; sleep 1; umount %s/%s/%s -l" % (self.config.workdir, role, idx)
                        try:
                            exec_shell(cmd)
                        except Exception, e:
                            pass

                self.disk_manage.raid_flush()
                cmd = "mount -a"
                exec_shell(cmd, p=True)
                self.start(op="simple")
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid load fail. %s" % str(e.args))
            exit(e.args[0])

    def raidflush(self):
        self.disk_manage = DiskManage(self)
        try:
	    self.disk_manage.raid_flush()
        except Exp, e:
            derror(e.err)
            exit(e.errno)
        except Exception, e:
            derror("raid flush fail. %s" % str(e.args))
            exit(e.args[0])

    def etcd(self, state, hosts):
        proxy = False

        print (state, hosts)
        array = hosts.split(',')
        #if self.config.hostname not in array:
        #    proxy = True

        #cmd = "rm /etc/etcd/etcd.conf && mkdir -p %s/etcd && chown etcd.etcd %s/etcd" % (self.config.workdir, self.config.workdir)
        #os.system(cmd)
        Etcd_manage.etcd_set(array, state, proxy)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    subparsers = parser.add_subparsers()

    #node operation
    def _start(args):
        node = Node()
        node.start(args.role, args.service, args.op)
    parser_start = subparsers.add_parser('start', help='start services')
    parser_start.add_argument("--role", default=None, help="", choices=["cds", "mond"])
    parser_start.add_argument("--service", default=None, type=int, help="the id of service")
    parser_start.add_argument("--op", default="all", help="simple or all")
    parser_start.set_defaults(func=_start)

    def _stop(args):
        node = Node()
        node.stop(args.role, args.service)
    parser_stop = subparsers.add_parser('stop', help='stop services')
    parser_stop.add_argument("--role", default=None, help="", choices=["cds", "mond"])
    parser_stop.add_argument("--service", default=None, type=int, help="the id of service")
    parser_stop.set_defaults(func=_stop)

    def _objck(args):
        node = Node()
        node.objck(args.check)
    parser_objck = subparsers.add_parser('objck', help='recovery data')
    parser_objck.add_argument("--check", default=None, help="check  data")
    parser_objck.set_defaults(func=_objck)

    def _chk_balance(args):
        node = Node()
        node.chunkbalance()
    parser_chkbalance = subparsers.add_parser('chunkbalance', help='chunk balance')
    parser_chkbalance.set_defaults(func=_chk_balance)

    def _stat(args):
        node = Node()
        node._stat_show()
    parser_stat  = subparsers.add_parser('stat', help='the stat of node')
    parser_stat.set_defaults(func=_stat)

    def _mond_init(args):
        node = Node()
        node.mond_init(args.service)
    parser_mond_init = subparsers.add_parser('mond_init', help='make mond init')
    parser_mond_init.add_argument("--service", required=True, type=int, help="the idx of mond")
    parser_mond_init.set_defaults(func=_mond_init)

    def _env_init(args):
        node = Node()
        node.env_init()
    parser_env_init = subparsers.add_parser('env_init', help='make env init')
    parser_env_init.set_defaults(func=_env_init)


    def _vip_start(args):
        node = Node()
        node._vip_start()
    parser_vipstart= subparsers.add_parser('vipstart', help='start vip service')
    parser_vipstart.set_defaults(func=_vip_start)

    def _vip_stop(args):
        node = Node()
        node._vip_stop()
    parser_vipstop= subparsers.add_parser('vipstop', help='stop vip service')
    parser_vipstop.set_defaults(func=_vip_stop)

    def _get_vips(args):
        node = Node()
        node._get_vips(args.group)
    parser_getvips = subparsers.add_parser('getvip', help='get node vips')
    parser_getvips.add_argument("--group", required=True, help="vip group")
    parser_getvips.set_defaults(func=_get_vips)

    def _set_vips(args):
        node = Node()
        node._set_vips(args.vips, args.mask)
    parser_setvips = subparsers.add_parser('setvip', help='set node vips')
    parser_setvips.add_argument("--vips", required=True, help="vip list")
    parser_setvips.add_argument("--mask", required=True, help="vip mask")
    parser_setvips.set_defaults(func=_set_vips)

    def _vip_isexist(args):
        node = Node()
        node._vip_isexist(args.vip)
    parser_vip_isexist = subparsers.add_parser('vipisexist', help='check vip is exist or not')
    parser_vip_isexist.add_argument("--vip", required=True, help="vip address")
    parser_vip_isexist.set_defaults(func=_vip_isexist)

    def _vip_del(args):
        node = Node()
        node._vip_del(args.vip, args.mask)
    parser_vipdel = subparsers.add_parser('vipdel', help='del a vip')
    parser_vipdel.add_argument("--vip", required=True, help="vip address")
    parser_vipdel.add_argument("--mask", required=True, help="vip mask")
    parser_vipdel.set_defaults(func=_vip_del)

    def _dns_conf(args):
        node = Node()
        node._dns_conf(args.name, args.dnsip, args.hosts)
    parser_dnsconf = subparsers.add_parser('dnsconf', help='set node dns')
    parser_dnsconf.add_argument("--name", required=True, help="domain name")
    parser_dnsconf.add_argument("--dnsip", required=True, help="dns server ip address")
    parser_dnsconf.add_argument("--hosts", required=True, help="domain name corresponds to the IP addresses, split by ','")
    parser_dnsconf.set_defaults(func=_dns_conf)

    def _dns_del(args):
        node = Node()
        node._dns_del(args.name)
    parser_dnsdel = subparsers.add_parser('dnsdel', help='del a domain name on dns')
    parser_dnsdel.add_argument("--name", required=True, help="domain name")
    parser_dnsdel.set_defaults(func=_dns_del)

    def _start_service(args):
        node = Node()
        node._start_service()
    parser_start_srv = subparsers.add_parser('startsrv', help='start service')
    parser_start_srv.set_defaults(func=_start_service)

    #ucarp operation
    def _ucarp_conf(args):
        node = Node()
        node.ucarp_conf(args.srcip, args.addr, args.skew)
    parser_ucarp_conf = subparsers.add_parser('ucarpconf', help='ucarp configure')
    parser_ucarp_conf.add_argument("--srcip", required=True, help="host [real] ip")
    parser_ucarp_conf.add_argument("--addr", required=True, help="virture ip address")
    parser_ucarp_conf.add_argument("--skew", required=True, help="advertisement skew")
    parser_ucarp_conf.set_defaults(func=_ucarp_conf)

    def _ucarp_start(args):
        node = Node()
        node.ucarp_start()
    parser_ucarp_start = subparsers.add_parser('ucarpstart', help='start ucarp services')
    parser_ucarp_start.set_defaults(func=_ucarp_start)

    def _ucarp_status(args):
        node = Node()
        node.ucarp_status()
    parser_ucarp_status = subparsers.add_parser('ucarpstatus', help='list the node ucarp status')
    parser_ucarp_status.set_defaults(func=_ucarp_status)

    def _ucarp_stop(args):
        node = Node()
        node.ucarp_stop()
    parser_ucarp_stop = subparsers.add_parser('ucarpstop', help='stop ucarp services')
    parser_ucarp_stop.set_defaults(func=_ucarp_stop)

    def _ucarp_destroy(args):
        node = Node()
        node.ucarp_destroy()
    parser_ucarp_destroy = subparsers.add_parser('ucarpdestroy', help='destroy ucarp services')
    parser_ucarp_destroy.set_defaults(func=_ucarp_destroy)

    #user operation
    def _useradd(args):
        node = Node()
        node.useradd(args.name, args.password, args.uid, args.gid)
    parser_useradd = subparsers.add_parser('useradd', help='add a user')
    parser_useradd.add_argument("--name", required=True, help="user name")
    parser_useradd.add_argument("--password", required=True, help="user password")
    parser_useradd.add_argument("--uid", default=None, help="user ID")
    parser_useradd.add_argument("--gid", default=None, help="user group ID")
    parser_useradd.set_defaults(func=_useradd)

    def _userdel(args):
        node = Node()
        node.userdel(args.name)
    parser_userdel = subparsers.add_parser('userdel', help='del a user')
    parser_userdel.add_argument("--name", required=True, help="user name")
    parser_userdel.set_defaults(func=_userdel)

    def _usermod(args):
        node = Node()
        node.usermod(args.name, args.password, args.uid, args.gid)
    parser_usermod = subparsers.add_parser('usermod', help='mod user info')
    parser_usermod.add_argument("--name", required=True, help="user name")
    parser_usermod.add_argument("--password", default=None, help="user password")
    parser_usermod.add_argument("--uid", default=None, help="user ID")
    parser_usermod.add_argument("--gid", default=None, help="user group ID")
    parser_usermod.set_defaults(func=_usermod)

    #group operation
    def _groupadd(args):
        node = Node()
        node.groupadd(args.name, args.gid)
    parser_groupadd = subparsers.add_parser('groupadd', help='add a group')
    parser_groupadd.add_argument("--name", required=True, help="group name")
    parser_groupadd.add_argument("--gid", default=None, help="group ID")
    parser_groupadd.set_defaults(func=_groupadd)

    def _groupdel(args):
        node = Node()
        node.groupdel(args.name)
    parser_groupdel = subparsers.add_parser('groupdel', help='del a group')
    parser_groupdel.add_argument("--name", required=True, help="group name")
    parser_groupdel.set_defaults(func=_groupdel)

    def _wrom(args):
        node = Node()
        node.worm(args.update)
    parser_worm = subparsers.add_parser("worm", help="worm operation")
    parser_worm.add_argument("-u", "--update", action="store_true", dest="update",
                             help="update worm clock time")
    parser_worm.set_defaults(func=_wrom)

    #disk operation
    def _disk_add(args):
        config = Config(load_config=True)
        node = Node(config)
        node.disk_add(args.disk, args.role, args.service, args.force)
    parser_disk_add = subparsers.add_parser('disk_add', help='add disk')
    parser_disk_add.add_argument("--disk", required=True, help="disk")
    parser_disk_add.add_argument("--role", default="cds", help="[cds|mond]", choices=["cds", "mond"])
    parser_disk_add.add_argument("--service", default=None, type=int, help="the idx of service")
    parser_disk_add.add_argument("--force", action='store_true', help="force to do")
    parser_disk_add.set_defaults(func=_disk_add)

    #raid tools
    def _raidlist(args):
        node = Node()
        node.raidlist()
    parser_raidlist = subparsers.add_parser('raidlist', help='list the raid device')
    parser_raidlist.set_defaults(func=_raidlist)

    def _raidadd(args):
        node = Node()
        devs = args.devs.split(",")
        dmsg("add raid %s" % (devs))
        node.raidadd(devs, args.f)
    parser_raidadd = subparsers.add_parser('raidadd', help='create raid0')
    parser_raidadd.add_argument("--devs", default=None, help="inq1,inq2,...")
    parser_raidadd.add_argument("-f", action='store_true', default=False, help="force add")
    parser_raidadd.set_defaults(func=_raidadd)

    def _raiddel(args):
        node = Node()
        devs = args.devs.split(",")
        dmsg("del raid %s" % (devs))
        node.raiddel(devs, args.f)
    parser_raiddel = subparsers.add_parser('raiddel', help='delete raid0')
    parser_raiddel.add_argument("--devs", required=True, default=None, help="/dev/sdx,/dev/sdy,...")
    parser_raiddel.add_argument("-f", action='store_true', default=False, help="force add")
    parser_raiddel.set_defaults(func=_raiddel)

    def _raidcache(args):
        node = Node()
        devs = args.devs.split(",")
        node.raidcache(args.op, devs, args.policy)
    parser_raidcache = subparsers.add_parser('raidcache', help='raid cache operator')
    parser_raidcache.add_argument("--op", default=None, choices=["show", "set"])
    parser_raidcache.add_argument("--devs", required=True, default=None, help="/dev/sdx,/dev/sdy,...")
    parser_raidcache.add_argument("--policy", default=None, help="'[WT,WB,NORA,RA,ADRA,Cached,Direct,CachedBadBBU,NoCachedBadBBU,EnDskCache,DisDskCache]'"
                                                                                "\n'[Cached,R/W,Direct,EnDskCache,DisDskCache,smartpath]'")
    parser_raidcache.set_defaults(func=_raidcache)

    def _raidlight(args):
        node = Node()
        devs = args.devs.split(",") if args.devs is not None else []
        node.raidlight(args.op, devs)
    parser_raidlight = subparsers.add_parser('raidlight', help='raid light operator')
    parser_raidlight.add_argument("--op", default=None, choices=["start", "stop", "stat", "list"])
    parser_raidlight.add_argument("--devs", default=None, help="/dev/sdx,/dev/sdy,...")
    parser_raidlight.set_defaults(func=_raidlight)

    def _raidmiss(args):
        node = Node()
        node.raidmiss()
    parser_raidmiss = subparsers.add_parser('raidmiss', help='cleanup raid missing')
    parser_raidmiss.set_defaults(func=_raidmiss)

    def _raidload(args):
        node = Node()
        node.raidload(args.r)
    parser_raidload = subparsers.add_parser('raidload', help='reload Foreign stat device')
    parser_raidload.add_argument("-r", action='store_true', default=False, help="run disk service")
    parser_raidload.set_defaults(func=_raidload)

    def _raidflush(args):
        node = Node()
        node.raidflush()
    parser_raidflush = subparsers.add_parser('raidflush', help='flush raid cache')
    parser_raidflush.set_defaults(func=_raidflush)

    def _etcd(args):
        config = Config(load_config=True)
        node = Node(config)
        node.etcd(args.state, args.hosts)
    parser_etcd = subparsers.add_parser('etcd', help='etcd option')
    parser_etcd.add_argument("--state", required=True, help="new/exsting/proxy")
    parser_etcd.add_argument("--hosts", required=True, help="host1,host2")
    parser_etcd.set_defaults(func=_etcd)
    
    if (len(sys.argv) == 1):
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)
