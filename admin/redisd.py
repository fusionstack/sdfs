#!/usr/bin/env python2

import os
import sys
import socket
import time
import subprocess
import fcntl
import types
import errno
import getopt
import random
import threading
import traceback
import json
import urllib2
import etcd
import uuid

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
#import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from signal import SIGTERM

#sys.path.insert(0, os.path.split(os.path.realpath(__file__))[0] + "/../")

from daemon import Daemon
from config import Config
from utils import dmsg, derror, dwarn, _check_config, set_value, get_value, exec_shell, _str2dict, Exp

DISK_INSTENCE = 32
NODE_PORT = 100

class Handler(BaseHTTPRequestHandler):
    def __sendfile(self, path):
        size = os.path.getsize(path)
        dmsg("%s size %u" % (path, size))

        fd = open(path, 'r')
        left = size
        while (left > 0):
            if (left > 524288):
                cp = 524288
            else:
                cp = left

            #_dmsg("send %s %u"  % (path, cp))
            buf = fd.read(cp)
            self.wfile.write(buf)
            left -= cp

        fd.close()

        return

    def __get_io(self):
        config = Config()
        tmp = config.home + '/tmp/dump'
        name ='io.' + str(int(time.time()))
        tar = tmp +'/' +  name + '.' + 'tar.gz'
        os.system('mkdir -p %s/dump/io' % (tmp))
        cmd = 'mv %s/io %s/%s > /dev/null 2>&1' % (tmp, tmp, name)
        #_dmsg(cmd)
        os.system(cmd)
        cmd = 'tar czvf %s -C %s %s >> /dev/null && rm -rf %s' % (tar, tmp, name, tmp + '/' + name)
        #_dmsg(cmd)
        os.system(cmd)

        self.send_response(200)
        size = os.path.getsize(tar)
        self.send_header('Content-type', 'application/gzip')
        self.send_header('Content-length', str(size))
        self.end_headers()

        self.__sendfile(tar)

        cmd = 'rm %s' % (tar)
        #_dmsg(cmd)
        os.system(cmd)
        return

    def do_GET(self):
        if (self.path == '/io.tar.gz'):
            self.__get_io()
        else:
            self.send_response(404)
            self.end_headers()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

#class Redisd(Daemon):
class Redisd():
    def __init__(self, workdir, diskid, disk_idx):
        self.name = ""
        self.config = Config()
        self.uuid = str(uuid.uuid1())
        self.workdir = workdir
        self.diskid = diskid
        self.localid = int(self.workdir.split('/')[-1])
        self.disk_idx = disk_idx
        self.hostname = socket.gethostname()
        self.etcd = etcd.Client(host='127.0.0.1', port=2379)
        self.id = None
        self.lock = False
        self.volume = None
        self.running = True
        self.replica_info = None
        self.disk_check = time.time();
        self.__redis_pid(False);

        os.system('mkdir -p ' + self.workdir + '/run')

        self.__layout_local()
        self.__layout_global()

    def __layout_local(self):
        config = os.path.join(self.workdir, "config")

        #dmsg("load local layout")
        try:
            self.volume = get_value(config + "/volume")
            if (self.volume[-1] == '\n'):
                self.volume = self.volume[:-1]
            
            v = get_value(config + "/id")
            t = v[1:-1].split(',')
            self.id = (int(t[0]), int(t[1]))

            v = get_value(config + "/port")
            self.port_idx = int(v)
            self.port = str(self.config.redis_baseport + self.port_idx)
            return True
        except:
            self.port = None
            #dmsg("load local layout fail")
            return False

    def __layout_global(self):
        #dmsg("load global layout")
        if not self.volume:
            return False
        
        try:
            #print (self.volume + "/sharding")
            key = "/sdfs/volume/" + self.volume + "/sharding"
            #dmsg("etcd read " + key)
            self.sharding = int(self.etcd.read(key).value)
            key = "/sdfs/volume/" +self.volume + "/replica"
            #dmsg("etcd read " + key)
            self.replica = int(self.etcd.read(key).value)

            #dmsg("%s sharding %d replica %d" % (self.volume, self.sharding, self.replica))
            return True
        except etcd.EtcdKeyNotFound:
            return False

        
    def __init_redisconf(self, path, hostname):
        src = os.path.join(self.config.home, "etc/redis.conf.tpl")
        dist = os.path.join(path, "redis.conf")
        cmd = "cp " + src + " " + dist
        os.system(cmd)

        cmd = "mkdir -p " + self.workdir + "/data"
        os.system(cmd)
        #cmd = "chmod a+w " + self.workdir + "/data"
        #os.system(cmd)
        _check_config(dist, "port", " ", self.port, True)
        _check_config(dist, "dir", " ", self.workdir + "/data", True)
        _check_config(dist, "pidfile", " ", self.workdir + "/run/redis-server.pid", True)
        _check_config(dist, "logfile", " ", self.workdir + "/redis.log", True)
        _check_config(dist, "bind", " ", hostname, True)
        return True

    def __init_register_port(self, path, hostname):
        prefix = "/sdfs/redis/%s" % (hostname)

        if (os.path.exists(path + "/port")):
            self.port_idx = int(get_value(path + "/port"))
            self.port = str(self.config.redis_baseport + self.port_idx)
            return True
        
        idx = None
        for i in range(NODE_PORT):
            key = prefix + "/port/" + str(i)
            try:
                #dmsg("etcd write " + key)
                self.etcd.write(key, "", prevExist=False)
                idx = i
                break;
            except etcd.EtcdAlreadyExist:
                continue;

        if (idx == None):
            derror("no port space in " + prefix )
            return False
        else:
            dmsg("register port " + str(idx))
            set_value(path + "/port", str(idx))
            self.port_idx = idx
            self.port = str(self.config.redis_baseport + self.port_idx)
            return True
        
    def __init_register_new__(self, slot, replica, addr):
        key = "/sdfs/volume/%s/slot/%d/redis/%d" % (self.volume, slot, replica)
        dmsg("set (%s, %s)" % (key, addr))
        try:
            #dmsg("etcd write " + key)
            self.etcd.write(key, addr, prevExist=False)
            #dmsg("key %s succss" % (key))
            return True
        except etcd.EtcdAlreadyExist:
            dmsg("key %s exist" % (key))
            return False
    
    def __init_register_new(self, path, hostname, reg):
        if not self.__init_register_port(path, hostname):
            return False

        self.hostname = hostname
        addr = self.__redis_addr()

        if not self.__init_register_new__(reg[0], reg[1], addr):
            derror("register fail")
            return False

        dmsg("register %s %s " % (self.volume, str(reg)))
        set_value(path + "/id", str(reg))
        set_value(path + "/volume", self.volume)
        self.id = reg
        return True

    
    def __etcd_create(self, key, value):
        key = "/sdfs/volume/%s/slot/%d/%s" % (self.volume, self.id[0], key)
        #dmsg("etcd write " + key)
        self.etcd.write(key, value, prevExist=False)

    def __etcd_set(self, key, value):
        key = "/sdfs/volume/%s/slot/%d/%s" % (self.volume, self.id[0], key)
        #dmsg("etcd write " + key)
        self.etcd.write(key, value)
        
    def __etcd_get(self, key):
        key = "/sdfs/volume/%s/slot/%d/%s" % (self.volume, self.id[0], key)
        #dmsg("etcd read " + key)
        res = self.etcd.read(key)
        return res.value

    def __etcd_delete(self, key):
        key = "/sdfs/volume/%s/slot/%d/%s" % (self.volume, self.id[0], key)
        self.etcd.delete(key)
    
    def __etcd_update_dbversion(self):
        key = "/sdfs/volume/%s/slot/%d/%s" % (self.volume, self.id[0], "dbversion")
        #dmsg("etcd read " + key)
        res = self.etcd.read(key)
        idx = res.modifiedIndex
        version = int(res.value)

        try:
            #dmsg("etcd write " + key)
            res = self.etcd.write(key, version + 1, prevIndex=idx)
        except etcd.EtcdCompareFailed:
            derror("%s update %s fail" % (self.workdir, key))
            return -1

        return version + 1
        
    def redis_stop(self):
        try:
            os.kill(self.redis_pid, SIGTERM)
        except:
            pass
        
        pidfile = os.path.join(self.workdir, 'run/redis-server.pid')

        cmd = "rm %s > /dev/null 2>&1" % (pidfile)
        try:
            #pid = int(get_value(pidfile))
            #os.kill(pid, SIGTERM)
            os.system(cmd)
        except OSError:
            pass

    def __redis_pid(self, force):
        pidfile = os.path.join(self.workdir, 'run/redis-server.pid')

        while (1):
            if os.path.exists(pidfile):
                self.redis_pid = int(get_value(pidfile))
                break;
            else:
                if force:
                    time.sleep(0.1)
                else:
                    break;
                    
    def __redis_start(self):
        cmd = "redis-server %s/config/redis.conf" % (self.workdir)
        os.system(cmd)

        pidfile = os.path.join(self.workdir, 'run/redis-server.pid')

        self.__redis_pid(True);
        
    def __init_redis_master(self, config):
        try:
            self.__etcd_create("dbversion", "0")
            dmsg("create dbversion " + str(self.id))
        except etcd.EtcdAlreadyExist:
            dmsg("dbversion exist")
            pass

        #self.__etcd_create("master", self.hostname + " " + self.port)
        
        #cmd = "redis-server %s/redis.conf" % (config)
        #os.system(cmd)

        #cmd = "redis-cli -h %s -p %s set dbversion 0" % (self.hostname, self.port)
        #os.system(cmd)

        #self.redis_stop(self)

    def __init_redis_slave(self, config):
        retry = 0
        while (self.running):
            try:
                self.__etcd_get("dbversion")
                return True
            except:
                if (retry > 100):
                    dwarn("get dbversion " + str(self.id) + " fail")
                    return False
                time.sleep(0.1)
                retry = retry + 1

        #master = self.__etcd_get("master")
        #redis_config = config + "/redis.conf"
        #_check_config(redis_config, "slaveof", " ", master, True)

        #cmd = "redis-server %s/redis.conf" % (config)
        #os.system(cmd)
        
    def __init_redis(self, config):
        while (self.running):
            locked = self.__lock()
            if (locked):
                self.__init_redis_master(config)
                break;
            else:
                if (self.__init_redis_slave(config)):
                    break

        return True


    def __register_get(self):
        for i in range(self.sharding):
            for j in range(self.replica):
                key = "/sdfs/volume/%s/wait/%d/redis/%d.wait" % (self.volume, i, j)
                try:
                    #dmsg("etcd read " + key)
                    value = self.etcd.read(key).value
                    array = value.split(",")
                    if (array[0] == self.hostname and int(array[1]) == self.disk_idx):
                        dmsg("use %s" % (value))
                        self.etcd.delete(key)
                        return (i, j)
                except:
                    continue

        return None

    
    def init(self, volume):
        self.volume = volume

        if not self.__layout_global():
            dwarn("load global layout fail")
            return False
        
        res = self.__register_get()
        if (res == None):
            #dwarn("get register fail")
            return False

        dmsg("register %s to volume %s slot(%u, %u)" % (self.workdir, self.volume,
                                                        res[0], res[1]))
        
        path = self.workdir
        if (os.path.exists(path + "/config")) :
            derror("%s already inited" % (path))
            return False

        config_tmp = os.path.join(path, "config.tmp")
        config = os.path.join(path, "config")
        cmd = "mkdir -p " + config_tmp
        #dmsg(cmd)
        os.system(cmd)

        if not self.__init_register_new(config_tmp, socket.gethostname(), res):
            dwarn("init register fail")
            return False

        if not self.__init_redisconf(config_tmp, socket.gethostname()):
            dwarn("init redis.conf fail")
            return False
            
        if not self.__init_redis(config_tmp):
            dwarn("init redis fail")
            return False

        self.running = False
        #dmsg("running " + str(self.running))

        cmd = "mv " + config_tmp + " " + config
        os.system(cmd)

        self.__layout_local()
        return True

    def __redis_ismaster(self):
        #return self.lock.is_acquired

        #dmsg("%s lock %d, 1" % (self.workdir, self.lock.is_acquired))
        #dmsg("%s redis master check" % (self.workdir))
        if not self.lock.is_acquired:
            dwarn("%s not locked" % (self.workdir))
            return False

        cmd = "redis-cli -h %s -p %s info replication | grep role | awk -F ':' '{print $2}'" % (self.hostname, self.port)
        #dmsg(cmd)
        retry = 0
        while (self.running):
            try:
                res = exec_shell(cmd, need_return=True, p=False)
                break;
            except Exp, e:
                dwarn("cmd %s fail\n" % (cmd))
                time.sleep(1)
                retry += 1;
                continue;

        #dmsg("result " + str(res))
        #return self.lock.is_acquired
            
        r = res[0]

        #return True
        if (r.find("master") != -1):
            return True
        else:
            derror("%s lost master, role %s" % (self.workdir, r))
            self.lock.release()
            return False
        

    def __lock(self, background = True):
        name = "volume(%s,%d)" % (self.volume, self.id[0])

        if (self.lock):
            dmsg("%s lock %s %d already locked, uuid %s" % (self.workdir, name, self.lock.is_acquired, self.uuid))
        else:
            path = self.workdir
            #print name
            self.lock = etcd.Lock(self.etcd, name)

        ret = self.lock.acquire(blocking=False, lock_ttl=10)
        if (ret):
            dmsg("%s lock %s %d success, uuid %s" % (self.workdir, name, self.lock.is_acquired, self.uuid))
            pass
        else:
            derror("%s lock %s %d ,fail, uuid %s" % (self.workdir, name, self.lock.is_acquired, self.uuid))
            #pass
            #return self.lock.is_acquired
            
        self.running = True
 
        def __lock__(args):
            ctx = args
            while (ctx.running):
                try:
                    ret = ctx.lock.acquire(blocking=False,  lock_ttl=10)
                    if (ret):
                        #dmsg("%s run as master %s" % (self.workdir, name))
                        pass
                    else:
                        #dmsg("%s run as slave %s" % (self.workdir, name))
                        pass

                except etcd.EtcdException:
                    derror(self.workdir + " etcd error fail, acquire:" + str(self.lock.is_acquired))
                    #print(str(etcd.EtcdException))
                    
                time.sleep(3)

            ctx.lock.release()

        if (background):
            self.lockthread = threading.Thread(target=__lock__, args=[self])
            self.lockthread.start()
        return self.lock.is_acquired

    def removed(self):
        try:
            key = "/sdfs/volume/" + self.volume + "/sharding"
            #dmsg("etcd read " + key)
            sharding = int(self.etcd.read(key).value)
            key = "/sdfs/volume/" + self.volume + "/replica"
            #dmsg("etcd read " + key)
            replica = int(self.etcd.read(key).value)

        except etcd.EtcdKeyNotFound:
            dmsg("volume %s removed, exiting.." % (self.volume))
            self.__exit()
            return True

        return False

    def __exit(self):
        self.running = False

        self.remove()

        try:
            dmsg("%s waiting lock thread exiting ..." % (self.workdir))
            self.lockthread.join()
            dmsg("lock thread exited")
        except AttributeError:
            pass

        try:
            dmsg("%s waiting mainloop thread exiting ..." % (self.workdir))
            self.loop.join()
            dmsg("%s mainloop thread exited" % (self.workdir))
        except AttributeError:
            pass

        self.redis_stop()

        removedir = self.workdir + "/../removed"
        cmd = "mkdir -p " + removedir
        print cmd
        os.system(cmd)
        cmd = "mv %s %s/%d.%s" % (self.workdir, removedir, self.localid, str(uuid.uuid1()))
        print cmd
        os.system(cmd)

        dmsg("%s exited" % (self.workdir))
        
    def __redis_dbversion(self):
        cmd = "redis-cli -h %s -p %s get dbversion" % (socket.gethostname(), self.port)

        retry = 0
        while (self.running):
            try:
                res = exec_shell(cmd, need_return=True)
            except:
                if (retry > 10):
                    self.running = False
                    exit(1)
                time.sleep(1)
                retry += 1;
                continue;

            r = res[0][:-1]
            if (len(r) == 0):
                return 0
            
            try:
                version = int(r)
            except ValueError:
                derror(r)
                time.sleep(1)
                continue;

            return int(r)

    def __get_replica_info(self):
        cmd = "redis-cli -h %s -p %s info replication" % (self.hostname, self.port)

        while (1):
            try:
                res = exec_shell(cmd, p=False, need_return=True)
            except Exp as e:
                derror(str(Exp))
                time.sleep(1)
                continue;

            break;
            
        d = _str2dict(res[0])
        return d

    def __wait_sync(self):
        dbversion_local = self.__redis_dbversion()

        dbversion_etcd = int(self.__etcd_get("dbversion"))

        blocklist = "blocklist/" + self.hostname + ":" + self.port
        blocked = None
        try:
            blocked = self.__etcd_get(blocklist)
        except etcd.EtcdKeyNotFound:
            pass

        if (dbversion_etcd == dbversion_local or not blocked):
            return

        derror("dbversion not equal local %d remote %d"
               % (dbversion_local, dbversion_etcd))
        
        self.__etcd_set(blocklist, "blocked")
        master = self.__run_slave__()

        while (1):
            info = self.__get_replica_info()
            if (info['master_repl_offset'] == info['slave_repl_offset']):
                break;
        
        dmsg("sync finish from master %s, remove block %s\n" %s (master, blocklist))
        self.__etcd_delete(blocklist)

    def __update_dbversion(self):
        dbversion = self.__etcd_update_dbversion()
        if (dbversion == -1):
            dwarn("%s master fail, update dbversion\n" % (self.workdir))
            return

        dmsg("update dbversion to %d" % (dbversion))
        cmd = "redis-cli -h %s -p %s set dbversion %d" % (self.hostname, self.port, dbversion)
        os.system(cmd)

    def __replica_changed(self):
        retval = False
        
        replica_info = self.__get_replica_info()
        if (self.replica_info == None):
            retval = False
        elif (self.replica_info['connected_slaves'] != replica_info['connected_slaves']):
            dmsg("replica changed %s -> %s" % (self.replica_info['connected_slaves'],
                                               replica_info['connected_slaves']))
            retval = True
        else:
            retval = False

        self.replica_info = replica_info
        #dmsg("retval: " + str(retval))
        return retval

    def __disk_check(self):
        now = time.time()
        if (now - self.disk_check < 3):
            return

        path = self.workdir + "/disk_check"

        try:
            _set_value(path, "disk_check");
        except:
            derror("write %s fail" % (path))
            self.redis_stop()

    def __heath_check(self):
        now = time.time()
        if (now - self.last_check < (60 * 10)):
            return;

        self.last_check = now

        dmsg("health check begin")
        def __health__(args):
            ctx = args
            cmd = "/opt/sdfs/app/bin/sdfs.health -s /sdfs/volume/%s/slot/%d  > /opt/sdfs/log/health_%s_%d.log 2>&1" % (ctx.volume, ctx.sharding, ctx.volume, ctx.sharding)
            dmsg(cmd)
            os.system(cmd)
        
        self.health_thread = threading.Thread(target=__health__, args=[self])
        self.health_thread.start()
            
    def __run_master(self):
        #dwarn("%s master %s fail\n" % (self.workdir, master))
        #dmsg("%s lock %d, 4" % (self.workdir, self.lock.is_acquired))
        dmsg("%s %s %s run as master" % (self.workdir, self.hostname, self.port))
        cmd = "redis-cli -h %s -p %s SLAVEOF NO ONE" % (self.hostname, self.port)
        os.system(cmd)
        self.last_check = time.time()

        self.__update_dbversion()
        self.__etcd_set("master", self.hostname + " " + self.port)

        while (self.running):
            if (not self.__redis_ismaster()):
                dwarn("%s master fail\n" % (self.workdir))
                break;
            else:
                if (self.__replica_changed()):
                    self.__update_dbversion()

                self.__heath_check()
            time.sleep(0.5)
                
    def __set_slave__(self):
        retry = 0

        while (1):
            try:
                master = self.__etcd_get("master")
            except etcd.EtcdKeyNotFound:
                dwarn("master not found, retry %d" % retry)
                retry = retry + 1
                if (retry > 5):
                    break;
                else:
                    time.sleep(1)
                    continue
                
            dmsg("%s get master %s" % (self.workdir, master))
            cmd = "redis-cli -h %s -p %s SLAVEOF %s" % (self.hostname, self.port, master)
            os.system(cmd)
            return master

    def __run_slave(self):
        dmsg("%s run as slave" % (self.workdir))
        master = self.__set_slave__()

        if (master == None):
            dmsg("set slave fail")
            return
        
        while (self.running):
            if (self.lock.is_acquired):
                dwarn("%s master %s fail\n" % (self.workdir, master))
                break;
            else:
                time.sleep(1)

    def __redis_addr(self):
        return self.hostname + " " + self.port + " " + self.diskid

    def __watch_start(self):
        def __watch__(args):
            ctx = args
            last_scan = time.time()
            while (ctx.running):
                if (time.time() - last_scan < 10):
                    time.sleep(1)
                    continue

                last_scan = time.time()
                cmd = "ps aux | grep redis-server | grep %s | wc -l" % (self.port)
                #dmsg("instence check %s:%s" % (ctx.name, cmd))
                dmsg("instence check %s" % (ctx.name))
                res = exec_shell(cmd, p=False, need_return=True)
                c = int(res[0])
                if (c == 0):
                    derror(self.name + " stopped, try to start")
                    ctx.__redis_start();
                
        
        self.watch_thread = threading.Thread(target=__watch__, args=[self])
        self.watch_thread.start()
                
    def run(self):
        key = "redis/%s" % (self.id[1])

        addr = self.__redis_addr()
        try:
            value = self.__etcd_get(key)
        except etcd.EtcdKeyNotFound:
            dmsg("%s:(%s) not exist" % (key, addr))
            exit(1)

        if (value != addr):
            dmsg("server replaced %s -> %s" % (addr, value))
            exit(1)

        self.__redis_start()
        self.__wait_sync()

        self.__lock()

        self.__watch_start()

        self.name = "%s %s/slot/%d" % (self.workdir, self.volume, self.sharding)
        
        #dmsg("%s lock %d, 0" % (self.workdir, self.lock.is_acquired))
        while (self.running):
            if (self.lock.is_acquired):
                self.__run_master()
            else:
                self.__run_slave()

    def status(self):
        cmd = "redis-cli -h %s -p %s info replication" % (self.hostname, self.port)

        try:
            (out, err) = exec_shell(cmd, p=False, need_return=True)
            return out
        except:
            return None

    def remove(self):
        key = "redis/%s" % (self.id[1])
        try:
            self.__etcd_delete(key)
        except etcd.EtcdKeyNotFound:
            pass
        
    def stop(self):
        self.redis_stop()
        #super(Redisd, self).stop()

    def start_loop(self):
        def __loop__(args):
            ctx = args
            ctx.run()
        
        self.loop = threading.Thread(target=__loop__, args=[self])
        self.loop.start()
        
class RedisDisk(Daemon):
    def __init__(self, workdir):
        self.config = Config()
        self.workdir = workdir
        self.localid = int(self.workdir.split('/')[-1])
        self.hostname = socket.gethostname()
        self.etcd = etcd.Client(host='127.0.0.1', port=2379)
        self.id = None
        self.running = True
        self.replica_info = None
        self.disk_check = time.time()
        self.instence = []

        diskid = os.path.join(self.workdir, "inited")
        try:
            self.diskid = get_value(diskid)
            #dmsg("diskid %s" % (self.diskid))
        except:
            self.diskid = None
            #dmsg("diskid not exist")
            pass

        os.system('mkdir -p ' + self.config.home + '/log')
        os.system('mkdir -p ' + self.workdir + '/run')
        pidfile = os.path.join(self.workdir, 'run/redisd.pid')
        log = self.config.home + '/log/redis_disk_%d.log' % (self.localid)
        os.system('touch ' + log)

        for i in range(DISK_INSTENCE):
            dir =  "%s/%d" % (self.workdir, i)
            if (os.path.exists(dir + "/config")):
                redis = Redisd(dir, self.diskid, self.localid)
                self.instence.append(redis)
        
        super(RedisDisk, self).__init__(pidfile, '/dev/null', log, log, self.workdir)
 
        self.__update_instence()
       
    def init(self):
        path = self.workdir
        if (os.path.exists(path + "/inited")) :
            derror("%s already inited" % (path))
            return False

        self.diskid = str(uuid.uuid1())
        set_value(path + "/inited", self.diskid)
        self.running = False
        return True

    def run(self):
        #dmsg("begin service")
        
        for i in self.instence:
            i.start_loop()

        #dmsg("begin service1")

        key = "/sdfs/redis/%s/disk/%d/trigger" % (self.hostname, self.localid)
        index = 0
        while (1):
            dmsg("%s instence %u" % (self.workdir, len(self.instence)))

            try:
                dmsg("etcd watch " + key)
                res = self.etcd.watch(key, index, timeout=1000)
            except etcd.EtcdWatchTimedOut:
                #dmsg("watch timeout");
                continue
            except etcd.EtcdEventIndexCleared:
                derror("watch outdated");
                res = self.etcd.read(key)
                
            dmsg("watch %s return, value %s, idx %u:%u" % (key, res.value, res.etcd_index, index))
            if (res.value == "0"):
                index = res.etcd_index + 1
                continue
            else:
                #dmsg("etcd write " + key)
                self.etcd.write(key, "0")
                #dmsg("etcd read " + key)
                res = self.etcd.read(key)
                index = res.etcd_index + 1
            #except:
            #    derror("watch error")
            #    continue
            
            if not self.running:
                break

            #time.sleep(3)
            self.__check_volume()
            self.__check_remove()

        dmsg("stoped")

    def __update_instence(self):
        key = "/sdfs/redis/%s/disk/%d/instence" % (self.hostname, self.localid)

        #dmsg("etcd write " + key)
        self.etcd.write(key, str(len(self.instence)))

        key = "/sdfs/redis/%s/disk/%d/trigger" % (self.hostname, self.localid)
        #dmsg("etcd write " + key)
        self.etcd.write(key, "0")
        
    def __check_remove(self):
        for i in self.instence:
            if (i.removed()):
                self.instence.remove(i)

        self.__update_instence()
                
    def __check_volume(self):
        try:
            key = "/sdfs/volume"
            #dmsg("etcd read " + key)
            r = self.etcd.read(key)._children
            lst = []
            for i in r:
                lst.append(i["key"].split("/")[-1])
        except etcd.EtcdKeyNotFound:
            return False

        for i in lst:
            self.__check_volume__(i)

        return True
        #dmsg(str(lst))

    def __check_volume__(self, volume):
        #print volume
        try:
            key = "/sdfs/volume/" + volume + "/sharding"
            #dmsg("etcd read " + key)
            sharding = int(self.etcd.read(key).value)
            key = "/sdfs/volume/" + volume + "/replica"
            #dmsg("etcd read " + key)
            replica = int(self.etcd.read(key).value)

            #dmsg("%s sharding %d replica %d" % (volume, sharding, replica))
        except etcd.EtcdKeyNotFound:
            return False

        lst = []
        for i in range(sharding):
            slot = "/sdfs/volume/%s/slot/%d/redis" % (volume, i)
            try:
                #dmsg("etcd read " + slot)                
                r = self.etcd.read(slot)._children
                for i in r:
                    lst.append(i["key"])
            except etcd.EtcdKeyNotFound:
                pass

        #print "sharing + replica :" + str(lst)

        count = sharding * replica - len(lst)
        if (count == 0):
            #dmsg("%s registed" % (volume))
            return False
        else:
            dmsg("%s need registed" % (volume))

        for i in range(count):
            self.__register(volume)

    def __register(self, volume):
        workdir = None
        for i in range(DISK_INSTENCE):
            dir =  "%s/%d" % (self.workdir, i)
            if (os.path.exists(dir + "/config")):
                continue

            cmd = "mkdir " + dir
            os.system(cmd)
            workdir = dir
            break;

        if not workdir:
            return False

        redis = Redisd(workdir, self.diskid, self.localid)
        if not redis.init(volume):
            #dwarn("register %s to %s fail\n" % (workdir, volume))
            return False
        
        #redis = Redisd(workdir, self.diskid, self.localid)
        #dmsg("register %s to volume %s slot(%u, %u)" % (workdir, volume)
        redis.start_loop()
        self.instence.append(redis)

        self.__update_instence()

        return True

    #def stop(self):
    #    dmsg("stop redis")

    def stop(self):
        for i in self.instence:
            i.redis_stop()
        
        super(RedisDisk, self).stop()

    def status(self):
        s = []
        for i in self.instence:
            s.append(i.status())

        return s
    
def usage():
    print ("usage:")
    print (sys.argv[0] + " --init <workdir>")
    print (sys.argv[0] + " --start <workdir>")
    print (sys.argv[0] + " --stop <workdir>")
    print (sys.argv[0] + " --remove <workdir>")
    #print (sys.argv[0] + " --test")
    print (sys.argv[0])

def main():
    op = ''
    ext = None
    try:
        opts, args = getopt.getopt(
                sys.argv[1:], 
            'h', ['start', 'stop', 'help', 'test', 'init', 'status', 'remove', 'restart']
                )
    except getopt.GetoptError, err:
        print str(err)
        usage()

    redisdisk = RedisDisk(sys.argv[2])
    for o, a in opts:
        if o in ('--help'):
            usage()
            exit(0)
        elif (o == '--start'):
            op = o
            redisdisk.start()
        elif (o == '--stop'):
            op = o
            redisdisk.stop()
        elif (o == '--init'):
            op = o
            redisdisk.init()
        elif (o == '--test'):
            redisdisk.run()
        elif (o == '--status'):
            res = redisdisk.status()
            if (res):
                for i in res:
                    print i
            else:
                print "offline"
        elif (o == '--remove'):
            redisdisk.remove()
        elif (o == '--restart'):
            redisdisk.restart()
        else:
            assert False, 'oops, unhandled option: %s, -h for help' % o
            exit(1)

if __name__ == '__main__':
    if (len(sys.argv) == 1):
        usage()
    else:
        main()
