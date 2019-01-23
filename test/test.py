#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import argparse
import errno
import os
import sys
import time
import random
import threading
import uuid
import socket

admin = os.path.abspath(os.path.split(os.path.realpath(__file__))[0] + '/../admin')
sys.path.insert(0, admin)

from utils import exec_shell, Exp, dwarn, dmsg, derror, kill9_self
from config import Config 
from instence import Instence
from node import Node
from cluster import Cluster

CUR_PATH = os.path.abspath(os.path.split(os.path.realpath(__file__))[0])
TEST_PATH = "/opt/sdfs"
INSTALL_PATH = "/opt/sdfs"
LOG_PATH = "/tmp/sdfs_test"
CDS = 7
MDS = 1
RUNNING = 0
OBJMVING = 0

def fail_exit(msg):
    derror(msg)
    os.system('for pid in `ps -ef | grep test_list.py | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('for pid in `ps -ef | grep test.py | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('for pid in `ps -ef | grep "health" | grep -v grep | cut -c 9-15`; do kill -9 $pid; done')
    os.system('kill -9 ' + str(os.getpid()))


def health(node):
    retry = 0
    retry_max = 10
    while (1):
        try:
            cmd = "%s/app/bin/sdfs.health -s all >> %s/recovery.log 2>&1" % (TEST_PATH, LOG_PATH)
            (out, err) = exec_shell(cmd, p=True, need_return=True)
            break
        except Exp, e:
            ret = e.errno
            if (ret in [errno.EAGAIN, errno.EBUSY, errno.ENONET]):
                time.sleep(1)
                dwarn("health fail, %s, retry" % (e))
                continue
            else:
                if (retry > retry_max):
                    raise Exp(ret, "_scan fail: ret: %d, %s" % (ret, e))
                else:
                    time.sleep(1)
                    retry = retry + 1

def cds_check(i):
    retry = 0
    retry_max = 3
    while True:
        if i.running():
            break
        if retry > retry_max:
            fail_exit("cds %s not runing" % (str(i)))
            raise Exp("%s %s check fail" % (i.role, i.service), 1)
        retry = retry + 1
        time.sleep(1)

def _fail_simulate_loop():
    config = Config(TEST_PATH)
    node = Node(config)
    instences = []
    for i in range(CDS):
        instences.append(Instence('cds', i, config))

    rand = random.randint(0, CDS)
    fail_num = 0
    global RUNNING
    while RUNNING:
        for i in instences:
            cds_check(i)

        i = (rand + fail_num) % CDS
        dmsg("simulate fail[%s], will stop cds[%s]" % (fail_num, i))
        instences[i].stop()
        time.sleep(10) #wait rpc timeout
        health(node)
        instences[i].start()
        fail_num = fail_num + 1

def fail_simulate_loop():
    try:
        _fail_simulate_loop()
    except Exp, e:
        derror(e)
        kill9_self()
        exit(e.errno)

def fail_simulate():
    #time.sleep(10)
    t = threading.Thread(target=fail_simulate_loop)#, args = self)
    t.start()

def _get_chkid(filename):
    cmd = "sdfs.stat %s -v | grep 'file ' | awk '{print $2}' | cut -d '[' -f 1" % (filename)
    #out, err = exec_shell(cmd, p=False, need_return=True)
    out, err = exec_shell(cmd, need_return=True)
    return out.split('\n')[0]

def _get_src_nid(filename):
    master_node = 0
    net_info = ""

    while True:
        cmd = "sdfs.stat %s -v | grep master | awk '{print $7}'" % (filename)
        out, err = exec_shell(cmd, p=False, need_return=True)
        if len(out.split('\n')[0]) != 0:
            master_node = int(out.split('\n')[0])
            break

    dmsg("++++++++++++++filename:%s, master:%d+++++++++++++++" % (filename, master_node))
    if master_node == 0:
        cmd = "sdfs.stat %s -v | grep available | awk '{print $1,$2}' | tail -n1" % (filename)
    else:
        cmd = "sdfs.stat %s -v | grep available | awk '{print $1,$2}' | head -n1" % (filename)

    while True:
        out, err = exec_shell(cmd, p=False, need_return=True)
        if len(out.split('\n')[0]) != 0:
            net_info = out.split('\n')[0]
            break

    nid = net_info.split(" ")[1].split("(")[1].split(")")[0]

    return nid

def _get_dest_nid(filename):
    replica_nids = []
    cluster_nids = []
    dest_nid = ""

    cmd = "sdfs.stat %s -v | grep net |awk '{print $2}'| cut -d '(' -f2 | cut -d ')' -f 1" % (filename)
    out, err = exec_shell(cmd, p=False, need_return=True)
    replica_nids = out.split('\n')
    if replica_nids[len(replica_nids) - 1] == "":
        replica_nids.pop() #delete the last '\n'

    print "replica nid:%s" % (replica_nids)

    cmd = "sdfs.cluster list | grep cds | grep running | awk '{print $3}'"
    out, err = exec_shell(cmd, p=False, need_return=True)
    cluster_nids = out.split('\n')
    if cluster_nids[len(cluster_nids) - 1] == "":
        cluster_nids.pop() #delete the last '\n'

    print "cluster nid:%s" % (cluster_nids)
    for nid in cluster_nids:
        if nid not in replica_nids:
            dest_nid = nid
            break

    return dest_nid

def test_uss_objmv(filename):
    global OBJMVING
    from_nid = ""
    to_nid = ""
    chkid = ""
    succ_count = 0
    total_move_count = 10

    chkid = _get_chkid(filename)
    if len(chkid) == 0:
        OBJMVING = 0
        return

    while succ_count < total_move_count:
        try:
            dmsg("\n--------------------before objmv, stat info-------------------------")
            cmd = "sdfs.stat %s -v" % (filename)
            out, err = exec_shell(cmd, need_return=True)
            dmsg(out)
            dmsg("--------------------------------------------------------------------\n")

            from_nid = _get_src_nid(filename)
            to_nid = _get_dest_nid(filename)
            cmd = "sdfs.objmv --chkid %s[0] -f %s -t %s" % (chkid, from_nid, to_nid)
            exec_shell(cmd)
            succ_count = succ_count + 1
            dmsg("=====object moved count %d, left %d====n" % (succ_count, total_move_count - succ_count))

            dmsg("\n--------------------after objmv, stat info-------------------------")
            cmd = "sdfs.stat %s -v" % (filename)
            out, err = exec_shell(cmd, need_return=True)
            dmsg(out)
            dmsg("--------------------------------------------------------------------\n")
        except Exp,  e:
            if e.errno == errno.ENOENT or e.errno == errno.ENONET or e.errno == errno.ETIMEDOUT or e.errno == errno.EPERM:
                pass
            else:
                derror("objmv from %s to %s fail. errno:%d" %(from_nid, to_nid, e.errno))
                out, err = exec_shell("sdfs.cluster list", need_return=True)
                dmsg(out)

                cmd = "sdfs.stat %s -v" % (filename)
                out, err = exec_shell(cmd, need_return=True)
                dmsg(out)
                OBJMVING = 0
                kill9_self()

    OBJMVING = 0

def _test_uss_write(filename):
    retry = 0

    content = str(uuid.uuid1())
    cmd = "sdfs.write '%s' %s" % (content, filename)

    retry = 0
    while (1):
        try:
            exec_shell(cmd)
        except Exp, e:
            if (e.errno == errno.EIO or retry > 300):
                raise

            time.sleep(1)
            retry = retry + 1
            continue
        break

    while True:
        cmd = "sdfs.cat %s" % (filename)
        out, err = exec_shell(cmd, need_return = True)
        result = out.split('\n')[0]
        if len(result) == 0:
            if retry < 10:
                retry = retry + 1
                time.sleep(1)
                continue
            else:
                raise Exp(errno.EIO, "file(%s) content(%s) error, '%s' is expected" % (filename, result, content))

        if result != content:
            raise Exp(errno.EIO, "file(%s) content(%s) error, '%s' is expected" % (filename, result, content))

        break

def test_uss_write(filename):
    global OBJMVING
    global RUNNING

    while OBJMVING:
        try:
            _test_uss_write(filename)
        except Exp, e:
            if e.errno == errno.ETIMEDOUT or e.errno == errno.ENONET:
                pass
            else:
                derror(e)
                RUNNING = 0
                kill9_self()

def test_objmv():
    ts = []
    global OBJMVING

    OBJMVING = 1
    test_objmv_dir = "/testobjmv"
    testfile = os.path.join(test_objmv_dir, "testfile");
    exec_shell("sdfs.mkdir %s" % (test_objmv_dir))
    exec_shell("sdfs.touch %s" % (testfile))

    t1 = threading.Thread(target=test_uss_objmv, args=(testfile,))
    ts.append(t1)

    t2 = threading.Thread(target=test_uss_write, args=(testfile,))
    ts.append(t2)

    for t in ts:
        t.start()

    [t.join() for t in ts]
    print "test objmv succ!!!"

def test_clean():
    try:
        os.system("/opt/sdfs/app/admin/node.py stop")
    except:
        pass

    os.system("pkill -9 sdfs")
    os.system("pkill -9 redis")
    dmsg("cleanup redis")
    #os.system("systemctl stop redis")
    #os.system("rm -rf /var/lib/redis/*")
    #os.system("systemctl start redis")
    dmsg("cleanup etcd")
    os.system("systemctl stop etcd")
    os.system("rm -rf `grep ETCD_DATA_DIR /etc/etcd/etcd.conf | awk -F '=' '{print $2}' | sed  's/\"//g'`")
    os.system("rm -rf /etc/etcd/etcd.conf")
    os.system("rm -rf /var/lib/etcd/*")
    os.system("chown etcd /var/lib/etcd")

    try:
        os.system("rm -rf %s" % (TEST_PATH))
    except Exp, e:
        #加try是为了规避目录非空的错误
        #目录下会存在 uss.mdstat todo 查下原因
        pass
    #os.system("redis-cli flushdb")
    os.system("rm -rf /dev/shm/sdfs")

def test_install():
    #os.system("cd %s/../build/ && cmake . && make -j7" % (CUR_PATH))
    os.system("cd %s/../build/ && cmake . -DVALGRIND=1 && make -j7 && make install" % (CUR_PATH))

#def test_start():
    #config = Config(TEST_PATH)
    #node = Node(config)
    #node.start()

def nfs_conf_init():
    nfs_conf_path = os.path.join(CUR_PATH, 'nfs_conf')
    nfs_etc_path = '/etc/ganesha'

    _exec = 'mkdir -p %s' % (nfs_etc_path)
    os.system(_exec)

    #  安装etc-conf
    _exec = 'cp -f %s/ganesha.conf %s/common.conf %s' % (nfs_conf_path, nfs_conf_path, nfs_etc_path)
    os.system(_exec)

    # 安装start-conf
    _exec = "cp -f %s/*.service /usr/lib/systemd/system" % (nfs_conf_path)
    _exec = _exec + " && " + "cp -f %s/nfs-ganesha /etc/sysconfig" % (nfs_conf_path)
    _exec = _exec + " && " + "mkdir -p /usr/libexec/ganesha/"
    _exec = _exec + " && " + "cp -f %s/nfs-ganesha-config.sh /usr/libexec/ganesha" % (nfs_conf_path)
    os.system(_exec)

def prepare_share_directory(config):
    #  创建/ftp
    _exec = '%s /ftp' % config.uss_mkdir
    try:
        os.system(_exec)
    except Exp, e:
        derror("mkdir /ftp failed, error:%s" % str(e))
        sys.exit(-1)

def nfs_installed():
    nfs_bin = '/usr/bin/ganesha.nfsd'
    try:
        os.stat(nfs_bin)
        res = 0
    except Exception as e:
        res = e.errno
        if res != errno.ENOENT:
            derror('stat %s failed' % nfs_bin)
            return False
    if res == errno.ENOENT:
        derror('ganesha not found')
        return False

    _exec = '%s -v' % nfs_bin
    try:
        out, _ = exec_shell(_exec, need_return=True)
    except Exp, e:
        derror('errno:%d, error:%s' % e.errno, str(e))
        return False

    dmsg('\n%s' % out)
    return True

#  通过是否可以导出目录，
#  判断nfs是否正常启动
def nfs_running():
    _exec = 'showmount -e localhost'
    retry = 3

    while (retry > 0):
        try:
            retry = retry - 1
            exec_shell(_exec, need_return=True, timeout=10)
            return True
        except Exp, e:
            derror("%s : %s\n" % (_exec, str(e)))
            time.sleep(10)

            _exec_nfs = "systemctl start nfs-ganesha.service"
            exec_shell(_exec_nfs)
            continue
    return False

def mounted(mount_point='/mnt/nfs'):
    _exec = 'cat /proc/mounts | grep %s' % (mount_point)
    try:
        exec_shell(_exec, need_return=True, timeout=10)
    except Exp, e:
        dwarn("%s : %s\n" % (_exec, str(e)))
        return False
    return True

#  mount 默认共享目录/nfs-ganesha,默认挂载点 /mnt/nfs
def start_mount_nfsv4(config, share_dir='/nfs-ganesha', mount_point='/mnt/nfs'):
    #  确保mount_point目录存在
    _exec = 'mkdir -p %s' % (mount_point)
    exec_shell(_exec)

    #  确保/nfs-ganesha存在
    _exec = '%s %s' % (config.uss_mkdir, share_dir)
    try:
        exec_shell(_exec)
    except Exp, e:
        if e.errno != errno.EEXIST:
            derror('mkdir %s failed' % (share_dir))
            sys.exit(e.errno)

    _exec_mount = 'mount -t nfs4 127.0.0.1:%s %s' % (share_dir, mount_point)
    try:
        exec_shell(_exec_mount, timeout=10)
        return True
    except Exp, e:
        derror("%s : %s\n" % (_exec_mount, str(e)))
        return False

def umount_nfsv4(mount_point='/mnt/nfs'):
    _exec = 'umount -l %s' % mount_point
    try:
        exec_shell(_exec)
    except Exp, e:
        derror('umount %s failed' % (mount_point))
        sys.exit(e.errno)

def prepare_mount_nfs(config):
    if not nfs_installed():
        sys.exit(-1)

    #  判断nfs是否正常运行
    if nfs_running():
        #  判断nfs客户端是否已经mount
        if not mounted():
            if not start_mount_nfsv4(config):
                sys.exit(-1)
    else:
        dwarn('nfs-ganesha != running healthy\n')
        sys.exit(-1)

def test_conf():
    os.system("mkdir -p %s/etc" % (TEST_PATH))
    os.system("mkdir -p %s/app" % (TEST_PATH))
    os.system("rm -rf %s/../build/*" % (CUR_PATH))
    os.system("mkdir -p %s/../build/" % (CUR_PATH))
    os.system("cp %s/../CMakeLists.txt %s/../build/" % (CUR_PATH, CUR_PATH))

    os.system('''sed -i s#'CMAKE_INSTALL_PREFIX "/opt/sdfs"'#'CMAKE_INSTALL_PREFIX "%s"'#g %s/../build/CMakeLists.txt''' % (TEST_PATH, CUR_PATH))
    os.system('''sed -i s#'YFS_HOME "/opt/sdfs"'#'YFS_HOME "%s"'#g %s/../build/CMakeLists.txt''' % (TEST_PATH, CUR_PATH))
    os.system('''sed -i s#'${CMAKE_CURRENT_SOURCE_DIR}'#'..'#g %s/../build/CMakeLists.txt''' % (CUR_PATH))

def test_init():
    test_conf()
    test_install()
    ip = socket.gethostbyname(socket.gethostname())


    #exec_shell("mkdir -p %s" % (TEST_PATH)) 
    #exec_shell("cd %s && tar czf %s/app.tar.gz app/ && cd %s && tar xvf app.tar.gz" % (INSTALL_PATH, TEST_PATH, TEST_PATH))
    os.system("cp -r %s/sdfs.conf %s/etc/" % (CUR_PATH, TEST_PATH))
    os.system("cp -r %s/exports.conf %s/etc/" % (CUR_PATH, TEST_PATH))
    #os.system("cp -r %s/cluster.conf %s/etc/" % (CUR_PATH, TEST_PATH))
    os.system("cp -r %s/redis.conf.tpl %s/etc/" % (CUR_PATH, TEST_PATH))
    os.system("cp -r %s/ftp.conf %s/etc/" % (CUR_PATH, TEST_PATH))
    cmd = "sed -i 's/127.0.0.0/%s/g' %s/etc/sdfs.conf" % (ip, TEST_PATH)
    print cmd
    os.system(cmd)
    #nfs_conf_init()
    os.system("mkdir -p %s/data/mond/0" % (TEST_PATH))
    for i in range(CDS):
        os.system("mkdir -p %s/data/cds/%s" % (TEST_PATH, i))
        os.system("touch %s/data/cds/%s/fake" % (TEST_PATH, i))

    ldconfig = "/etc/ld.so.conf.d/sdfs.conf"
    os.system("rm -rf %s" % (ldconfig))

    #fake cluster config
    r = "redis[" + ','.join(str(i) for i in range(2)) + ']'
    c = "cds[" + ','.join(str(i) for i in range(CDS)) + ']'
    cmd = "echo '%s %s mond[0] %s nfs[0]' > %s/etc/cluster.conf" % (socket.gethostname(), r, c, TEST_PATH)
    #print cmd
    os.system(cmd)
   
    config = Config(TEST_PATH)
    count = config.redis_sharding * config.redis_ha

    #for i in count
    r = "redis[" + ','.join(str(i) for i in range(count)) + ']'
    c = "cds[" + ','.join(str(i) for i in range(CDS)) + ']'
    cmd = "echo '%s %s mond[0] %s nfs[0]' > %s/etc/cluster.conf" % (socket.gethostname(), r, c, TEST_PATH)
    print cmd
    os.system(cmd)
    
    config = Config(TEST_PATH)
    count = config.redis_sharding * config.redis_ha
    #for i in range(count):
    #    os.system("mkdir -p %s/data/redis/%d" % (TEST_PATH, i))

    cluster = Cluster(config)
    cluster.create([socket.gethostname()])
    #prepare_share_directory(config)
    #prepare_mount_nfs(config)

def test():
    script_path = os.path.join(CUR_PATH, 'script')

    #test_objmv()
    exec_shell("python2 %s/test_list.py --length 10 --home %s >> %s/fileop.log 2>&1" % (CUR_PATH, TEST_PATH, LOG_PATH))
    exec_shell("python2 %s/nfs_test.py --home %s  >> %s/nfs.log 2>&1" % (script_path, TEST_PATH, LOG_PATH))
    exec_shell("python2 %s/ftp_test.py --home %s  >> %s/ftp.log 2>&1" % (script_path, TEST_PATH, LOG_PATH))
    #exec_shell("python2 %s/quota_test.py  >> %s/misc.log 2>&1" % (script_path, LOG_PATH))
    exec_shell("python2 %s/group_test.py  >> %s/misc.log 2>&1" % (script_path, LOG_PATH))
    exec_shell("python2 %s/user_test.py >> %s/misc.log 2>&1" % (script_path, LOG_PATH))
    exec_shell("python2 %s/share_test.py  >> %s/misc.log 2>&1" % (script_path, LOG_PATH))
    """
    """

    #exec_shell("python2 %s/fuse_test.py --home %s" % (CUR_PATH, TEST_PATH))
    #exec_shell("python2 %s/flock.py" % (script_path))
    #umount_nfsv4()
    #time.sleep(2)
    dmsg("test all successfully")

def test_exec(args):
    dmsg("test begin, log is in %s" % (LOG_PATH))
    if not args.noclean:
        dmsg("clean begin")
        test_clean()
    else:
        dmsg("no clean")
    test_init()

    begin = time.time()
    global RUNNING
    RUNNING = 1

    if not args.nofail:
        fail_simulate()

    time.sleep(3)
    try:
        test()
    except Exception, e:
        umount_nfsv4()
        print 'test fail'
        print e
        RUNNING = 0
        end = time.time()
        print "used %s!" % (end - begin)
        kill9_self()
        exit(e.errno)
    else:
        print "test succ!"

    RUNNING = 0
    end = time.time()
    print "used %s!" % (end - begin)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nofail", action="store_true", help="nofail")
    parser.add_argument("--noclean", action="store_true", help="noclean")
    parser.add_argument("--config", action="store_true", default=False, help="noclean")
    args = parser.parse_args()

    cmd = "mkdir -p %s && list=`ls %s/*.log` ;for i in $list; do echo "" > $i;done" % (LOG_PATH, LOG_PATH)
    os.system(cmd)
    if args.config:
        test_conf()
    else:
        test_exec(args)
