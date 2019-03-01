#!/usr/bin/env python2.7
#-*- coding: utf-8 -*-

import commands
import os
import errno
import fcntl
import sys
import socket
import subprocess
import signal
import time
import paramiko
import platform
import threading
import syslog
import json
from concurrent import futures

from color_output import red, yellow, blue, green, darkgreen

from paramiko import SSHException

msg_lock = threading.Lock()
DEBUG = False

def _dmsg(str1, ttyonly = False):
    return dmsg(str1, ttyonly)

def _dwarn(str1, ttyonly = False):
    return dwarn(str1, ttyonly)

def _derror(str1, ttyonly = False):
    return derror(str1, ttyonly)

def _exec_shell1(cmd, retry = 3, p = True):
    if (p):
        _dmsg ('  ' + cmd)

    _retry = 0
    while (1):
        try:
            p = subprocess.Popen(cmd, shell=True,
                    stdout=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                return stdout, stderr
            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                #_derror("cmd " + cmd + " : " + os.strerror(ret))
                #exit(1)
                raise Exp(ret, "%s: %s" % (cmd, stderr))
        except KeyboardInterrupt as err:
            _dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

class Exp(Exception):
    def __init__(self, errno, err, out = None):
        self.errno = errno
        self.err = err
        self.out = out

    def __str__(self):
        exp_info = 'errno:%s, err:%s'%(self.errno, self.err)
        if self.out is not None:
            exp_info += ' stdout:' + self.out
        return repr(exp_info)

def _str2dict(s, row='\n', col=':'):
    if len(s) == 0:
        return {}

    if (s[-1] == row):
        s = s[:-1]

    a = s.split(row)
    d = {}
    for i in a:
        if col not in i:
            continue

        p = i.split(col)
        if (d.get(p[0])):
            raise Exp(errno.EEXIST, "dup key exist")
        try:
            d[p[0].strip()] = p[1].strip()
        except IndexError as err:
            print ("str %s" % (s))
            raise
    return d

def _syserror(str1):
        syslog.openlog("FusionNAS", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "ERROR: " + str1
        syslog.syslog(syslog.LOG_ERR, msg)
        syslog.closelog()

def _syswarn(str1):
        syslog.openlog("FusionNAS", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "WARN: " + str1
        syslog.syslog(syslog.LOG_WARNING, msg)
        syslog.closelog()

def _sysinfo(str1):
        syslog.openlog("FusionNAS", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "INFO: " + str1
        syslog.syslog(syslog.LOG_INFO, msg)
        syslog.closelog()

def dwarn(str1, ttyonly = False):
    if msg_lock.acquire(5):
        if (os.isatty(sys.stdout.fileno()) or ttyonly):
            sys.stdout.write("\x1b[1;01m%s\x1b[0m\n" % (str1))
        else:
            sys.stdout.write("[%s %s] WARNING:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        sys.stdout.flush()
        msg_lock.release()

def derror(str1, ttyonly = False):
    if (msg_lock.acquire(5)):
        if (os.isatty(sys.stderr.fileno()) or ttyonly):
            sys.stderr.write("\x1b[1;31m%s\x1b[0m\n" % (str1))
        else:
            sys.stderr.write("[%s %s] ERROR:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        sys.stdout.flush()
        msg_lock.release()

def dmsg(str1, ttyonly = False):
    if (msg_lock.acquire(5)):
        if (os.isatty(sys.stdout.fileno()) or ttyonly):
            sys.stdout.write("%s\n" % (str1))
            #sys.stdout.write("\x1b[0;33m%s\x1b[0m\n" % (str1))
        else:
            sys.stdout.write("[%s %s] INFO:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        sys.stdout.flush()
        msg_lock.release()

def alarm_handler(signum, frame):
    raise Exp(errno.ETIME, "command execute time out")

def _scape(s):
    s = s.replace("[", "\[")
    s = s.replace("]", "\]")
    return s



def restart_cron():
    pass

def unset_crontab():
    cron = "/etc/cron.d/usscron"
    if os.path.isfile(cron):
        cmd = "rm /etc/cron.d/usscron"
        os.system(cmd)
    restart_cron()

def _check_config(configfile, key, split, value, fix):
    cmd = "grep -P '^[\t ]*%s[\t ]%s[\t ]*%s$' %s > /dev/null" % (_scape(key), split, value, configfile)
    #print (cmd)
    ret = subprocess.call(cmd, shell=True)
    if (ret):
        #dwarn(cmd + " fail")
        if (fix):
            cmd = "grep -P '^[\t ]*%s' %s > /dev/null" % (_scape(key), configfile)
            #print (cmd)
            ret = subprocess.call(cmd, shell=True)
            if (ret):
                cmd = "echo '%s %s %s' >> %s" %(key, split, value, configfile)
                ret = subprocess.call(cmd, shell=True)
                if (ret):
                    raise Exception("sysctl set fail ret %u" % (ret))
                #dmsg(cmd)
            else:
                if (key.find('/') != -1 or split.find('/') != -1 or value.find('/') != -1):
                    cmd = "sed -i 's:^[\t ]*%s.*$:%s %s %s:g' %s > /dev/null" % (_scape(key), key, split, value, configfile)
                else:
                    cmd = "sed -i 's/^[\t ]*%s.*$/%s %s %s/g' %s > /dev/null" % (_scape(key), key, split, value, configfile)
                #print(cmd)
                ret = subprocess.call(cmd, shell=True)
                if (ret):
                    raise Exception("sysctl set fail ret %u" % (ret))
                #dmsg(cmd)
        else:
            raise Exception("sysctl check fail ret %u" % (ret))

def _check_sysctl(key, value, fix):
        #cmd = "sysctl -a | grep '%s = %s' > /dev/null" % (key, value)
        cmd = "sysctl %s | grep '%s = %s' > /dev/null" % (key, key, value)
        #print (cmd)
        ret = subprocess.call(cmd, shell=True)
        if (ret):
            #dwarn(cmd + " fail")
            if (fix):
                cmd = "sysctl -e %s=%s > /dev/null" % (key, value)
                ret = subprocess.call(cmd, shell=True)
                if (ret):
                    raise Exception("set key %s value %s fail" % (key, value))
                dmsg(cmd)
            else:
                raise Exception("check key %s value %s fail" % (key, value))

def check_sysctl(config, fix=True):
    #dmsg("check sysctl")
    corepath = os.path.abspath(config.home + "/core/")
    os.system("mkdir -p %s" % (corepath))
    #print (corepath)
    _check_sysctl("net.core.wmem_max", str(config.wmem_max), fix)
    _check_sysctl("net.core.rmem_max", str(config.rmem_max), fix)
    _check_sysctl("net.ipv4.ip_forward", "0", fix)
    _check_sysctl("kernel.core_pattern", "%s/core-%%e-%%p-%%s" % (corepath), fix)
    _check_config("/etc/sysctl.conf", "net.core.wmem_max", "=", str(config.wmem_max), fix)
    _check_config("/etc/sysctl.conf", "net.core.rmem_max", "=", str(config.rmem_max), fix)
    _check_config("/etc/sysctl.conf", "net.ipv4.ip_forward", "=", "0", fix)
    _check_config("/etc/sysctl.conf", "kernel.core_pattern", "=", "%s/core-%%e-%%p-%%s" % (corepath), fix)
    #dmsg("check sysctl finished")

def _check_crontab(cron, plan, task, log):
    cmd = "grep '%s' %s > /dev/null" % (task, cron)
    #print (cmd)
    ret = subprocess.call(cmd, shell=True)
    if (ret == 0):
        cmd = "sed -i '/%s/d' %s" % (task.replace('/', '\/'), cron)
        #print (cmd)
        os.system(cmd)
    cmd = "cp %s %s.tmp -f && echo '%s root %s >> %s 2>&1' >> %s.tmp && mv %s.tmp %s -f" % (
            cron, cron, plan, task, log, cron, cron, cron)
    #print (cmd)
    os.system(cmd)

def check_crontab(config):
    #为了删除旧版本的umpcron
    cron = "/etc/cron.d/usscron"
    log_dir = os.path.join(config.home, "log/")
    core_dir = os.path.join(config.home, "core/")
    log_backup_dir = os.path.join(config.home, "log/backup/")
    mds_home = os.path.join(config.home, "mds/0")

    create_umpcron(cron)
    _check_crontab(cron,
                   "0 */1 * * *",
                   "bash %s %s %s" % (config.uss_cleanlog, log_dir, log_backup_dir),
                   "%s" % (os.path.join(log_dir, "cleanlog.log")))
    _check_crontab(cron,
                   "0 */1 * * *",
                   "bash %s %s" % (config.uss_cleancore, core_dir),
                   "%s" % (os.path.join(log_dir, "cleancore.log")))
    ''' unused 2017-07-20
    _check_crontab(cron,
                   "*/10 * * * *",
                   "python2.7 %s objck" % (config.uss_node),
                   "%s" % (os.path.join(log_dir, "ussobjck.log")))
    _check_crontab(cron,
                   "*/1 * * * *",
                   "python2.7 %s checkvip" % (config.uss_node),
                   "%s" % (os.path.join(log_dir, "checkvip.log")))
    '''
    _check_crontab(cron,
                   "0 */8 * * *",
                   "python2.7 %s chunkbalance" % (config.uss_node),
                   "%s" % (os.path.join(log_dir, "chunkbalance.log")))
    _check_crontab(cron,
                   "0 */1 * * *",
                   "python2.7 %s worm -u" % (config.uss_node),
                   "%s" % (os.path.join(log_dir, "uss_worm.log")))
    restart_cron()

def create_umpcron(umpcron):
    umpcron_tmp = umpcron + ".tmp"
    fd = open(umpcron_tmp, 'w')
    fd.write("SHELL=/bin/bash\n")
    fd.write("PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin\n")
    fd.write("# For details see man 4 crontabs\n\n")
    fd.write("# Example of job definition:\n")
    fd.write("# .---------------- minute (0 - 59)\n")
    fd.write("# |  .------------- hour (0 - 23)\n")
    fd.write("# |  |  .---------- day of month (1 - 31)\n")
    fd.write("# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...\n")
    fd.write("# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat\n")
    fd.write("# |  |  |  |  |\n")
    fd.write("# *  *  *  *  * user-name command to be executed\n\n")
    fd.flush()
    fd.close()
    os.rename(umpcron_tmp, umpcron)


def mutil_exec_futures(func, hosts, is_raise=0, timeout=30):
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_host = dict((executor.submit(func, h), h) for h in hosts)
        for future in futures.as_completed(future_to_host):
            host = future_to_host[future]
            try:
                data = future.result(timeout)
            except futures.TimeoutError as e:
                if is_raise:
                    raise Exp(e.errno, "host:%s timeout" % host)
                else:
                    print "host:%s timeout" % host
            except Exception as e:
                if is_raise:
                    raise Exp(e.errno, "exec on host:%s fail, errmsg:%s\n" % (host, e.err))

def mutil_exec(func, args, timeout = None, timeout_args = None):
    #args = [[arg1, ...], ...]
    ts = []
    for i in args:
        t = threading.Thread(target=func, args=i)
        t._args = i
        ts.append(t)

    [t.setDaemon(True) for t in ts]
    [t.start() for t in ts]
    if timeout is None:
        [t.join() for t in ts]
    else:
        for t in ts:
            if (timeout <=0 ):
                break

            start = int(time.time())
            t.join(timeout)
            finished = int(time.time())

            timeout = timeout - (finished - start)

    for t in ts:
        if t.is_alive():
            timeout_args.append(t._args)

def _session_recv(session):
    try:
        data = session.recv(4096)
    except socket.timeout as err:
        data = ""

    return data

def _session_recv_stderr(session):
    try:
        data = session.recv_stderr(4096)
    except socket.timeout as err:
        data = ""

    return data

#>>> 60*60*8
#28800
def _exec_remote(host, cmd, user = "root", password=None, timeout = 1, exectimeout=28800):
    stdout = ""
    stderr = ""
    status = 0

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(host, 22, user, password, timeout = timeout)
        transport = client.get_transport()
        session = transport.open_channel(kind='session')
        session.settimeout(3)
        session.exec_command(cmd)

        now1 = time.time()
        while True:
            if session.recv_ready():
                data = _session_recv(session)
                stdout = stdout + data

            if session.recv_stderr_ready():
                data = _session_recv_stderr(session)
                stderr = stderr + data

            if session.exit_status_ready():
                while True:
                    data = _session_recv(session)
                    if data == "":
                        break
                    stdout = stdout + data

                while True:
                    data = _session_recv_stderr(session)
                    if data == "":
                        break
                    stderr = stderr + data

                break

            now2 = time.time()
            if (now2 - now1) > exectimeout:
                raise Exp(errno.ETIMEDOUT, "timeout, now1: %s, now2: %s, exectimeout: %s"%(now1, now2, exectimeout))

        status = session.recv_exit_status()

    except socket.timeout as err:
        raise Exp(err.errno, 'Socket timeout')
    except socket.error as err:
        raise Exp(err.errno, err.strerror)
    except paramiko.AuthenticationException as err:
        raise Exp(250, 'Authentication failed')

    session.close()
    client.close()
    return stdout, stderr, status

def exec_remote(host, cmd, user = "root", password=None,
                timeout = 1, exectimeout=28800, retry=3):
    _retry = 0
    while True:
        stdout, stderr, status =  _exec_remote(host, cmd, user,
                                            password, timeout, exectimeout)
        if int(status) == 0:
            return (stdout, stderr)
        elif (status == errno.EAGAIN and _retry < retry):
            _retry = _retry + 1
            time.sleep(1)
        else:
            msg = "host: %s, cmd %s, status: %s, stdout: %s, stderr: %s" % (
                    host, cmd, status, stdout, stderr)
            raise Exp(status, msg)

def exec_shell(cmd, retry=3, p=True, need_return=False,
               shell=True, timeout=0):
    if (p):
        dmsg(cmd)

    _retry = 0
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    while (1):
        try:
            if need_return:
                p = subprocess.Popen(cmd, shell=shell,
                                     close_fds=True,
                                     stdout=subprocess.PIPE,
                                     stdin=subprocess.PIPE,
                                     stderr=subprocess.PIPE, env=env)
            else:
                p = subprocess.Popen(cmd, shell=shell, env=env)

            if timeout != 0:
                signal.signal(signal.SIGALRM, alarm_handler)
                signal.alarm(timeout)

            stdout, stderr = p.communicate()
            signal.alarm(0) #disable the alarm
            ret = p.returncode
            if (ret == 0):
                return (stdout, stderr)

            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                raise Exp(ret, stderr, stdout)
        except KeyboardInterrupt as err:
            dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

def exec_pipe1(cmd, retry=3, p=True, timeout=0):
    cmd = ' '.join(cmd)
    return exec_shell(cmd, retry, p, need_return=True, shell=True)

def exec_pipe(cmd, retry=3, p=True, timeout=0):
    res, _ = exec_pipe1(cmd, retry, p, timeout)
    return res

def _exec_pipe(cmd, retry = 3, p = True, timeout = 0):
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        dmsg(cmd1)
    if DEBUG:
        start = time.time()
        print cmd1,

    while (1):
        p = None
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, env = env)
        except Exception, e:
            raise Exp(e.errno, cmd1 + ": command execute failed")

        if timeout != 0:
            deadline = time.time() + timeout
        else:
            deadline = 0

        while time.time() < deadline and p.poll() == None:
            time.sleep(1)
        if deadline and p.poll() == None:
            p.terminate()
            raise Exp(errno.ETIME, cmd1 + ": command execute time out")

        try:
            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                if DEBUG:
                    print "used ", str(time.time() - start)
                return stdout
            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                #_derror("cmd %s" % (cmd1))
                raise Exp(ret, cmd1 + ": " + os.strerror(ret))

        except KeyboardInterrupt as err:
            _dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

'''
ecec_pipe return stdout & stderr
'''
def _exec_pipe1(cmd, retry = 3, p = True, timeout = 0):
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        dmsg(cmd1)
    if DEBUG:
        start = time.time()
        print cmd1,

    while (1):
        p = None
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env = env)
        except Exception, e:
            raise Exp(e.errno, cmd1 + ": command execute failed")

        if timeout != 0:
            deadline = time.time() + timeout
        else:
            deadline = 0

        while time.time() < deadline and p.poll() == None:
            time.sleep(1)
        if deadline and p.poll() == None:
            p.terminate()
            raise Exp(errno.ETIME, cmd1 + ": command execute time out")

        try:
            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                if DEBUG:
                    print "used ", str(time.time() - start)
                return stdout, stderr
            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                #_derror("cmd %s" % (cmd1))
                #return stdout, stderr
                raise Exp(ret, stderr, stdout)
        except KeyboardInterrupt as err:
            _dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

'''
ecec_pipe with stdin
'''
def _exec_pipe2(cmd, retry = 3, p = True, timeout = 0, stdin = None):
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        dmsg(cmd1)
    if DEBUG:
        start = time.time()
        print cmd1,

    while (1):
        p = None
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, env = env)
        except Exception, e:
            raise Exp(e.errno, cmd1 + ": command execute failed")

        if stdin:
            p.stdin.write('%s\n' % stdin)
            p.stdin.flush()

        if timeout != 0:
            deadline = time.time() + timeout
        else:
            deadline = 0

        while time.time() < deadline and p.poll() == None:
            time.sleep(1)
        if deadline and p.poll() == None:
            p.terminate()
            raise Exp(errno.ETIME, cmd1 + ": command execute time out")

        try:
            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                if DEBUG:
                    print "used ", str(time.time() - start)
                return stdout
            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                #_derror("cmd %s" % (cmd1))
                raise Exp(ret, cmd1 + ": " + os.strerror(ret))

        except KeyboardInterrupt as err:
            _dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

def put_remote(host, local, remote, user = "root", password=None, timeout = 1):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        s.connect(host, 22, user, password, timeout = timeout)
        f = s.open_sftp()
        f.put(local, remote)
    except socket.timeout as err:
        raise Exp(err.errno, err.strerror)
    except socket.error as err:
        raise Exp(err.errno, err.strerror)
    except paramiko.AuthenticationException as err:
        raise Exp(250, 'Authentication failed')
    except IOError, e:
        raise Exp(e.errno, e.strerror)
    f.close()
    s.close()

def set_value(path, buf):
    fd = open(path, 'w')
    fd.write(buf)
    fd.close()

def get_value(path):
    size = os.path.getsize(path)
    fd = open(path, 'r')
    buf = fd.read(size)
    fd.close()
    return buf

def exec_system(cmd, p=True, out=True, err=True):
    if p:
        print cmd;
    if not out:
        cmd += " >/dev/null"
    if not err:
        cmd += " 2>/dev/null"

    errno = os.system(cmd)
    errno >>= 8

    return errno

def check_ip_valid(ipaddr):
    addr = ipaddr.strip().split('.')
    if len(addr) != 4:
        raise Exp(errno.EINVAL, "ip %s is invalid" %(ipaddr))

    for i in range(4):
        try:
            addr[i] = int(addr[i])
        except:
            raise Exp(errno.EINVAL, "ip %s is invalid" %(ipaddr))

        if addr[i] >= 0 and addr[i] <= 255:
            pass
        else:
            raise Exp(errno.EINVAL, "ip %s is invalid" %(ipaddr))

def lock_file(key, timeout=3600, p=True):
    #return 0
    key = os.path.abspath(key)
    parent = os.path.split(key)[0]
    os.system("mkdir -p " + parent)

    while (1):
        if p:
            dmsg("lock " + key)
        lock_fd = open(key, 'a')

        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as err:
            #print ("errno %d" % err.errno)
            if err.errno == errno.EAGAIN:
                fd = open(key, 'r')
                s = fd.read()
                fd.close()
                if (',' in s):
                    (ltime, pid) = s.split(',')
                    ltime = int(ltime)
                    pid = int(pid)
                    now = int(time.time())
                    if (now > ltime and ltime != -1):
                        derror("%s locked by %d time %u, just kill it" % (key, pid, now - ltime))
                        cmd = "ps --ppid " + str(pid) + " | awk 'NR!=1{print $1}' | xargs kill -9 2>/dev/null"
                        os.system(cmd)
                        os.system("kill -9 %d" % pid)
                        lock_fd.close()
                        os.unlink(key)
                        continue
                    else:
                        derror("%s locked, %d before timeout, exit..." % (key, ltime - now))
                        sys.exit(errno.EBUSY)
                else:
                    os.unlink(key)
                    lock_fd.close()
                    continue
            else:
                raise

        lock_fd.truncate(0)
        if timeout == -1:
            s = '-1,' + str(os.getpid())
        else:
            s = str(int(time.time()) + timeout) + ',' + str(os.getpid())
        lock_fd.write(s)
        lock_fd.flush()
        break
    return lock_fd

def lock_file1(key, timeout=5, p=False):
    key = os.path.abspath(key)
    parent = os.path.split(key)[0]
    os.system("mkdir -p " + parent)

    if p:
        dmsg("get lock %s" %(key))

    lock_fd = open(key, 'a')

    if timeout != 0:
        dmsg("lock_file1 signal timeout %d" % (timeout))
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(timeout)

    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
        signal.alarm(0)
        if p:
            dmsg("lock %s success" %(key))
    except Exception, e:
        raise Exp(errno.EPERM, "lock %s failed" %(key))

    return lock_fd

def unlock_file1(lock_fd):
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

def human_readable(num, idx=0):
    u = ['B', 'K', 'M', 'G', 'T']
    #print ("num %u idx %u" % (num, idx))
    if (num / 1024 < 1):
        return str(int(num)) + u[idx]
    else:
        return human_readable(num / 1024, idx + 1)

def _human_readable(num, human=False, idx=0):
    if human:
        trans = 1000.0
    else:
        trans = 1024.0

    u = ['B', 'KB', 'MB', 'GB', 'TB']
    #print ("num %.1f idx %u" % (num, idx))
    if ((num / trans < 1) or (idx == len(u) - 1)):
        return str(round(num, 1)) + u[idx]
    else:
        return _human_readable(num / trans, human, idx + 1)

def _human_unreadable(_size, human=False):
    if human:
        trans = 1000
    else:
        trans = 1024

    if _size[-1] == 'b' or _size[-1] == 'B':
        unit = _size[-2]
        num = float(_size[0:-2])
    else:
        unit = _size[-1]
        num = float(_size[0:-1])

    assert(unit in "kKmMgGtT")
    if (unit in ("kK")):
        return num * trans
    elif (unit in ("Mm")):
        return num * trans * trans
    elif (unit in ("Gg")):
        return num * trans * trans * trans
    else:
        return num * trans * trans * trans * trans

def dev_lsblks():
    """
    [root@test02 cherry]# lsblk -astbpnio NAME,TYPE,SIZE,MOUNTPOINT
    /dev/fd0                disk         4096
    /dev/sda1               part   1073741824 /boot
    `-/dev/sda              disk 107374182400
    /dev/sdb                disk 107374182400
    /dev/sr0                rom    1073741312
    /dev/mapper/centos-root lvm   88042635264 /
    `-/dev/sda2             part 105230893056
      `-/dev/sda            disk 107374182400
    /dev/mapper/centos-swap lvm   17179869184 [SWAP]
    `-/dev/sda2             part 105230893056
      `-/dev/sda            disk 107374182400
    """
    #dev = {"/dev/fd0": {"type": "disk", "size": 4096,
    #       "mountpoint": "/boot", "parent": 'xxx'}}
    #devs = [dev, dev, ...]
    cmd = "lsblk -astbpio NAME,TYPE,SIZE,MOUNTPOINT,UUID"
    rs, _ = exec_shell(cmd, p=False, need_return=True, shell=True)
    lines = rs.strip("\n").split("\n")
    prefix = "`-"
    devs = {}

    head = lines[0]
    lines_new = []
    for line in lines:
        line_new = list(line)
        for k in head.split():
            i = head.index(k)
            t = i + len(k)
            w = line[i:t].strip()
            if not w:
                line_new[i:t] = "F"*(t-i)

        lines_new.append("".join(line_new))

    lines = lines_new
    for i in range(len(lines)):
        line = lines[i].lstrip(" ")
        line_next = None
        try:
            line_next = lines[i+1].lstrip(" ")
        except IndexError as err:
            pass

        blk = []
        for x in line.split():
            if x.strip("F"):
                blk.append(x)
            else:
                blk.append(None)

        name = blk[0].strip(prefix)
        parent = None
        if line_next and line_next.startswith(prefix):
            parent = line_next.split()[0].strip(prefix)

        dev = {name: {"type": blk[1], "size": blk[2],
               "mountpoint": blk[3], "uuid": blk[4], "parent": parent}}
        devs.update(dev)

    return devs

def _get_dev_childs(device, devs):
    childs = []
    for k in devs.keys():
        v = devs[k]
        if v["parent"] == device:
            childs.append(k)

    for c in childs:
        childs.extend(_get_dev_childs(c, devs))

    return childs

def dev_childs(device, devs=None):
    if devs is None:
        devs = dev_lsblks()
    return _get_dev_childs(device, devs)

def dev_mountpoints(device):
    devs = dev_lsblks()
    childs = dev_childs(device, devs)
    childs.append(device)
    ms = []
    for c in childs:
        dev = devs[c]
        m = dev["mountpoint"]
        if m:
            ms.append(m)
    return ms

def dev_mounted(name):
    #ms = dev_mountpoints(name)
    #['/boot', '/', '[SWAP]']
    pass

def fstab_del_mount(mountpoint):
    m = mountpoint.replace('/', '\/')
    cmd = "sed -i '/^[^#].*%s /d' /etc/fstab" % (m)
    exec_shell(cmd)

def dev_mkfs_ext4(dev):
    cmd = "mkfs.ext4 -F %s" % (dev)
    exec_shell(cmd)

def dev_uuid(dev):
    devs = dev_lsblks()
    return devs[dev]["uuid"]

def dev_clean(dev):
    cmd = "sgdisk -g --clear %s" % (dev)
    exec_shell(cmd)

def _ssh_deploy_key(host, passwd):
    while (1):
        os.system("if [ ! -f  /root/.ssh/id_dsa.pub ];then ssh-keygen -t dsa -P '' -f /root/.ssh/id_dsa ; fi")
        pub = get_value('/root/.ssh/id_dsa.pub')
        private = get_value('/root/.ssh/id_dsa')
        if "'" in pub or '"' in pub or "'" in private or '"' in private :
            os.system('rm -rf /root/.ssh/id_dsa*')
        else:
            break

    try:
        cmd = "if [ ! -d /root/.ssh ]; then mkdir /root/.ssh; fi"
        cmd = cmd + " && echo '%s' >> /root/.ssh/authorized_keys" % (pub)
        cmd = cmd + " && echo '%s' > /root/.ssh/id_dsa.pub" % (pub)
        cmd = cmd + " && echo '%s' > /root/.ssh/id_dsa" % (private)
        cmd = cmd + " && chmod 0600 /root/.ssh/id_dsa"
        cmd = cmd + " && if [ -f /etc/init.d/iptables ];then /etc/init.d/iptables stop; setenforce 0;fi"
        exec_remote(host, cmd, 'root', passwd)
        exec_remote(host, 'ls', 'root', passwd)
    except socket.gaierror as err:
        raise Exp(err.errno, err.strerror)
    except SSHException as err:
        raise Exp(errno.EINVAL, str(err))

def ssh_set_nopassword(hosts, password):
    for i in hosts:
        try:
            print("deploy sshkey for " + i)
            _ssh_deploy_key(i, password)
            #_exec_pipe([self.config.lich + "/admin/gen_hostkey.sh", i])
        except SSHException as err:
            derror("%s:%s" % (i, str(err)))
            continue
        except Exp, e:
            derror("%s:%s" % (i, e.err))
            continue

    os.system("cat /root/.ssh/known_hosts > /tmp/known_hosts")

    for i in hosts:
        try:
            put_remote(i, "/tmp/known_hosts", "/root/.ssh/known_hosts", 'root', password)
        except SSHException as err:
            derror("%s:%s" % (i, str(err)))
            continue
        except Exp, e:
            derror("%s:%s" % (i, e.err))
            continue

    os.system("rm -f  /tmp/known_hosts")

def lsb_release():
    """
    Get LSB release information from platform.

    Returns truple with distro, release and codename.

    distro supports :('SuSE', 'debian', 'fedora', 'redhat', 'centos',
    'mandrake', 'mandriva', 'rocks', 'slackware', 'yellowdog', 'gentoo',
    'UnitedLinux', 'turbolinux', 'Ubuntu')

    """
    distro, release, codename = platform.dist()
    return (str(distro).rstrip(), str(release).rstrip(), str(codename).rstrip())

def kill9_self():
    self_pid = os.getpid()
    exec_shell("kill -9 %s" % (self_pid))


def _install_init_ussd(home):
    dwarn("systemd disable\n")
    return
    
    (distro, release, codename) = lsb_release()
    if (distro == 'Ubuntu'):
        src = os.path.join(home, "app/admin/ussd_init_ubuntu")
        dst = "/etc/init.d/ussd"
        cmd = 'cp %s %s' % (src, dst)
        os.system(cmd)

        ldst = "/etc/rcS.d/S42ussd"
        cmd = 'ln -s %s %s' % (dst, ldst)
        if not os.path.exists(ldst):
            os.system(cmd)
    elif (distro == 'centos'):
        version = int(release.split(".")[0])
        if version <=  6:
            src = os.path.join(home, "app/admin/ussd_init_centos_6")
            dst = "/etc/init.d/ussd"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            os.system("chkconfig --add ussd")
        elif version == 7:
            src = os.path.join(home, "app/admin/ussd_init_centos_7")
            dst = "/usr/lib/systemd/system/ussd.service"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            cmd = "chmod 644 %s" % (dst)
            os.system(cmd)
            cmd = "systemctl enable ussd.service"
            os.system(cmd)
        else:
            raise Exception("not support %s %s %s" % (distro, release, codename))
    else:
        derror("not support %s %s %s" % (distro, release, codename))

def _install_init_samba(home):
    (distro, release, codename) = lsb_release()
    if (distro == 'Ubuntu'):
        src = os.path.join(home, "app/admin/uss_samba_init_ubuntu")
        dst = "/etc/init.d/uss_samba"
        cmd = 'cp %s %s' % (src, dst)
        os.system(cmd)

        ldst = "/etc/rcS.d/S42uss_samba"
        cmd = 'ln -s %s %s' % (dst, ldst)
        if not os.path.exists(ldst):
            os.system(cmd)
    elif (distro == 'centos'):
        version = int(release.split(".")[0])
        if version <=  6:
            src = os.path.join(home, "app/admin/uss_samba_init_centos_6")
            dst = "/etc/init.d/uss_samba"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            os.system("chkconfig --add uss_samba")
        elif version == 7:
            src = os.path.join(home, "app/admin/uss_samba_init_centos_7")
            dst = "/usr/lib/systemd/system/uss_samba.service"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            cmd = "chmod 644 %s" % (dst)
            os.system(cmd)
            cmd = "systemctl daemon-reload"
            os.system(cmd)
        else:
            raise Exception("not support %s %s %s" % (distro, release, codename))
    else:
        derror("not support %s %s %s" % (distro, release, codename))

def _install_init_ucarp(home):
    (distro, release, codename) = lsb_release()
    if (distro == 'Ubuntu'):
        src = os.path.join(home, "app/admin/uss_ucarp_init_ubuntu")
        dst = "/etc/init.d/uss_ucarp"
        cmd = 'cp %s %s' % (src, dst)
        os.system(cmd)

        ldst = "/etc/rcS.d/S42uss_ucarp"
        cmd = 'ln -s %s %s' % (dst, ldst)
        if not os.path.exists(ldst):
            os.system(cmd)
    elif (distro == 'centos'):
        version = int(release.split(".")[0])
        if version <=  6:
            src = os.path.join(home, "app/admin/uss_ucarp_init_centos_6")
            dst = "/etc/init.d/uss_ucarp"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            os.system("chkconfig --add uss_ucarp")
        elif version == 7:
            src = os.path.join(home, "app/admin/uss_ucarp_init_centos_7")
            dst = "/usr/lib/systemd/system/uss_ucarp.service"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            cmd = "chmod 644 %s" % (dst)
            os.system(cmd)
            cmd = "systemctl enable uss_ucarp.service"
            os.system(cmd)
        else:
            raise Exception("not support %s %s %s" % (distro, release, codename))
    else:
        derror("not support %s %s %s" % (distro, release, codename))

def _install_init_vip(home):
    (distro, release, codename) = lsb_release()
    if (distro == 'Ubuntu'):
        src = os.path.join(home, "app/admin/uss_vip_init_ubuntu")
        dst = "/etc/init.d/uss_vip"
        cmd = 'cp %s %s' % (src, dst)
        os.system(cmd)

        ldst = "/etc/rcS.d/S42uss_vip"
        cmd = 'ln -s %s %s' % (dst, ldst)
        if not os.path.exists(ldst):
            os.system(cmd)
    elif (distro == 'centos'):
        version = int(release.split(".")[0])
        if version <=  6:
            src = os.path.join(home, "app/admin/uss_vip_init_centos_6")
            dst = "/etc/init.d/uss_vip"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            os.system("chkconfig --add uss_vip")
        elif version == 7:
            src = os.path.join(home, "app/admin/uss_vip_init_centos_7")
            dst = "/usr/lib/systemd/system/uss_vip.service"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            cmd = "chmod 644 %s" % (dst)
            os.system(cmd)
            cmd = "systemctl enable uss_vip.service"
            os.system(cmd)
        else:
            raise Exception("not support %s %s %s" % (distro, release, codename))
    else:
        derror("not support %s %s %s" % (distro, release, codename))

def install_init(home):
    _install_init_ussd(home)
    _install_init_ucarp(home)
    _install_init_samba(home)
    _install_init_vip(home)

def ip_addrs():
    cmd = "ip addr|grep 'inet '|awk '{print $2}'"
    rs, _ = exec_shell(cmd, p=False, need_return=True, shell=True)
    addrs = []
    for ip in rs.split():
        addrs.append(ip)
    return addrs

def ping(host):
        cmd = 'ping %s -c 3 -W 1' % (host)
        try:
            exec_shell(cmd)
            return True
        except Exp, err:
            return False

def json_store(data, json_file):
    with open(json_file, 'w') as f:
        f.write(json.dumps(data))

def json_load(json_file):
    with open(json_file) as f:
        data = json.load(f)
        return data

def check_process_exists(key):
    try:
        cmd = "ps -ef | grep '%s' | grep -v grep | wc -l" % (key)
        out, err = exec_shell(cmd, p=False, need_return=True)
        return int(out.strip())
    except Exception as e:
        return 0

def get_dev_by_addr(addr):
    if addr == "":
        return ""

    ridx = addr.rindex(".")
    addr_seg = addr[0:ridx]
    try:
        cmd = """ifconfig | grep -B1 '%s\.' | head -n1 | awk -F':' '{print $1}'""" % (addr_seg)
        out, err = exec_shell(cmd, p=False, need_return=True)
        dev = out.strip()
        if len(dev) == 0:
            raise Exp(errno.ENOENT, "addr:%s device not found !" % (addr))

        return dev
    except Exception as e:
        raise Exp(errno.EPERM, "get addr:%s device fail!" % (addr))

def get_mask_by_addr(addr):
    if addr == "":
        return ""

    ridx = addr.rindex(".")
    addr_seg = addr[0:ridx]
    try:
        cmd = """ip addr | grep "%s\." | head -n1 | awk '{print $2}' | awk -F"/" '{print $2}'""" % (addr_seg)
        out, err = exec_shell(cmd, p=False, need_return=True)
        mask = out.strip()
        if len(mask) == 0:
            raise Exp(errno.ENOENT, "addr:%s mask not found !" % (addr))

        return mask
    except Exception as e:
        raise Exp(errno.EPERM, "get addr:%s mask fail!" % (addr))

def get_tcp_connections_for_ip(addr):
    cmd = "netstat -anp | grep ESTABLISHED | grep '%s' | awk '{print $4, $5}'" % (addr)
    out, err = exec_shell(cmd, p=False, need_return=True)
    return out.strip().split('\n')

def kill_tcp_connections(addr):
    one_way = False
    killtcp_tool = "/usr/local/samba/libexec/ctdb/ctdb_killtcp"
    _connections = []
    _killcount = 0

    if not os.path.isfile(killtcp_tool):
        dwarn("tool:(%s) not find" % killtcp_tool)
        return

    dev =  get_dev_by_addr(addr)
    conn_list = get_tcp_connections_for_ip(addr)
    for i in range(len(conn_list)):
        if len(conn_list[i]) == 0:
            continue

        dst = conn_list[i].split(" ")[0]
        src = conn_list[i].split(" ")[1]
        ridx = dst.rindex(":")
        dest_port = int(dst[ridx+1:])
        #wo only do one-way killtcp for CIFS
        if dest_port == 139 or dest_port == 445:
            one_way = True

        _connections.append("%s %s" % (src, dst))
        if not one_way:
            _connections.append("%s %s" % (dst, src))

        _killcount = _killcount + 1

    for conn in _connections:
        try:
            cmd = "%s %s %s > /dev/null" % (killtcp_tool, dev, conn)
            #exec_shell(cmd, p=False)
            exec_shell(cmd, p=False)
        except Exp, e:
            derror("kill conn:%s fail", conn)
            pass

        
if __name__ == "__main__":
    #print dev_lsblks()
    #print dev_mountpoints("/dev/sda")
    #print dev_mountpoints("/dev/sdb")
    print "hello, word!"
    kill_tcp_connections("192.168.120.105")
