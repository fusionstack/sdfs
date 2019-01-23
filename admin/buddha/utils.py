#!/usr/bin/python2
#coding:utf8

import commands
import os
import sys
import errno
import socket
import subprocess
import paramiko
import fcntl
import time
import threading
import struct
import urllib2
import re
import signal
import json
import random
import syslog
from paramiko import SSHException

import lsb

msg_lock = threading.Lock()
BACKTRACE = "/dev/shm/lich4/msgctl/backtrace"

def _print1(str1):
    print("\x1b[1;32m%s\x1b[0m" % (str1))

def _print2(str1):
    print("\x1b[1;31m%s\x1b[0m" % (str1))

def _print3(str1):
    print("\x1b[0;33m%s\x1b[0m" % (str1))

def _dwarn(str1, ttyonly = False):
    if msg_lock.acquire(5):
        if (os.isatty(sys.stdout.fileno()) or ttyonly):
            sys.stdout.write("\x1b[1;01m%s\x1b[0m\n" % (str1))
        else:
            sys.stdout.write("[%s %s] WARNING:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        msg_lock.release()

def _derror(str1, ttyonly = False):
    if (msg_lock.acquire(5)):    
        if (os.isatty(sys.stderr.fileno()) or ttyonly):
            sys.stderr.write("\x1b[1;31m%s\x1b[0m\n" % (str1))
        else:
            sys.stderr.write("[%s %s] ERROR:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        msg_lock.release()

def _dmsg(str1, ttyonly = False):
    if (msg_lock.acquire(5)):
        if (os.isatty(sys.stdout.fileno()) or ttyonly):
            sys.stdout.write("%s\n" % (str1))
            #sys.stdout.write("\x1b[0;33m%s\x1b[0m\n" % (str1))
        else:
            sys.stdout.write("[%s %s] INFO:%s\n" % (int(time.time()), time.strftime('%F %T'), str1))
        msg_lock.release()

def _syserror(str1):
        syslog.openlog("FusionStor", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "ERROR: " + str1
        syslog.syslog(syslog.LOG_ERR, msg)
        syslog.closelog()
            
def _syswarn(str1):
        syslog.openlog("FusionStor", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "WARN: " + str1
        syslog.syslog(syslog.LOG_WARNING, msg)
        syslog.closelog()

def _sysinfo(str1):
        syslog.openlog("FusionStor", syslog.LOG_CONS | syslog.LOG_PID | syslog.LOG_NDELAY, syslog.LOG_LOCAL1)
        msg = "INFO: " + str1
        syslog.syslog(syslog.LOG_INFO, msg)
        syslog.closelog()

def _get_value(path):
    size = os.path.getsize(path)
    fd = open(path, 'r')
    buf = fd.read(size)
    fd.close()
    return buf

def _set_value(path, buf):
    fd = open(path, 'w')
    fd.write(buf)
    fd.close()

def _get_backtrace():
    return _get_value(BACKTRACE).strip() == '1'

def is_ping(host):
        cmd = 'ping %s -c 3 -W 1' % (host)
        try:
            _exec_shell(cmd)
            return True
        except Exp, err:
            return False

def _load_version(array):
    if (len(array) == 0 or len(array[0]) == 0 or array[0][0] != '#'):
        raise Exp(errno.EINVAL, "bad config file")

    k = array[0].split('=')
    if (len(k) != 2):
        raise Exp(errno.EINVAL, "bad config file")

    hosts_version = int(k[1])
    return hosts_version


def _hosts_load(configfile):
    hosts = {}
    hosts_version = 0

    try:
        size = os.path.getsize(configfile)
    except OSError as err:
        if err.errno != errno.ENOENT:
            raise
        else:
            return (hosts_version, hosts)

    fd = open(configfile, 'r')
    buf = fd.read(size)
    fd.close()

    array = buf.split('\n')
    for i in array:
        if (len(i) and i[0] != '#'):
            #print (i)
            k = i.split(':')
            if (len(k) == 1):
                hosts[k[0]] = 1
            else:
                hosts[k[0]] = int(k[1])

    hosts_version = _load_version(array)
    return (hosts_version, hosts)

def _net_blocked(hostname):
    s = hostname
    path = "/dev/shm/lich4/blocklist/" + s[0]
    if (os.path.exists(path)):
        return True
    else:
        return False

def _scape(s):
    s = s.replace("[", "\[")
    s = s.replace("]", "\]")
    return s

def _check_config(configfile, key, split, value, fix):
    cmd = "grep -P '^[\t ]*%s[\t ]%s[\t ]*%s$' %s > /dev/null" % (_scape(key), split, value, configfile)
    #print (cmd)
    ret = subprocess.call(cmd, shell=True)
    if (ret):
        #_dwarn(cmd + " fail")
        if (fix):
            cmd = "grep -P '^[\t ]*%s' %s > /dev/null" % (_scape(key), configfile)
            #print (cmd)
            ret = subprocess.call(cmd, shell=True)
            if (ret):
                cmd = "echo '%s %s %s' >> %s" %(key, split, value, configfile)
                ret = subprocess.call(cmd, shell=True)
                if (ret):
                    raise Exception("sysctl set fail ret %u" % (ret))
                _dmsg(cmd)
            else:
                if (key.find('/') != -1 or split.find('/') != -1 or value.find('/') != -1):
                    cmd = "sed -i 's:^[\t ]*%s.*$:%s %s %s:g' %s > /dev/null" % (_scape(key), key, split, value, configfile)
                else:
                    cmd = "sed -i 's/^[\t ]*%s.*$/%s %s %s/g' %s > /dev/null" % (_scape(key), key, split, value, configfile)
                #print(cmd)
                ret = subprocess.call(cmd, shell=True)
                if (ret):
                    raise Exception("sysctl set fail ret %u" % (ret))
                _dmsg(cmd)
        else:
            raise Exception("sysctl check fail ret %u" % (ret))

def _check_crontab(plan, task, log):
    cmd = "grep '%s' /etc/cron.d/lichcron > /dev/null" % (task)
    #print (cmd)
    ret = subprocess.call(cmd, shell=True)
    if (ret == 0):
        cmd = "sed -i '/%s/d' /etc/cron.d/lichcron" % (task.replace('/', '\/'))
        #print (cmd)
        os.system(cmd)
    cmd = "cp /etc/cron.d/lichcron /etc/cron.d/lichcron.tmp -f && echo '%s root %s >> %s' >> /etc/cron.d/lichcron.tmp && mv /etc/cron.d/lichcron.tmp /etc/cron.d/lichcron -f" % (plan, task, log)
    #print (cmd)
    os.system(cmd)

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

def _str2dict(s):
    if len(s) == 0:
        return {}

    if (s[-1] == '\n'):
        s = s[:-1]

    a = s.split('\n')
    d = {}
    for i in a:
        if ':' not in i:
            continue

        p = i.split(':')
        if (d.get(p[0])):
            raise Exp(errno.EEXIST, "dup key exist")
        try:
            d[p[0].strip()] = p[1].strip()
        except IndexError as err:
            print ("str %s" % (s))
            raise
    return d

def _getrack(name):
    r = re.compile("([^.]+)\.([^.]+)\.([^.]+)")
    m = r.match(name)
    if (m == None):
        r = re.compile("([^.]+)\.([^.]+)")
        m = r.match(name)
        if (m == None):
            return "default"
        else:
            return m.groups()[0]
    else:
        return m.groups()[1]

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
def _exec_remote2(host, cmd, user = "root", password=None, timeout = 10, exectimeout=28800):
    if (_net_blocked(host)):
        raise Exp(errno.ENOENT, "blocked")

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

def _exec_remote(host, cmd, user = "root", password=None, timeout = 10, exectimeout=28800):
    str1, str2, status =  _exec_remote2(host, cmd, user, password, timeout, exectimeout)
    return str1, str2

def _exec_remote1(host, cmd, user = "root", password=None, timeout = 10, exectimeout=28800):
    str1, str2, status =  _exec_remote2(host, cmd, user, password, timeout, exectimeout)
    if status != 0:
        raise Exp(status, str1 + " " + str2)
    return str1, str2

def _exec_http(host, cmd, user = "root", password=None, timeout = 1):
    if (_net_blocked(host)):
        raise Exp(errno.ENOENT, "blocked")

    url = 'http://' + host + ':27901/' + urllib2.quote(cmd)
    #print(url)
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req)
        rep = response.read()
    except socket.timeout as err:
        raise Exp(err.errno, err.strerror)
    except socket.error as err:
        raise Exp(err.errno, err.strerror)

    #print(rep)
    #print(type(rep))
    res = json.loads(rep) 
    stdout = res['stdout']
    stderr = res['stderr']
    err = res['errno']
    return stdout, stderr

def _put_remote(host, local, remote, user = "root", password=None, timeout = 10):
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

def _get_remote(host, remote, local, user = "root", password=None, timeout = 10):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        s.connect(host, 22, user, password, timeout = timeout)
        f = s.open_sftp()
        f.get(remote, local)
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

def _lock_file(key, timeout=3600, p=True):
    key = os.path.abspath(key)
    parent = os.path.split(key)[0]
    os.system("mkdir -p " + parent)

    while (1):
        if p:
            _dmsg("lock " + key)
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
                        _derror("%s locked by %d time %u, just kill it" % (key, pid, now - ltime))
                        if (cmp(key, "/var/run/fusionstack_recover.lock")):
                            print "file is not /var/run/fusionstack_recover.lock"
                        else:
                            cmd = "ps --ppid " + str(pid) + " | awk 'NR!=1{print $1}' | xargs kill -9"
                            os.system(cmd)
                        os.system("kill -9 %d" % pid)
                        lock_fd.close()
                        os.unlink(key)
                        continue
                    else:
                        _derror("%s locked, %d before timeout, exit..." % (key, ltime - now))
                        exit(errno.EBUSY)
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

def alarm_handler(signum, frame):
    raise Exp(errno.ETIME, "command execute time out")

def _lock_file1(key, timeout=5, p=False):
    key = os.path.abspath(key)
    parent = os.path.split(key)[0]
    os.system("mkdir -p " + parent)


    if p:
        _dmsg("get lock %s" %(key))
    lock_fd = open(key, 'a')

    if timeout != 0:
        signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(timeout)
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
        signal.alarm(0)
        if p:
            _dmsg("lock %s success" %(key))
    except Exception, e:
        raise Exp(errno.EPERM, "lock %s failed" %(key))

    return lock_fd

def _unlock_file1(lock_fd):
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

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

def _exec_system(cmd, p=True, out=True, err=True):
    if p:
        print cmd;
    if not out:
        cmd += " >/dev/null"
    if not err:
        cmd += " 2>/dev/null"

    errno = os.system(cmd)
    errno >>= 8

    return errno

def __killWork(t):
    while t > 0:
        time.sleep(1)
        t -= 1
    else:
        _derror("execute time out, will be killed")
        os.kill(os.getpid(), 9)

def __startTiming(t):
    t = threading.Thread(target=__killWork, args=[t])
    t.setDaemon(True)
    t.start()

def timeit(timeout=60*60*2):
    def decorator(func):
        def wrapper(*args, **kw):
            __startTiming(timeout)
            result = ''
            try:
                result = func(*args, **kw)
            except Exp as e:
                _dwarn("execute time out")
                exit(e.errno)
            return result
        return wrapper
    return decorator

def _exec_pipe(cmd, retry = 3, p = True, timeout = 0):
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        _dmsg(cmd1)
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


def _exec_pipe1(cmd, retry=3, p=True, timeout=0):
    env = {"LANG" : "en_US", "LC_ALL" : "en_US", "PATH" : os.getenv("PATH")}
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        _dmsg(cmd1)
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

def _exec_big(cmd, retry = 3, p = True, print_result=False):
    #cmd = self.lich_inspect + " --movechunk '%s' %s  --async" % (k, loc)
    _retry = 0
    cmd1 = ''
    for i in cmd:
        cmd1 = cmd1 + i + ' '
    if (p):
        _dmsg(cmd1)
    while (1):
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)

        try:
            res = ""
            while (p.poll() == None):
                r = p.communicate()[0]
                res += r
                if (print_result):
                    print(r),

            ret = p.wait()
            if (ret == 0):
                return res
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

def _exec_shell(cmd, retry = 3, p = True):
    if (p):
        _dmsg (cmd)
    _retry = 0
    while (1):
        try:
            p = subprocess.Popen(cmd, shell=True)
            stdout, stderr = p.communicate()
            ret = p.returncode
            if (ret == 0):
                return
            elif (ret == errno.EAGAIN and _retry < retry):
                _retry = _retry + 1
                time.sleep(1)
                continue
            else:
                #_derror("cmd " + cmd + " : " + os.strerror(ret))
                #exit(1)
                raise Exp(ret, "%s: %s" % (cmd, os.strerror(ret)))
        except KeyboardInterrupt as err:
            _dwarn("interupted")
            p.kill()
            exit(errno.EINTR)

def _str2list(str1):
    r = re.compile("\[[^\]]+]")
    res = r.findall(str1)
    if (len(res) == 0):
        return [str1]
    elif (len(res) > 1):
        ret = errno.EINVAL
        raise Exp(ret, os.strerror(ret))

    left = r.split(str1)
    if (len(left) != 2):
        ret = errno.EINVAL
        raise Exp(ret, os.strerror(ret))

    newlist = []
    res = res[0][1:-1]
    lst = res.split(',')
    for i in lst:
        if ('-' in i):
            r1 = i.split('-')
            r1 = range(int(r1[0]), int(r1[1]) + 1)
            newlist = newlist + r1
        else:
            newlist.append(int(i))

    newlist = list(set(newlist))
    result = []

    if (left[1] == ''):
        for i in newlist:
            result.append(left[0] + str(i))
    else:
        for i in newlist:
            result.append(str(i) + left[1])

    return result

def _strjoin(a1, a2):
    a3 = []
    for i in a1:
        for j in a2:
            a3.append(i + '.' + j)

    return a3
    
def _str2hosts1(str1):
    a1 = str1.split('.')

    if (len(a1) > 3):
        ret = errno.EINVAL
        raise Exp(ret, os.strerror(ret))
    
    a2 = []
    for i in a1:
        a2.append(_str2list(i))

    a3 = []
    if (len(a1) == 1):
        return a2[0]
    elif (len(a1) == 2):
        return _strjoin(a2[0], a2[1])
    else:
        return _strjoin(_strjoin(a2[0], a2[1]), a2[2])

def _isip(str1):
    try:
        m = re.match("^\d+\.\d+\.\d+\.\d+$", str1).group()
    except Exception, e:
        return False
    return True

def _str2hosts(list1):
    newlist = []
    for i in list1:
        if (_isip(i)):
            newlist = newlist + [i]
        else:
            newlist = newlist + _str2hosts1(i)

    return newlist

def _star2hosts(hostlist):
    newlist = []
    for i in hostlist:
        if (i.find('*') < 0):
            newlist.append(i)
            continue
        i = i.replace('.', '\.')
        i = i.replace('*', '.*')
        cmd = "grep %s /etc/hosts" % i;
        res = os.popen(cmd).read()
        for host in res.split('\n'):
            if (len(host) > 0 and host.strip()[0] != '#'):
                r = re.compile("^"+i+"$")
                m = r.match(host.split()[-1])
                if (m != None):
                    newlist.append(host.split()[-1])

    return newlist

def _hostname2ip(hostname):
    fd = open("/etc/hosts")
    hosts = fd.read()
    fd.close()
    for line in hosts.split('\n'):
        if line.strip().startswith('#') or line.strip() == '':
            continue
        items = line.split()
        if items[1] == hostname:
            return items[0]

    return None

def _check_hosts(hostlist, nohosts):
    for i in hostlist:
        if (nohosts):
            if not _isip(i):
                raise Exp(errno.EINVAL, "nohosts type only allow ip address, %s" % (i))
        else:
            if _isip(i):
                raise Exp(errno.EINVAL, "not nohosts type only allow hostname, %s" % (i))

            if len(i) > 64:
                raise Exp(errno.ENAMETOOLONG, "hostname %s to long, must less than 64" % (i))

    return 0

def _random_str(start, end):
    if start > end:
        raise Exp(errno.EINVAL, "random string %d bigger than %d"%(start, end))
    rand = random.randint(start, end)
    random_str = ""
    for i in range(rand):
        random_str += random.choice(' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~')

    return random_str

def _install_init(home):
    (distro, release, codename) = lsb.lsb_release()
    if (distro == 'Ubuntu'):
        src = os.path.join(home, "lich/admin/lichd_init_ubuntu")
        dst = "/etc/init.d/lichd"
        cmd = 'cp %s %s' % (src, dst)
        os.system(cmd)

        ldst = "/etc/rcS.d/S42lichd"
        cmd = 'ln -s %s %s' % (dst, ldst)
        if not os.path.exists(ldst):
            os.system(cmd)
    elif (distro == 'CentOS'):
        version = int(release.split(".")[0])
        if version <=  6:
            src = os.path.join(home, "lich/admin/lichd_init_centos")
            dst = "/etc/init.d/lichd"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            os.system("chkconfig --add lichd")
        elif version == 7:
            src = os.path.join(home, "lich/admin/lichd.service")
            dst = "/usr/lib/systemd/system/lichd.service"
            cmd = 'cp %s %s' % (src, dst)
            os.system(cmd)
            cmd = "chmod 754 %s" % (dst)
            os.system(cmd)
            cmd = "systemctl enable lichd.service"
            os.system(cmd)
        else:
            raise Exception("not support %s %s %s" % (distro, release, codename))
    else:
        _derror("not support %s %s %s" % (distro, release, codename))
        pass

def _search_hosts(hosts, regs):
    s = ''
    for i in hosts:
        s = s + i + '\n'

    newlist = []
    for i in regs:
        if (i[-1] == '*'):
            r = re.compile(i[:-1].replace('.', '\.') + "[^\\n]+")
            newlist = newlist + r.findall(s)
        else:
            newlist = newlist + [i];

    return list(set(newlist))

def _sshkey(host, passwd):
    while (1):
        os.system("if [ ! -f  /root/.ssh/id_dsa.pub ];then ssh-keygen -t dsa -P '' -f /root/.ssh/id_dsa ; fi")
        pub = _get_value('/root/.ssh/id_dsa.pub')
        private = _get_value('/root/.ssh/id_dsa')
        if "'" in pub or '"' in pub or "'" in private or '"' in private :
            os.system('rm -rf /root/.ssh/id_dsa*')
        else:
            break

    #remote_tmpkey = '/tmp/lich_authorized_key'
    try:
        cmd = "if [ ! -d /root/.ssh ]; then mkdir /root/.ssh; fi"
        cmd = cmd + " && echo '%s' >> /root/.ssh/authorized_keys" % (pub)
        cmd = cmd + " && echo '%s' > /root/.ssh/id_dsa.pub" % (pub)
        cmd = cmd + " && echo '%s' > /root/.ssh/id_dsa" % (private)
        cmd = cmd + " && chmod 0600 /root/.ssh/id_dsa"
        cmd = cmd + " && if [ -f /etc/init.d/iptables ];then /etc/init.d/iptables stop; setenforce 0;fi"
        #print (cmd)
        _exec_remote(host, cmd, 'root', passwd)
        #_exec_remote(host, "if [ ! -d /root/.ssh ]; then mkdir /root/.ssh; fi", "root", passwd)
        #_put_remote(host, "/root/.ssh/id_dsa.pub", remote_tmpkey, "root", passwd)
        #_exec_remote(host, "cat %s >> /root/.ssh/authorized_keys" % (remote_tmpkey) , "root", passwd)
    except socket.gaierror as err:
        raise Exp(err.errno, err.strerror)
    except SSHException as err:
        raise Exp(errno.EINVAL, str(err))

def _is_physic(dev):
    f = os.path.join("/sys/class/net/", dev, "device")
    return os.path.isdir(f)

def ___get_speed_iface(dev):
    s = _exec_pipe(["ethtool", '-i', dev], 1, False)
    d = _str2dict(s)
    if (d['driver'] in ['virtio_net']):
        _dwarn("%s is not recommended" % (d['driver']))
        return 1000

    speed = 0
    s = _exec_pipe(["ethtool", dev], 1, False)
    r = re.search("Speed:[^\n]+", s)
    if r is not None:
        r2 = re.search("\d+", r.group())
        if r2 is not None:
            speed = r2.group()

    return speed

def ___get_speed_ifaces(dev, ifaces):
    speeds = [(i, int(___get_speed_iface(i))) for i in ifaces if _is_physic(i)]
    speeds_lower = [x for (x, y) in speeds if y < 1000]

    if (len(speeds) == 0):
        _derror("these is no physic dev with " + dev)
        return 100

    if (speeds_lower):
        _derror("these devs is lower speed: " + ",".join(speeds_lower))
        return 100

    return 1000

def __get_speed_bonding(dev):
    #cat /proc/net/bonding/bond0 |grep 'Slave Interface'
    ifaces = []
    s = _exec_pipe(["cat", '/proc/net/bonding/%s'%(dev)], 1, False)
    pre = "Slave Interface:"

    for i in s.strip().split("\n"):
        if (i.strip().startswith(pre)):
            ifaces.append((i.split(":")[-1]).strip())

    return ___get_speed_ifaces(dev, ifaces)

def __get_speed_bridge(dev):
    #brctl show virbr0|awk '{print $NF}'
    ifaces = []
    s = _exec_pipe(["brctl", 'show', dev], 1, False)

    for i in s.strip().split("\n"):
        if (len(i.strip().split()) >= 1):
            ifaces.append(i.strip().split()[-1])
    ifaces = ifaces[1:]

    return ___get_speed_ifaces(dev, ifaces)

def __get_speed_openvswitch(dev):
    s = _exec_pipe(["ovs-vsctl", 'list-ifaces', dev], 1, False)
    ifaces = s.strip().split("\n")

    return ___get_speed_ifaces(dev, ifaces)

def _get_speed(dev):
    #return int(speed)
    s = _exec_pipe(["ethtool", '-i', dev], 1, False)
    d = _str2dict(s)
    if (d['driver'] in ['virtio_net']):
        _dwarn("%s is not recommended" % (d['driver']))
        return 1000

    if (d['driver'] == 'bonding'):
        return  __get_speed_bonding(dev)

    if (d['driver'] == 'bridge'):
        return  __get_speed_bridge(dev)

    if (d['driver'] == 'openvswitch'):
        return  __get_speed_openvswitch(dev)

    speed = ___get_speed_iface(dev)
    return speed

def _get_addr(dev):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mask = socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x891b, struct.pack('256s' , dev))[20:24])
    net = socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s' , dev))[20:24])
    s.close()
    return (net, mask)

def _test_ping(ip, p=False):
    try:
        status,out = commands.getstatusoutput('ping %s -c 2 -W 1'%ip)
        if status == 0:
            return True
        raise
    except Exception, e:
        return False

def _exchange_mask(mask):
    count_bit = lambda bin_str: len([i for i in bin_str if i== '1'])
    mask_splited = mask.split('.')
    mask_count = [count_bit(bin((int(i)))) for i in mask_splited]
    return sum(mask_count)

def _exchange_maskint(mask_int):
    bin_arr = ['0' for i in range(32)]
    for i in range(mask_int):
        bin_arr[i] = '1'
    tmpmask = [''.join(bin_arr[i * 8:i * 8 + 8]) for i in range(4)]
    tmpmask = [str(int(tmpstr, 2)) for tmpstr in tmpmask]
    return '.'.join(tmpmask)

def _get_allip():
    allip = {}
    res = _exec_pipe(['ip', 'addr'], 0, False)
    for line in res.splitlines():
        m = re.search('^\s*inet\s+(\d+\.\d+\.\d+\.\d+\/\d+)\s+.*', line)
        if m is not None:
            eth = m.group().split()[-1]
            allip[m.group(1)] = eth

    return allip

def _set_ip(ip, maskint, eth):
    try:
        cmd = 'ip -4 addr flush label %s && ip addr add %s/%s brd + dev %s'%(eth, ip, maskint, eth)
        res = _exec_shell(cmd, p=False)
    except Exception as e:
        _derror('set ip (%s/%s) to dev (%s) failed'%(ip, maskint, eth))
        raise

def _remove_ip(ip):
    allip = _get_allip()
    if ip in allip:
        res = _exec_pipe(['ifconfig', allip[ip], 'down'], 0, False)
        _dwarn("remove ip ( %s ) ( %s )"%(ip, allip[ip]))

def _get_network():
    #s = _exec_pipe(["ifconfig"], 1, False)
    #lst = re.findall(r'^[^ \n]+', s, re.M)
    s = _exec_pipe(["ip", 'link'], 1, False)
    lst = re.findall(r'^\d+: +[^ \n:]+', s, re.M)

    lst1 = {}
    for i in lst:
        #if (i[-1] == ':'):
        #    i = i[:-1]
        i = i.split(':')[1].strip()
        try:
            if '@' in i:
                i = i.split('@')[0]
            lst1[i] = _get_addr(i)
        except IOError as err:
            if (err.errno != errno.EADDRNOTAVAIL):
                _derror("get %s error %s" % (i, str(err)))
            continue
    return lst1

def _net_mask(net, mask):
    return _ntoa(_aton(net) & _aton(mask))

def _ntoa(n):
    return socket.inet_ntoa(struct.pack('I', n))

def _aton(a):
    return struct.unpack('I', socket.inet_aton(a))[0]

def ethernets_get_all():
    return os.listdir("/sys/class/net/")

def ethernet_info(eth):
    mac = None
    out = commands.getstatusoutput(
            "ip addr show %s|grep 'link\/ether'"%(eth))
    if out[0] == 0:
        mac  = out[1].strip().split()[1]

    ips = []
    out = commands.getstatusoutput(
            "ip addr show %s|grep 'inet' \
            |grep -v 'inet6'|awk '{print $2}'"%(eth))
    if out[0] == 0:
        ips  = out[1].strip().split()

    rb = None 
    out = commands.getstatusoutput(
            "ifconfig %s|grep 'RX packets' \
            |awk -F':' '{print $2}'|awk '{print $1}'"%(eth))
    if out[0] == 0:
        rb  = out[1].strip()

    tb = None 
    out = commands.getstatusoutput(
            "ifconfig %s|grep 'TX packets' \
            |awk -F':' '{print $2}'|awk '{print $1}'"%(eth))
    if out[0] == 0:
        tb  = out[1].strip()

    speed = None
    out = commands.getstatusoutput(
            "ethtool %s|grep Speed| awk '{print $2}'"%(eth))
    if out[0] == 0:
        speed  = out[1].strip()

    is_linked = 'no'
    out = commands.getstatusoutput(
            "ethtool %s|grep 'Link detected' \
            | awk -F':' '{print $2}'"%(eth))
    if out[0] == 0:
        is_linked  = out[1].strip()

    return {'ifname': eth, 'mac': mac, 'ips': ips, 
            'rx_bytes': rb, 'tx_bytes': tb, 
            'speed': speed, 'is_linked': is_linked}

def etchosts_update(hosts_file, ip, hostname):
    '''
    如果不存在该IP， 把该Ip的hostname写入到hosts_file中
    如果已经存在则更新
    '''
    lines = []
    ip_exist = False
    new_line = "%s %s\n"%(ip, hostname)

    if os.path.isfile(hosts_file):
        with open(hosts_file, 'r') as f:
            for line in f:
                if not line.strip():
                    lines.append(line)
                    continue

                if line.strip().startswith("#"):
                    lines.append(line)
                    continue

                ip_old, hostname_old = [x.strip() for x in line.split() if x.strip()]

                if ip == ip_old:
                    lines.append(new_line)
                    ip_exist = True
                else:
                    if hostname == hostname_old:
                        raise Exp(errno.EINVAL, "hostname %s double" % (hostname))
                    else:
                        lines.append(line)

    if not ip_exist:
        lines.append(new_line)
    
    os.system('echo "">%s'%(hosts_file))
    with open(hosts_file, 'w+') as f:
        for line in lines:
            f.write('%s'%(line))

def etchosts_delete(hosts_file, ip, hostname):
    '''
    通过Ip and hostname删除hosts_file的行
    '''
    lines = []
    ip_exist = False
    if os.path.isfile(hosts_file):
        with open(hosts_file, 'r') as f:
            for row,line in enumerate(f):
                if not line.strip():
                    lines.append(line)
                    continue

                if line.strip().startswith("#"):
                    lines.append(line)
                    continue

                ip_old, hostname_old = [x.strip() for x in line.split() if x.strip()]

                if ip == ip_old and hostname == hostname_old:
                    continue
                lines.append(line)

    
    os.system('echo "">%s'%(hosts_file))
    with open(hosts_file, 'w+') as f:
        for line in lines:
            f.write('%s'%(line))

def _read_cpu_usage():  
    """Read the current system cpu usage from /proc/stat."""  
    try:  
        fd = open("/proc/stat", 'r')  
        lines = fd.readlines()  
    finally:  
        if fd:  
            fd.close()  
    for line in lines:  
        l = line.split()  
        if len(l) < 5:  
            continue  
        if l[0].startswith('cpu'):  
            return l  
    return []  
  
def cpu_usage():  
    """ 
    get cpu avg used by percent 
    """  
    cpustr = _read_cpu_usage()
    if not cpustr:  
        return 0  

    usni1 = sum([float(x) for x in cpustr[1:8]])
    usn1 = sum([float(x) for x in cpustr[1:4]])
    time.sleep(1)

    cpustr = _read_cpu_usage()
    if not cpustr:  
        return 0

    usni2 = sum([float(x) for x in cpustr[1:8]])
    usn2 = sum([float(x) for x in cpustr[1:4]])
    rs = ((usn2-usn1) / (usni2-usni1))
    return "%.3f" % (rs)
    #return ((usn2-usn1) / (usni2-usni1)) * 100

def mutil_exec(func, args, timeout = None, timeout_args = None):
    #args = [[arg1, ...], ...]
    ts = []
    for i in args:
        t = threading.Thread(target=func, args=i)
        t._args = i
        ts.append(t)

    [t.start() for t in ts]
    if timeout is None:
        [t.join() for t in ts]
    else:
        [t.join(timeout) for t in ts]

    for t in ts:
        if t.is_alive():
            timeout_args.append(t._args)

def mutil_call_remote(hosts, cmd, timeout=120):
    def _exec_remote_warp(host, cmd, rs):
        stdin, stdout, stderr, exp = None, None, None, None
        try:
            stdout = _exec_remote(host, cmd)
        except Exception, e:
            exp = str(e)
        rs.append({'host': host, 'stdout': stdout,
            'stderr': stderr, 'exp': exp})

    ts = []
    rs = []
    for i in range(len(hosts)):
        t = threading.Thread(
                target=_exec_remote_warp, args=[hosts[i], cmd, rs])
        ts.append(t)

    [t.start() for t in ts]
    [t.join() for t in ts]
    return rs

def qos(qosnum):
    """
    it was danger: 'no unlock' and 'will exit', and 'change sys.argv'
    """
    lock_ok = False

    sys.qos_lock_fds = []
    if sys.argv[-1] == "--skipqos":
        sys.argv = sys.argv[:-1]
        return 0

    qosdir = "/dev/shm/lich4/qosdir"
    os.system("mkdir -p %s" % qosdir)
    for i in range(qosnum):
        lock_f = os.path.join(qosdir, str(i))
        lock_fd = open(lock_f, 'a')
        sys.qos_lock_fds.append(lock_fd)

    for i in sys.qos_lock_fds:
        try:
            fcntl.flock(i.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_ok = True
        except IOError, e:
            print >>sys.stderr, "qos warn: ", e, i.name
            continue

        print 'qos ok:', i.name
        break

    if not lock_ok:
        exit(errno.EBUSY)

def setlunattr(lich_home, lun, key, value):
    cmd = os.path.join(lich_home, "libexec/lichfs")
    try:
        (out_msg, err_msg) = _exec_pipe1([cmd, '--attrset', lun, key, value], 1, False)
    except Exp, e:
        raise Exp(e.errno, "attrset fail")


def getlunattr(lich_home, lun, key):
    cmd = os.path.join(lich_home, "libexec/lichfs")
    try:
        (out_msg, err_msg) = _exec_pipe1([cmd, '--attrget', lun, key], 1, False)
        return out_msg
    except Exp, e:
        if e.errno == 126: #ENOKEY error
            if key == "snapshot_switch":
                default_value = "off"
            elif key == "snapshot_reserve":
                default_value = "1h-1,1d-1,1w-1"
                setlunattr(lich_home, lun, key, default_value)
            else:
                raise Exp(e.errno, "attrget fail")
            return default_value
        else:
            raise Exp(e.errno, "attrget fail")


def main():
    print ('just for test')
    #lock_fd = _lock_file("/tmp/test/fusionstack/balance_lock", 20)

    retry = 10
    while (retry > 0):
        print(retry)
        retry = retry - 1
        time.sleep(1)


if __name__ == '__main__':
    #print cpu_usage()
    #print _get_speed("eth1")
    #print  __get_speed_openvswitch("br0")
    #print  __get_speed_bridge("virbr0")
    #print  __get_speed_bonding("bond0")
    #hosts = ['datacenter1.rack1.node1', 'datacenter1.rack2.node1''datacenter1.rack3.node1', 'datacenter1.rack4.node1']
    #for i in range(200):
        #hosts.append('datacenter1.rack1.node1%s'%(i))
    #r = mutil_call_remote(hosts, 'pwd')
    #print '-'*10
    #print r
    #main()
    print _human_unreadable("1GB")
    print _human_unreadable("1G", True)
    print _human_readable(1048500)
    print _human_readable(1048500, True)
    exit(1)
    print _ntoa(_aton("192.168.1.138"))
    print _ntoa(_aton("192.168.1.128"))

    for i in range(255):
        ip = "192.168.1.%s" % (str(i))
        print ip, _aton(ip) & _aton("255.255.255.0"), _aton(ip) & _aton("255.255.255.192")
        #print ip, _aton(ip), _aton("255.255.255.0"), _aton(ip), _aton("255.255.255.192"), type(_aton(ip))
        #print ip, _aton(ip) & 16777215, _aton(ip) & 67108863

    print _aton("192.168.1.128") & 16777215
    print _aton("192.168.1.138") & 16777215
