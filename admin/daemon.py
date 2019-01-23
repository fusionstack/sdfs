#!/usr/bin/env python2.7

import sys, os, time, atexit
import fcntl, errno
from signal import SIGTERM 

def _lock_file(key, is_exp):
    key = os.path.abspath(key)
    parent = os.path.split(key)[0]
    os.system("mkdir -p " + parent)

    #sys.stderr.write("lock failed: %d (%s)\n" % (e.errno, e.strerror))
    #_dmsg("lock " + key)
    lock_fd = open(key, 'a')

    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError as err:
        #print ("errno %d" % err.errno)
        if is_exp:
            raise
        elif err.errno == errno.EAGAIN:
            sys.stderr.write("lock %s failed: %d (%s)\n" % (key, err.errno, err.strerror))
            #x_dwarn("%s locked")
            exit(err.errno)
        else:
            raise

    lock_fd.truncate(0)
    #s = str(os.getpid()) + '\n'
    #lock_fd.write(s)
    #lock_fd.flush()
    return lock_fd

def _try_lock_file(key):
    key = os.path.abspath(key)
    if not os.path.exists(key):
        raise Exp(errno.EPERM, key + ' not exist')

    lock_fd = open(key, 'a')

    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)

class Daemon(object):
    """
    A generic daemon class.
    
    Usage: subclass the Daemon class and override the run() method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null', name="noname"):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.name = name

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
        except OSError, e: 
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 

        print('start:%s, pid:%s' % (self.name, self.pidfile))
    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+', 0)
        se = file(self.stderr, 'a+', 0)
        #print('out: ' + self.stdout)

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno()) 
   
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        open(self.pidfile,'w').write("%s\n" % pid)
        #os.system("echo %s ,%s> /var/run/docyou/echofile"%(self.pidfile, pid) )

    
    def delpid(self):
        os.remove(self.pidfile)

    def start(self, is_exp=False):
        """
           Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        """
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            message = "%s is running\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)
        """

        pid = _lock_file(self.pidfile, is_exp)

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self, is_exp=False):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            if is_exp:
                raise Exp(errno.ENOINT, 'daemon not running')
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                sys.stderr.write(err)
                sys.exit(1)

    def stat(self):
        """
           Status of the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        if not os.path.exists(self.pidfile):
            return False

        try:
            _try_lock_file(self.pidfile)
        except Exception, e:
            return True

        return False

    def restart(self):
        """
            Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
            You should override this method when you subclass Daemon. It will be called after the process has been
            daemonized by start() or restart().
        """
