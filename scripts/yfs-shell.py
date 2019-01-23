#!/usr/bin/env python

import os
import sys
import time
import socket
import re
import string
import readline
import ConfigParser
from optparse import OptionParser

sys.path.append("/sysy/yfs/app/lib/python")
from color_output import red, yellow, blue, \
        green, purple, darkgreen, darkyellow, darkblue, \
        turquoise, fuchsia

YFS_PREFIX = "/sysy/yfs/"

class YFSConfig:
    def __init__(self, yfscfg):
        self.yfscfg = yfscfg
        self._parse_configure()
    def _get_value(self, conf, section, option_key, iscds=False):
        """Get option value in section, if no
        section in configure file, raise NoOptionError,
        else if no option_key, ingore and
        return None, default return the key.
        """
        try:
            value = conf.get(section, option_key)
        # if the entry was commented, ingore.
        except ConfigParser.NoOptionError:
            return []
        except:
            raise

        if iscds:
            return self._split_cdsvalue(value)
        else:
            return self._split_multvalue(value)


    def _split_multvalue(self, value):
        if value:
            return [i.strip() for i in value.split(',') if i]
        else:
            return []


    def _split_cdsvalue(self, value):
        if value:
            cdses = []
            tmp = [i.strip() for i in value.split(',') if i]

            for i in tmp:
                cds = i.split(':')
                cdses.append({
                    'host' : cds[0].strip(' ()'),
                    'num' : cds[1].strip(' ()')
                    })
            return cdses
        else:
            return value


    def _parse_configure(self):
        """read all host address from configure file.
        """
        yfsconf = ConfigParser.RawConfigParser()
        yfsconf.read(self.yfscfg)

        self.mds = self._get_value(yfsconf, 'master', 'mds')
        self.yftp = self._get_value(yfsconf, 'client', 'yftp')
        self.yiscsi = self._get_value(yfsconf, 'client', 'yiscsi')
        self.yweb = self._get_value(yfsconf, 'client', 'yweb')
        self.net = self._get_value(yfsconf, 'net', 'net')
        self.mask = self._get_value(yfsconf, 'net', 'mask')
        self.log = self._get_value(yfsconf, 'backup', 'log')
        self.cds = self._get_value(yfsconf, 'slaves', 'cds', True)
        self.proxy = self._get_value(yfsconf,'client','proxy')

        print 'net:\t%s'  % self.net
        print 'mask:\t%s' % self.mask
        print 'mds:\t%s'  % self.mds
        print 'cds:\t%s'  % self.cds
        print 'yiscsi:\t%s' % self.yiscsi
        print 'yftp:\t%s' % self.yftp
        print 'yweb:\t%s' % self.yweb
        #print 'proxy:\t%s' % self.proxy

class YFSManager(YFSConfig):
    def __init__(self, confile):
        self.tmp_script = '/tmp/start_script'

        YFSConfig.__init__(self, confile)
        self.sbin = os.path.join(YFS_PREFIX, 'app/sbin')
        self.bin = os.path.join(YFS_PREFIX, 'app/bin')
        self.lib = os.path.join(YFS_PREFIX, 'app/lib')
        self.startup_script = os.path.join(self.sbin, 'local_startup.py')
        self.startup_script_tpl = os.path.join(self.sbin, 'local_startup.tpl')

        self.hosts = []
        try:
            self.hosts += self.mds
            self.hosts += self.yiscsi
            self.hosts += self.yweb
            self.hosts += self.yftp
            self.hosts +=self.proxy
            self.hosts += [i['host'] for i in self.cds]
        except TypeError:
            raise

        self.hosts = list(set([i for i in self.hosts if i]))
        print self.hosts

        self.services = ['mds', 'cds', 'yiscsi', 'yweb',
                'yftp', 'yfs', 'ydog', 'proxy', 'all']

    def _conf_amend(self):
        yfscfg_path = os.path.join(YFS_PREFIX, 'etc/yfs-shell.conf')
        yfsconf = ConfigParser.RawConfigParser()
        yfsconf.read(yfscfg_path)

    def fetch_job(self, cmd):
        cmd = cmd.strip().split(None, 2)

        if len(cmd) == 1 and cmd[0] not in ['show', 'merge', 'help']:
            print '%s what ...' % cmd[0]
            return

        service = None
        host = None
        if cmd[0] in ['start', 'stop']:
            if '@' in cmd[1]:
                (service, host) = cmd[1].split('@')
                if host not in self.hosts:
                    print 'No such a host %s' % host
                    return
            else:
                service = cmd[1]
                host = 'all'

            if service not in self.services:
                print 'bad service %s' % service
                return

            if cmd[0] == 'start' and len(cmd) == 2:
                self._start_srv(host, service)
            elif cmd[0] == 'stop' and len(cmd) <= 3:
                now = False
                if len(cmd) == 3:
                    if cmd[2] == 'now':
                        now = True
                    else:
                        print 'Oops, bad command'
                        return
                #print host, service;
                self._stop_srv(host, service, now)
            else:
                print 'Oops, bad command'
                return

        elif cmd[0] == 'deploy':
            if cmd[1] == 'ssh':
                self._deploy_ssh()
            elif cmd[1] == 'yfs':
                if len(cmd) == 3 and os.path.exists(cmd[2]):
                    self._conf_amend()
                    self._deploy_yfs(cmd[2])
                else:
                    print 'Oop, give right path: app/path/to/update'
                    return
            else:
                print 'Oop, give right operate: \'ssh\' \'yfs\''
                return
        elif cmd[0] == 'log':
            if cmd[1] in ['clean', 'backup', 'show']:
                self._log_operate(cmd[1])
            else:
                print 'Oop, give right operate: \'backup\' \'clean\' \'show\''
                return
        elif cmd[0] == 'show':
            self._show_srv()
        elif cmd[0] == 'merge':
	        self._merge_cds()
        elif cmd[0] == 'help':
            self._show_help()
        else:
            print 'command error %s %s' % (cmd[0], cmd[1])
            return


    def _start_srv(self, host, prog):
        if prog == 'all':
            if self.mds:
                for host in self.mds:
                    cmd = "ssh root@%s '%s --start %s --host %s'" % (
                        host, self.startup_script, 'mds', host
                        )
                    os.system(cmd)

            if self.cds:
                for host in [i['host'] for i in self.cds]:
                    cmd = "ssh root@%s '%s --start %s --host %s'" % (
                            host, self.startup_script, 'cds', host
                            )
                    os.system(cmd)

        else:
            if host == 'all':
                host = self.hosts
            else:
                host = [host]

            for i in host:
                cmd = "ssh root@%s '%s --start %s --host %s'" % (
                        i, self.startup_script, prog, i
                        )
                os.system(cmd)

    def _stop_srv(self, host, prog, now):
        if host == 'all':
            host = self.hosts
        else:
            host = [host]
        verify = None

        if prog == 'all':
            if not now:
                try:
                    verify = raw_input(darkyellow("Are you sure to stop all? (y/n) ")).lower()
                    if verify == 'y':
                        print ">>> Waiting 3 seconds before stoping..."
                        print ">>> (Control-C to abort)..."
                        print ">>> Stoping in:",

                        for i in range(3, 0, -1):
                            print red('%d') % i,
                            sys.stdout.flush()
                            time.sleep(1)
                        print
                    else:
                        return
                except KeyboardInterrupt:
                    print
                    return

            if self.proxy:
                for host in self.proxy:
                    cmd = "ssh root@%s '%s --stop %s --host %s'" % (
                            host, self.startup_script, 'proxy', host
                        )
		    os.system(cmd)
            if self.yiscsi:
                for host in self.yiscsi:
                    cmd = "ssh root@%s '%s --stop %s --host %s'" % (
                            host, self.startup_script, 'yiscsi', host
                            )
                    os.system(cmd)
            if self.cds:
                for host in [i['host'] for i in self.cds]:
                    cmd = "ssh root@%s '%s --stop %s --host %s'" % (
                            host, self.startup_script, 'cds', host
                            )
                    os.system(cmd)
            if self.mds:
                for host in self.mds:
          	    cmd = "ssh root@%s '%s --stop %s --host %s'" % (
                        host, self.startup_script, 'mds', host
                        )
                    os.system(cmd)

        else:
            for i in host:
                cmd = "ssh root@%s '%s --stop %s --host %s'" % (
                        i, self.startup_script, prog, i
                        )
                os.system(cmd)

    def _show_srv(self):
        for host in self.hosts:
            cmd = "ssh root@%s '%s --show --host %s'" % (
                    host, self.startup_script, host
                    )
            os.system(cmd)

    def _deploy_ssh(self):
        gen_sshkey = os.path.join(self.bin, 'gen_sshkey.sh')
        os.system(gen_sshkey)

        remote_tmpkey = '/tmp/yfs_authorized_key'
        for host in self.hosts:
            print green('deploy ssh %s, please input passwd three times') % host
            mk_sshdir = 'ssh root@%s "if [ ! -d /root/.ssh ]; then mkdir /root/.ssh; fi"' % host
            cp_pubkey = 'scp /root/.ssh/id_dsa.pub root@%s:%s' % (host, remote_tmpkey)
            mv_pubkey = 'ssh root@%s "cat %s >> /root/.ssh/authorized_keys"' % (host, remote_tmpkey)

            os.system(mk_sshdir)
            os.system(cp_pubkey)
            os.system(mv_pubkey)

    def _deploy_yfs(self, up_path):
        mds_cfg = os.path.join(YFS_PREFIX, 'etc/mds_node')
        mds_num = 'echo %s > %s' % (len(self.mds), mds_cfg)
        os.system(mds_num)
        for host in self.mds:
            os.system('echo %s >> %s' % (host, mds_cfg))

	cmd = "sed -i 's/mds_count.*/mds_count\ %s\;/g' /sysy/yfs/etc/yfs.conf" % (len(self.mds))
	os.system(cmd)
 	cds_num = 0
	for host in self.cds:
	    cds_num = cds_num + int(host['num'])
	cmd = "sed -i 's/cds_count.*/cds_count\ %s\;/g' /sysy/yfs/etc/yfs.conf" % cds_num
	os.system(cmd)

	client_num = len(self.proxy) + len(self.yftp) + len(self.yweb) + len(self.yiscsi)
	cmd = "sed -i 's/client_count.*/client_count\ %s\;/g' /sysy/yfs/etc/yfs.conf" % client_num
	os.system(cmd)

        bak_dir = '/sysy/yfs/YFS_APP_BACK'
        bak_day = '%s/%s' % (bak_dir, time.strftime('%Y-%m-%d'))
        bak_time = time.strftime('%H:%M:%S')
        bak_dir = '%s/%s\(%s\)' % (bak_day, 'app', bak_time)
        tmp_update = '/tmp/app-tmp-%s' % bak_time

        for host in self.hosts:
            print 'deploy yfs to %s' % host
            # backup and update app directory
            print '%s: Back app to "%s"' % (host, bak_dir)

            cmd = 'scp -r %s root@%s:%s > /dev/null' % (
                    up_path,
                    host,
                    tmp_update
                    )
            os.system(cmd)
            cmd = "ssh root@%s \"if [ ! -e %s ]; then \
                                    if [ -e /sysy/yfs/app ]; then \
                                        [[ -e %s ]] && : || mkdir -p %s; \
                                        mv /sysy/yfs/app %s; fi; \
                                    mv %s /sysy/yfs/app; fi\"" % (
                    host,
                    bak_dir,
                    bak_day,
                    bak_day,
                    bak_dir,
		            tmp_update
                    )
            os.system(cmd)

            # update etc directory
            cmd = 'scp -r %s root@%s:%s > /dev/null' % (
                    '/sysy/yfs/etc',
                    host,
                    '/sysy/yfs'
                    )
            os.system(cmd)

            # adjust ld.so.conf.d
            clibcmd = "ssh root@%s 'uname -a | grep -Eq gentoo && { echo LDPATH=%s>/etc/env.d/50sysy; env-update 2>/dev/null;} || { echo %s>/etc/ld.so.conf.d/sysy.conf; echo /usr/local/db-4.8/lib >>/etc/ld.so.conf.d/sysy.conf; ldconfig 2>/dev/null;}'" % (
                    host,
                    self.lib,
                    self.lib
                    )
            os.system(clibcmd)

            # adjust hosts file
            hostcmd = "grep -Eq '^[[:space:]]*(((2[0-4][0-9]|25[0-5]|[01]?[0-9]?[0-9])\.){3}(2[0-4][0-9]|25[0-5]|[01]?[0-9]?[0-9]))([[:space:]]+)mds1([[:space:]]*)$' /etc/hosts &&             \
                            sed -i 's/^\(\s*\)\(\(\(2[0-4][0-9]\|25[0-5]\|[01]\?[0-9]\?[0-9]\)\.\)\{3\}\(2[0-4][0-9]\|25[0-5]\|[01]\?[0-9]\?[0-9]\)\)\(\s\+\)mds1\(\s*\)$/%s mds1/g' /etc/hosts \
                            || echo '%s mds1' >> /etc/hosts" % (self.mds[0], self.mds[0])
            cmd = "ssh root@%s \"%s\"" % (host, hostcmd)
            os.system(cmd)

        self._local_startup()


    def _local_startup(self):
        """generate and scp local_startup.py
        """
        for host in self.hosts:
            script = open(self.startup_script_tpl, 'r').read()

            script = script.replace('$HOST_NAME', host)

            if host in self.mds:
                script = script.replace('$ENABLE_MDS', 'True')

            for i in self.cds:
                if i['host'] == host:
                    script = script.replace('$ENABLE_CDS', 'True')
                    script = script.replace('$CDS_NUM', str(i['num']))

            script = script.replace('$CDS_NUM', '0')

            if self.yiscsi and host in self.yiscsi:
                script = script.replace('$ENABLE_YISCSI', 'True')

            if self.yweb and host in self.yweb:
                script = script.replace('$ENABLE_YWEB', 'True')

            if self.yftp and host in self.yftp:
                script = script.replace('$ENABLE_YFTP', 'True')

            if self.proxy and host in self.proxy:
                script = script.replace('$ENABLE_PROXY', 'True')

            if self.net:
                script = script.replace('$NET', self.net[0])
            else:
                script = script.replace('$NET', '')

            if self.mask:
                script = script.replace('$MASK', self.mask[0])
            else:
                script = script.replace('$MASK', '')

            for i in ['$ENABLE_MDS', '$ENABLE_SMDS', '$ENABLE_CDS',
                    '$ENABLE_YISCSI', '$ENABLE_YWEB', '$ENABLE_YFTP', '$ENABLE_PROXY']:
                if i in script:
                    script = script.replace(i, 'False')

            local_script = '%s_%s' % (self.tmp_script, host)
            open(local_script, 'w').write(script)

            cmd = 'scp %s root@%s:%s' % (
                    local_script,
                    host,
                    self.startup_script
                    )
            os.system(cmd)

            cmd = "ssh root@%s 'chmod +x %s'" % (
                    host,
                    self.startup_script
                    )
            os.system(cmd)

    def _log_operate(self, operate):
        """backup or clean log
        """
        if operate == 'backup':
            bak_time = time.strftime('%Y%m%d%H%M%S')
            bak_log_dir = '%s%s%s' % (self.log[0], os.sep, bak_time)

            for host in self.hosts:
                print green('backup log %s') % host
                cmd = 'ssh root@%s \'if [ ! -e %s ];then mkdir -p %s;fi; find /var/log/uss -maxdepth 1 -name \"*.log\" | xargs -i cp {} %s; find /var/log/uss -maxdepth 1 -name \"*.log\" | xargs -i echo \"cat /dev/null > \" {} | sh\'' % (host, bak_log_dir, bak_log_dir, bak_log_dir)
                os.system(cmd)
        elif operate == 'clean':
            for host in self.hosts:
                print green('clean log %s') % host
                cmd = 'ssh root@%s \'find /var/log/uss -maxdepth 1 -name \"*.log\" | xargs -i echo \"cat /dev/null > \" {} | sh\'' % host
                os.system(cmd)
        elif operate == 'show':
            for host in self.hosts:
                print green('show log %s') % host
                cmd = 'ssh root@%s \'find /var/log/uss -maxdepth 1 -name \"*.log\" | xargs -i echo \"du -hs \" {} | sh\'' % host
                os.system(cmd)

    def _show_help(self):
        help = " CMD            ARGS            ARGS            DESCRIPTOR                              \n" \
               " ==============================================================================         \n" \
               " deploy         ssh                             Deploy ssh key to every server          \n" \
               "                yfs             path/to/app     Deploy app and etc to every server      \n" \
               "                                                                                        \n" \
               " start/stop     mds                             Start/stop mds                          \n" \
               "                cds                             Start/stop cds                          \n" \
               "                yiscsi                            Start/stop yiscsi                         \n" \
               "                yweb                            Start/stop yweb                         \n" \
               "                yftp                            Start/stop yftp                         \n" \
               "                proxy                           Start/stop proxy                        \n" \
               "                all                             Start/stop mds cds                 \n" \
               "                                                                                        \n" \
               " log            clean                           Truncate log                            \n" \
               "                back                            Backup and truncate log                 \n" \
               "                                                                                        \n" \
               " show                                           Show the status of YFS                  \n" \
               "                                                                                        \n" \
               " merge                                          Merge cds jnl                           \n" \
               "                                                                                        \n" \
               " help                                           Show this message                       \n"
        print help

class YFShell:
    PROMPT = red('root') + fuchsia('@yfs # ')

    def __init__(self):
        self.sbin = os.path.join(YFS_PREFIX, 'app/sbin')
        self.bin = os.path.join(YFS_PREFIX, 'app/bin')
        self.cmds = os.listdir(self.bin)
        self.cmds += ['start', 'stop', 'deploy', 'show', 'merge', 'log', 'help']

        confile = os.path.join(YFS_PREFIX, 'etc/yfs-shell.conf')
        self.yfsmanager = YFSManager(confile)

    def _completer(self, word, index):
        if '/' in word:
            matches = self._path_completer(word)
        else:
            matches = [c for c in self.cmds if c.startswith(word)]
        try:
            return matches[index] + ''
        except IndexError:
            pass

    def _path_completer(self, text):
        (head, tail) = os.path.split(text)
        matches = []
        if head and os.path.exists(head):
            if head == '/':
                matches = [head + f if os.path.isfile(os.path.join(head, f)) else head + f + os.sep for f in os.listdir(head) if f.startswith(tail)]
            else:
                matches = [head + os. sep + f if os.path.isfile(os.path.join(head, f)) else head + os.sep + f + os.sep for f in os.listdir(head) if f.startswith(tail)]
        return matches

    def start(self):
        readline.set_completer(self._completer)
        readline.set_completer_delims(''.join([c for c in readline.get_completer_delims() if c != '/' and c != '-']))
        readline.parse_and_bind("tab: complete")
        cmd = None

        while True:
            try:
                cmd = raw_input(self.PROMPT)
            except EOFError:
                print 'exit'
                EXIT(0)
            except KeyboardInterrupt:
                print 'type ctrl-D to exit yfs shell'
                continue

            if cmd:
                self._parse_cmd(cmd)

    def _parse_cmd(self, cmd):
        if cmd.startswith('start') or \
                cmd.startswith('stop') or \
                cmd.startswith('show') or \
                cmd.startswith('merge') or \
                cmd.startswith('help') or \
                cmd.startswith('log') or \
                cmd.startswith('deploy'):
            self.yfsmanager.fetch_job(cmd)
        else:
            self._shell(cmd)

    def _shell(self, cmd):
        command = cmd.strip().split()[0]
        command = os.path.join(self.bin, command)

        if os.path.exists(command):
            os.system(os.path.join(self.bin, cmd))
        else:
            print 'yfs-sh: %s: command not found' % cmd


if __name__ == '__main__':
    argc = len(sys.argv)

    if os.geteuid() != 0:
        print red('Need to be root :(')
        EXIT(1)

    if argc == 1:
        test = YFShell()
        test.start()
    else:
        print 'invalid option'
