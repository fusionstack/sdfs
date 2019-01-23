#!/usr/bin/env python

import os
import sys
import signal
import getopt
import time
from subprocess import Popen, PIPE
import ConfigParser
from optparse import OptionParser

sys.path.append('/sysy/yfs/app/lib/python')
from color_output import red, yellow, blue, green, darkgreen

hostname = '$HOST_NAME'

progs_enable = {
        'mds'  : $ENABLE_MDS, 
        'cds'  : $ENABLE_CDS,
        'yiscsi' : $ENABLE_YISCSI,
        'yweb' : $ENABLE_YWEB,
        'yftp' : $ENABLE_YFTP, 
        'proxy': $ENABLE_PROXY 
        }

def enable_required(prog):
    def _enable_required(func):
        def check_enable(*args, **argvkw):
                if not progs_enable[prog]:
                    return
                else:
                    new_fun = func(*args, **argvkw)
                    return new_fun
        return check_enable
    return _enable_required


class YFS:
    def __init__(self, host):
        self.net = '$NET'
        self.mask = '$MASK'
        self.cds_number = $CDS_NUM
        self.host = host

        self.sbin_path = '/sysy/yfs/app/sbin'
        self.bin_path = '/sysy/yfs/app/bin'
        self.dmsg('[ %s ]\n' % darkgreen(self.host))

    def _is_ppid(self,re):
        p1 = Popen(['ps', '-ef'], stdout=PIPE)
        p2 = Popen(['grep', re], stdin=p1.stdout, stdout=PIPE)
        p3 = Popen(['grep', '-v', 'grep'], stdin=p2.stdout, stdout=PIPE)
        p4 = Popen(['awk', '{print $3}'], stdin=p3.stdout, stdout=PIPE)
        flag = [int(i.strip()) for i in p4.communicate()[0].split('\n') if i]
	#self.dmsg("in is_ppid\n")
	#self.dmsg("re is %d\n" % int(re))
        if 1 in flag:
            return True
        else:
            return False

    def dmsg(self, msg):
        sys.stdout.write(msg)
        sys.stdout.flush()

    def _bind_net(self, cmd):
        if self.net and self.mask:
            cmd += ' -h %s -m %s' % (self.net, self.mask)
        return cmd

    def _start_mds(self, type):
        no = 0
        mdsprog = os.path.join(self.sbin_path, 'yfs_mds')
        mdscmd = '%s -n %d ' % (mdsprog, no)
        mdscmd = self._bind_net(mdscmd)

        os.system(mdscmd)
        time.sleep(0.3)

        mds = 'mds'

        pids = self.__get_mdspid(no)
        if len(pids) == 1:
            self.dmsg('%s start %s %s\n' % (
                green('*'), mds, ('['+green('ok')+']').rjust(20)
                ))
        else:
            self.dmsg('%s start %s %s\n' % (
                red('*'), mds, ('['+red('ok')+']').rjust(20)
                ))

    def __get_normpid(self, prog):
        re = "%s.*" % prog
        pids = self._get_progpid(re)
        return pids

    def __get_mdspid(self, no):
        re = 'yfs_mds.* -n 0'  
        pids = self._get_progpid(re)
        return pids

    def __get_cdspid(self, cds_no):
        re = 'yfs_cds.* -n %d' % cds_no
        pids = self._get_progpid(re)
        return pids

    def _start_normsrv(self, prog):
        service = os.path.join(self.sbin_path, prog)
	service = self._bind_net(service)
        os.system(service)
        time.sleep(0.3)

        pids = self.__get_normpid(prog)
        if len(pids) == 1:
            self.dmsg('%s start %s %s\n' % (
                green('*'), prog, ('['+green('ok')+']').rjust(20)
                ))
        else:
            self.dmsg('%s start %s %s\n' % (
                red('*'), prog, ('['+red('ok')+']').rjust(20)
                ))

    def _stop_mds(self, no):
        pids = self.__get_mdspid(no)
        for pid in pids:
            try:
                os.kill(pid, signal.SIGUSR2)
            except IOError:
                pass

        mds = 'mds'

        pids = self.__get_mdspid(no)
        if len(pids) == 0:
            self.dmsg('%s stop %s %s\n' % (
                green('*'), mds, ('['+green('ok')+']').rjust(20)
                ))
        else:
            self.dmsg('%s stop %s %s\n' % (
                red('*'), mds, ('['+red('ok')+']').rjust(20)
                ))

    def _get_progpid(self, re):
        p1 = Popen(['ps', 'aux'], stdout=PIPE)
        p2 = Popen(['grep', re+'$'], stdin=p1.stdout, stdout=PIPE)
        p3 = Popen(['grep', '-v', 'grep'], stdin=p2.stdout, stdout=PIPE)
        p4 = Popen(['awk', '{print $2}'], stdin=p3.stdout, stdout=PIPE)

        pid = [int(i.strip()) for i in p4.communicate()[0].split('\n') if i]
        pid.sort() # kill father first
        return pid

    def _start_oss(self):
        ossprog = os.path.join(self.sbin_path, 'oss')
        cmd = '%s -s' % ossprog
        os.system(cmd)

    @enable_required('mds')
    def start_mds(self):
        if self.__get_mdspid(0): 
            sys.stderr.write('MDS already running, stop it first\n')
        else:
            self._start_mds(type=0)

    @enable_required('cds')
    def start_cds(self):
        cdsprog = os.path.join(self.sbin_path, 'yfs_cds')

        for i in range(0, self.cds_number):
            if self.__get_cdspid(i):
                sys.stderr.write('CDS %d already running, stop it first\n' % i)
            else:
                cdscmd = '%s -n %d' % (cdsprog, i)
                cdscmd = self._bind_net(cdscmd)
                os.system(cdscmd)
                time.sleep(0.3)

                pids = self.__get_cdspid(i)
                if len(pids) == 1:
                    self.dmsg('%s start cds -n %d %s\n' % (
                        green('*'), i, ('['+green('ok')+']').rjust(20)
                        ))
                else:
                    self.dmsg('%s start cds -n %d %s\n' % (
                        red('*'), i, ('['+red('ok')+']').rjust(20)
                        ))

    @enable_required('yiscsi')
    def start_yiscsi(self):
        if self.__get_normpid('yiscsi_server'):
            sys.stderr.write('YISCSI already running, stop it first\n')
        else:
            self._start_normsrv('yiscsi_server')

    @enable_required('yweb')
    def start_yweb(self):
        if self.__get_normpid('yweb_server'):
            sys.stderr.write('YWEB already running, stop it first\n')
        else:
            self._start_normsrv('yweb_server')
        
    @enable_required('yftp')
    def start_yftp(self):
        if self.__get_normpid('yftp_server'):
            sys.stderr.write('YFTP already running, stop it first\n')
        else:
            self._start_normsrv('yftp_server')

    @enable_required('proxy')
    def start_proxy(self):
        if self.__get_normpid('proxy_server'):
            sys.stderr.write('PROXY already running, stop it first\n')
        else:
            self._start_normsrv('proxy_server')

    def _stop_oss(self):
        ossprog = os.path.join(self.sbin_path, 'oss')
        cmd = '%s -k' % ossprog
        os.system(cmd)
        
    @enable_required('mds')
    def stop_mds(self):
        self._stop_mds(1)

    @enable_required('cds')
    def stop_cds(self):
        re = 'yfs_cds.*'
	pidf = [];
	pidc = [];
        for i in range(0, self.cds_number):
            pids = self.__get_cdspid(i)
            for pid  in pids:
                try:
		    os.kill(pid, signal.SIGUSR2)
#	            if self._is_ppid(str(pid)):
#		        #self.dmsg('is father\n')
#                        #os.kill(pid, signal.SIGKILL)
#		        #time.sleep(0.3)
#			pidf.append(pid)
#		    else:
#		        if self._is_ppid(str(pid)) == False:
#			    #self.dmsg('is child\n')
#		            #os.kill(pid,signal.SIGTERM)
#			    pidc.append(pid)
#
                    
                except OSError:
                    pass
#        for pid in pids:
#	    if pid in pidf:
#	        os.kill(pid, signal.SIGKILL)
#            if pid in pidc:
#	        os.kill(pid, signal.SIGTERM)

            pids = self.__get_cdspid(i)
            if len(pids) == 0:
                self.dmsg('%s stop cds -n %d %s\n' % (
                            green('*'), i, ('['+green('ok')+']').rjust(20)
                            ))
            else:
                self.dmsg('%s stop cds -n %d %s\n' % (
                            red('*'), i, ('['+red('ok')+']').rjust(20)
                            ))

    def _stop_normsrv(self, prog):
        pids = self.__get_normpid('%s_server' % prog)
	pidf = []
	pidc = []
        for pid in pids:
            try:
		os.kill(pid, signal.SIGUSR2)
#	        #self.dmsg('in stop \n');
#	        if self._is_ppid(str(pid)):
#                    #os.kill(pid, signal.SIGKILL)
#		    #time.sleep(0.3)
#		    pidf.append(pid)
#		else:
#		    if self._is_ppid(str(pid)) == False:
#		        #os.kill(pid,signal.SIGTERM)
#			pidc.append(pid);
		    
            except OSError:
                pass
#       for pid in pids:
#            if pid in pidf:
#	        os.kill(pid, signal.SIGKILL)
#            if pid in pidc:
#	        os.kill(pid, signal.SIGTERM)

        time.sleep(0.3)
        pids = self.__get_normpid('%s_server'% prog)
        if len(pids) == 0:
            self.dmsg('%s stop %s %s\n' % (
                green('*'), prog, ('['+green('ok')+']').rjust(20)
                ))
        else:
            self.dmsg('%s stop %s %s\n' % (
                red('*'), prog, ('['+red('ok')+']').rjust(20)
                ))
    @enable_required('yiscsi')
    def stop_yiscsi(self):
        self._stop_normsrv('yiscsi')

    @enable_required('yweb')
    def stop_yweb(self):
        self._stop_normsrv('yweb')

    @enable_required('yftp')
    def stop_yftp(self):
        self._stop_normsrv('yftp')

    @enable_required('proxy')
    def stop_proxy(self):
        self._stop_normsrv('proxy')

    def _show_mds(self, type):
        mds = 'mds'

        pids = self.__get_mdspid(type)
        if len(pids) == 1:
            print green('*'), '%s: started' % mds
        else:
            print red('*'), '%s: stoped' % mds

    def _show_normsrv(self, prog):
        service = '%s_server' %  prog
        pids = self.__get_normpid(service)
        if len(pids) == 1:
            print green('*'), '%s: started' % prog
        else:
            print red('*'), '%s: stoped' % prog

    def show(self):
        if progs_enable['mds']:
            self._show_mds(0)

        if progs_enable['cds']:
            started_cds = 0
            half_started_cds = 0
            for i in range(0, self.cds_number):
                pids = self.__get_cdspid(i)
                if len(pids) == 1:
                    started_cds += 1
                else:
                    pass

            if started_cds < self.cds_number and half_started_cds == 0:
                print red('*'), 'cds: %s/%s' % (
                        red(str(started_cds)),
                        str(self.cds_number)
                        )
            elif half_started_cds > 0:
                print yellow('*'), 'cds: %s/%s, half started %s' % (
                        red(str(started_cds)),
                        str(self.cds_number),
                        yellow(str(str(half_started_cds)))
                        )
            else:
                print green('*'), 'cds: %s/%s' % (
                        green(str(started_cds)),
                        str(self.cds_number)
                        )

        if progs_enable['yiscsi']:
            self._show_normsrv('yiscsi')

        if progs_enable['yweb']:
            self._show_normsrv('yweb')

        if progs_enable['yftp']:
            self._show_normsrv('yftp')

        if progs_enable['proxy']:
            self._show_normsrv('proxy')

    def start_yfs(self):
        self.start_mds()
        self.start_cds()

    def stop_yfs(self):
        self.stop_cds()
        self.stop_mds()

    def start_all(self):
        self.start_yfs()
        self.start_yiscsi()
        self.start_yweb()
        self.start_yftp()
        self.start_proxy()

    def stop_all(self):
        self.stop_yiscsi()
        self.stop_yweb()
        self.stop_yftp()
	self.stop_proxy()
        self.stop_yfs()

def usage():
    print '%s --start=service' % sys.argv[0]
    print '   --stop =service'
    print '   --show '
    print '   service [mds, cds, yiscsi, yweb, yftp, yfs, all]'

def main():
    try:
        opts, args = getopt.getopt(
                sys.argv[1:], 
                'h', ['start=', 'stop=', 'help', 'host=', 'show']
                )
    except getopt.GetoptError, err:
        print str(err)
        usage()

    op = None
    service = None
    cdsnum = 0
    host = ''
    show = False
    for o, a in opts:
        if o in ('-h', '--help'):
            usage()
            exit(0)
        elif o in ('--start', '--stop'):
            op = o[2:]
            service = a
        elif o == '--host':
            host = a
        elif o == '--show':
            show = True
        else:
            assert False, 'oops, unhandled option: %s, -h for help' % o
            exit(1)


    yfs = YFS(host)    
    if show == False:
        if service not in ['mds', 'cds', 'yiscsi', 
                           'yweb', 'yftp', 'yfs', 'proxy', 'all']:
            print 'invalid service %s' % service
            exit(1)
        operation = 'yfs.%s_%s()' % (op, service)
        exec(operation)
    else:
        yfs.show()

    print ''



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



if __name__ == '__main__':
    main()

