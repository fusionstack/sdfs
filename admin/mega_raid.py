#!/usr/bin/env python2

import platform
import errno
import re
import os
import string
import time

from buddha import lsb
from buddha.smart import SMART

from disk_light import DiskLight
from utils import Exp, dmsg, dwarn, _exec_pipe


class MegaRAID:
    def __init__(self):
        host_type = platform.architecture()[0]
        kernel = platform.uname()[2]
        (distro, release, codename) = lsb.lsb_release()

        self.cmd_str = ''
        if not kernel.startswith('2.6') and distro == 'Ubuntu':
            self.cmd_str += 'setarch ' + platform.uname()[4] + ' --uname-2.6 '
        self.cmd_str += '/opt/MegaRAID/MegaCli/MegaCli'
        if host_type == '64bit':
            self.cmd_str += '64'
        self.cmd = self.cmd_str.split()
        self.res = self.get_show_all()
        self.all_raid = self.get_all_raid()
        self.pdlist = self.get_pd_list()
        self.policy = {'WB' : 'WriteBack',
                       'WT' : 'WriteThrough',
                       'RA' : 'ReadAhead',
                       'NORA' : 'ReadAheadNone',
                       'ADRA' : 'ReadAdaptive',
                       'DIRECT' : 'Direct',
                       'CACHED' : 'Cached',
                       'NOCACHEDBADBBU' : 'No Write Cache if Bad BBU',
                       'CACHEDBADBBU' : 'Write Cache OK if Bad BBU',
                       'ENDSKCACHE' : 'Enable Disk Cache',
                       'DISDSKCACHE' : 'Disable Disk Cache'}

    def raid_refresh(self):
        self.res = self.get_show_all()
        self.all_raid = self.get_all_raid()
        self.pdlist = self.get_pd_list()

    def get_show_all(self):
        res = _exec_pipe(self.cmd + ['-CfgDsply', '-aAll', '-Nolog'], 0, False)
        return res

    def get_pd_list(self):
        res = _exec_pipe(self.cmd + ['-PdList', '-aAll', '-Nolog'], 0, False)
        return res

    def get_all_disk(self):
        '''
        format: {'adp':['inq']}
        {'1': ['S1E0KM3MST2000DM001-9YN164 CC4B',
               'W1E06EL4ST2000DM001-9YN164 CC49'],
         '0': ['W1E292ZZST2000DM001-9YN164 CC4B',
               'BTPR211201BH120LGN INTEL SSDSA2CW120G3 4PC10362',
               'CVPR13600B3S120LGN INTEL SSDSA2CW120G3 4PC10362',
               'W1E1RR7JST2000DM001-1CH164 CC24']}
        '''
        all_disk = {}
        res = self.pdlist
        for line in res.splitlines():
            adp_re = re.search('^Adapter #(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                all_disk[adp] = []
            inq_re = re.search('^Inquiry Data:(.*)', line)
            if inq_re is not None:
                inq = '_'.join(inq_re.group(1).split())
                all_disk[adp].append(inq)

        return all_disk

    def get_raid_disk(self):
        '''
        format: {'adp':['inq']}
        {'1': ['S1E0KM3MST2000DM001-9YN164 CC4B'],
         '0': ['BTPR211201BH120LGN INTEL SSDSA2CW120G3 4PC10362',
               'CVPR13600B3S120LGN INTEL SSDSA2CW120G3 4PC10362']}
        '''
        raid_disk = {}
        for line in self.res.splitlines():
            adp_re = re.search('^Adapter:\s*(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                raid_disk[adp] = []
            inq_re = re.search('^Inquiry Data:(.*)', line)
            if inq_re is not None:
                inq = '_'.join(inq_re.group(1).split())
		raid_disk[adp].append(inq)

        return raid_disk

    def get_new_disk(self):
        '''
        format: {'adp':['inq']}
        {'1': [ 'W1E06EL4ST2000DM001-9YN164 CC49'],
         '0': ['W1E292ZZST2000DM001-9YN164 CC4B',
               'W1E1RR7JST2000DM001-1CH164 CC24']}
        '''
        all_disk = self.get_all_disk()
        raid_disk = self.get_raid_disk()
        new_disk = {}
        for adp, disks in all_disk.iteritems():
            for disk in disks:
                if disk not in raid_disk[adp]:
                    if adp not in new_disk:
                        new_disk[adp] = {}
                    new_disk[adp][disk] = self.get_disk_info(disk)

        return new_disk

    def get_all_raid(self):
        '''
        format: {'adp':{'vd':{'pds':'1', disk:['inq']}}}
        {'1': {'1': {'raid': '0', 'vds': '1', 'disk': ['S1E0KMJVST2000DM001-9YN164 CC4B'], 'pds': '1'},
               '2': {'raid': '0', 'vds': '1', 'disk': ['S1E0KVG9ST2000DM001-9YN164 CC4B'], 'pds': '1'}},
         '0': {'1': {'raid': '0', 'vds': '1', 'disk': ['W1E292ZZST2000DM001-9YN164 CC4B'], 'pds': '1'},
               '0': {'raid': '1', 'vds': '1', 'disk': ['BTPR211201BH120LGN INTEL SSDSA2CW120G3 4PC10362',
                                                      'CVPR13600B3S120LGN INTEL SSDSA2CW120G3 4PC10362'], 'pds': '2'},
               '2': {'raid': '0', 'vds': '1', 'disk': ['W240HMHGST2000DM001-9YN164 CC4C'], 'pds': '1'}}}
        '''
        raid_miss = self.get_raid_missing(False)
        bbu_info = self.get_bbu_info()

        all_raid = {}
        for line in self.res.splitlines():
            adp_re = re.search('^Adapter:\s*(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                all_raid[adp] = {}
            name_re = re.search('^Product Name:\s*(.*)', line)
            if name_re is not None:
                name = name_re.group(1)
                if len(raid_miss[adp]) != 0:
                    name += '(Virtual Drive %s missing)' % raid_miss[adp]
                all_raid[adp]['name'] = name
                all_raid[adp]['type'] = 'LSI'
                all_raid[adp]['memory'] = 'UNKNOW'
                all_raid[adp]['bbu_info'] = bbu_info[adp]
            mem_re = re.search('^Memory:\s*(.*)', line)
            if mem_re is not None:
                mem = mem_re.group(1)
                all_raid[adp]['memory'] = mem
            pds_re = re.search('^Number of PDs:\s*(\d+)', line)
            if pds_re is not None:
                pds = pds_re.group(1)
            vds_re = re.search('^Number of VDs:\s*(\d+)', line)
            if vds_re is not None:
                vds = vds_re.group(1)
            vd_re = re.search('^Virtual Drive:\s*(\d+)', line)
            if vd_re is not None:
                vd = vd_re.group(1)
                all_raid[adp][vd] = {}
                all_raid[adp][vd]['adp'] = adp
                all_raid[adp][vd]['pds'] = pds
                all_raid[adp][vd]['vds'] = vds
                all_raid[adp][vd]['vd'] = vd
                all_raid[adp][vd]['disk'] = []
            raid_re = re.search('^RAID Level\s*:\s*Primary\-(\d+)', line)
            if raid_re is not None:
                raid = raid_re.group(1)
                all_raid[adp][vd]['raid'] = raid
            raid_cache_re = re.search('^Current Cache Policy:\s*(.*)', line)
            if raid_cache_re is not None:
                raid_cache = raid_cache_re.group(1)
                if 'Cached' in raid_cache:
                    all_raid[adp][vd]['raid_cache'] = 'Enabled'
                else:
                    all_raid[adp][vd]['raid_cache'] = 'Disabled'

                all_raid[adp][vd]['cache_policy'] = raid_cache
                if raid_cache != 'WriteBack, ReadAhead, Cached, No Write Cache if Bad BBU':

                    if len(raid_miss[adp]) != 0:
                        all_raid[adp][vd]['cache_stat'] = "Missing"
                    elif bbu_info[adp]['bbu'] == 'Present':
                        if bbu_info[adp].has_key('learn_cycle') and bbu_info[adp]['learn_cycle'] == 'Yes':
                            all_raid[adp][vd]['cache_stat'] = "Learn"
                        else:
                            all_raid[adp][vd]['cache_stat'] = "Unknow"
                    else:
                        all_raid[adp][vd]['cache_stat'] = "Unknow"

            disk_cache_re = re.search('^Disk Cache Policy\s*:\s*(.*)', line)
            if disk_cache_re is not None:
                disk_cache = disk_cache_re.group(1)
                all_raid[adp][vd]['disk_cache'] = disk_cache
            inq_re = re.search('^Inquiry Data:(.*)', line)
            if inq_re is not None:
                inq = '_'.join(inq_re.group(1).split())
		all_raid[adp][vd]['disk'].append(inq)

        return all_raid

    def get_all_pdinfo(self):
        '''
        format:
        {'slot': '3', 'adp': '1', 'encl': '252', 'inq': 'W1E06EL4ST2000DM001-9YN164 CC49', 'type': 'SATA', 'size': '1.819TB'}
        '''
        all_raid = self.all_raid

        all_pdinfo = {}
        res = self.pdlist
        for line in res.splitlines():
            adp_re = re.search('^Adapter #(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                all_pdinfo[adp] = {}
            encl_re = re.search('^Enclosure Device ID:\s*(\d+)', line)
            if encl_re is not None:
                encl = encl_re.group(1)
            slot_re = re.search('^Slot Number:\s*(\d+)', line)
            if slot_re is not None:
                slot = slot_re.group(1)
            devid_re = re.search('^Device Id:\s*(\d+)', line)
            if devid_re is not None:
                devid = devid_re.group(1)
            type_re = re.search('^PD Type:\s*(\S+)', line)
            if type_re is not None:
                type = type_re.group(1)
            size_re = re.search('^Raw Size:\s*(\S+\s+\S+)', line)
            if size_re is not None:
                size = ''.join(size_re.group(1).split())
            stat_re = re.search('^Firmware state:\s*([^,]+)', line)
            if stat_re is not None:
                stat = stat_re.group(1)
            inq_re = re.search('^Inquiry Data:(.*)', line)
            if inq_re is not None:
                inq = '_'.join(inq_re.group(1).split())
                all_pdinfo[adp][inq] = {}
                all_pdinfo[adp][inq]['inq'] = inq
                all_pdinfo[adp][inq]['encl'] = encl
                all_pdinfo[adp][inq]['slot'] = slot
                all_pdinfo[adp][inq]['devid'] = devid
                all_pdinfo[adp][inq]['interface'] = type
                all_pdinfo[adp][inq]['size'] = size
                all_pdinfo[adp][inq]['adp'] = adp
                all_pdinfo[adp][inq]['adp_name'] = all_raid[adp]['name']
                all_pdinfo[adp][inq]['adp_type'] = all_raid[adp]['type']
                all_pdinfo[adp][inq]['stat'] = stat
            foreign_re = re.search('^Foreign State:\s*(\S+)', line)
            if foreign_re is not None:
                foreign = foreign_re.group(1)
                all_pdinfo[adp][inq]['foreign'] = foreign
            media_type_re = re.search('^Media Type:\s*(.*)', line)
            if media_type_re is not None:
                media_type = media_type_re.group(1)
                if media_type == 'Hard Disk Device':
                    media_type = 'HDD'
                elif media_type == 'Solid State Device':
                    media_type = 'SSD'
                all_pdinfo[adp][inq]['media_type'] = media_type
            temp_re = re.search('^Drive Temperature :\s*(\d+)', line)
            if temp_re is not None:
                temp = temp_re.group(1)
                all_pdinfo[adp][inq]['curr_temp'] = temp
                all_pdinfo[adp][inq]['max_temp'] = temp

        return all_pdinfo

    def get_all_ldpdinfo(self):
        '''
        format:
        {vdid: devid}
        '''
        all_raid = self.all_raid

        all_ldpdinfo = {}
        res = _exec_pipe(self.cmd + ['-LdPdInfo', '-aAll', '-Nolog'], 0, False)
        for line in res.splitlines():
            adp_re = re.search('^Adapter #(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                all_ldpdinfo[adp] = {}
            vdid_re = re.search('^Virtual Drive:\s*(\d+)\s* \(Target Id:\s*\d+\)', line)
            if vdid_re is not None:
                vdid = vdid_re.group(1)
            devid_re = re.search('^Device Id:\s*(\d+)', line)
            if devid_re is not None:
                devid = devid_re.group(1)
                all_ldpdinfo[adp][vdid] = devid

        return all_ldpdinfo

    def get_disk_info(self, disk):
        '''
        format:
        {'slot': '3', 'adp': '1', 'encl': '252', 'inq': 'W1E06EL4ST2000DM001-9YN164 CC49', 'type': 'SATA', 'size': '1.819TB'}
        '''
        all_pdinfo = self.get_all_pdinfo()

        disk_info = {}
        for adp in all_pdinfo:
            for pd in all_pdinfo[adp]:
                if pd == disk:
                    disk_info = all_pdinfo[adp][pd]

        return disk_info

    def get_dev_device_model(self, dev):
        model = ''
        dev_info = self.get_dev_info(dev)
        disk_info = self.get_disk_info(dev_info['disk'][0])

        if disk_info['media_type'] != 'SSD':
            return model

        model_re = re.search('(INTEL_SSD\w+)_', disk_info['inq'])
        if model_re is not None:
            model = model_re.group(1).replace('_', ' ')
            return model

        arg = 'sat+megaraid,' + disk_info['devid']

        smart = SMART()
        res = smart.get_dev_info(dev, arg)
        for line in res.splitlines():
            line = line.strip()
            model_re = re.search('^Device Model:\s*(.+)', line)
            if model_re is not None:
                model = model_re.group(1)

        return model

    def get_disk_rotation(self, raid_type, dev):
        dev_info = self.get_dev_info(dev)
        disk_info = self.get_disk_info(dev_info['disk'][0])

        arg = ''
        if disk_info['media_type'] == 'SATA':
            arg = 'sat+'
        arg += 'megaraid,' + disk_info['devid']

        smart = SMART()
        return smart.get_dev_rotation(dev, arg)

    def get_disk_vd(self, disk):
        disk_adp = ''
        disk_vd = ''
        all_raid = self.all_raid

        for adp in all_raid:
            for vd in all_raid[adp]:
                if vd == 'name':
                    continue
                for inq in all_raid[adp][vd]['disk']:
                    if inq == disk:
                        disk_adp = adp
                        disk_vd = vd

        return disk_adp, disk_vd

    def get_dev_vd(self, dev):
        dev_adp = ''
        dev_vd = ''

        res = _exec_pipe(['disk2lid', dev], 0, False)
        for line in res.splitlines():
            dev_re = re.search('^MEIDISEN_INFO:.*:\s*(\d+)\s*:\s*Logical Drive Number :\s*(\d+)', line)
            if dev_re is not None:
                (dev_adp, dev_vd) = dev_re.group(1, 2)
        if dev_adp == '' or dev_vd == '':
            raise Exp(errno.EPERM, dev + " not found in raid array use disk2lid")

        return (dev_adp, dev_vd)

    def get_dev_info(self, dev):
        dev_info = {}
        (dev_adp, dev_vd) = self.get_dev_vd(dev)
        all_raid = self.all_raid

        for adp in all_raid:
            for vd in all_raid[adp]:
                if dev_adp == adp and dev_vd == vd:
                    dev_info = all_raid[adp][vd]
                    dev_info['adp_name'] = all_raid[adp]['name']
                    dev_info['adp_memory'] = all_raid[adp]['memory']
                    dev_info['adp_type'] = all_raid[adp]['type']
                    dev_info['bbu_info'] = all_raid[adp]['bbu_info']
                    dev_info['disk_info'] = {}
                    for disk in  dev_info['disk']:
                        disk_info = self.get_disk_info(disk)
                        dev_info['disk_info'][disk] = disk_info

        if len(dev_info) == 0:
            raise Exp(errno.EINVAL, "%s in Adapter %s not found" % (dev, dev_adp))

        return dev_info

    def get_bbu_info(self):
        '''
        format:
        {'1': {                     #Adapter slot number
               'status': 'None',    #(Charging | Discharging | None)
                 'full': '1342',    #(mAh) Full Charge Capacity
          'temperature': '26',      #(C)
           'learn_next': '2014-03-22 16:40:22', #next learn time
         'learn_period': 30,        #(days) Auto Learn Period
              'replace': 'No',      #(Yes | No) Battery Replacement required
             'relative': '84',      #(%) Relative State of Charge
              'voltage': '3973',    #(mV)
          'learn_cycle': 'No',      #(Yes | No) Learn Cycle Active
             'absolute': '73',      #(%) Absolute state of charge
                 'type': 'Unknown', #Bettery Type
            'remaining': '1118',    #(mAh) Remaining Capacity
                  'low': 'No'       #(Yes | No) Remaining Capacity Low
        }}
        '''
        bbu_info = {}
        try:
            res = _exec_pipe(self.cmd + ['-AdpBbuCmd', '-aAll', '-Nolog'], 0, False)
        except:
            res = ""
        for line in res.splitlines():
            adp_re = re.search('BBU status for Adapter:\s*(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                bbu_info[adp] =  {}
            type_re = re.search('^BatteryType:\s*(\S+)', line)
            if type_re is not None:
                type = type_re.group(1)
                bbu_info[adp]['type'] = type
            v_re = re.search('^Voltage:\s*(\d+)', line)
            if v_re is not None:
                v = v_re.group(1)
                bbu_info[adp]['voltage'] = v
            t_re = re.search('^Temperature:\s*(\d+)', line)
            if t_re is not None:
                t = t_re.group(1)
                bbu_info[adp]['temperature'] = t
            status_re = re.search('Charging Status\s*:\s*(\S+)', line)
            if status_re is not None:
                status = status_re.group(1)
                bbu_info[adp]['status'] = status
            learn_re = re.search('Learn Cycle Active\s*:\s*(\S+)', line)
            if learn_re is not None:
                learn = learn_re.group(1)
                bbu_info[adp]['learn_cycle'] = learn
            replace_re = re.search('Battery Replacement required\s*:\s*(\S+)', line)
            if replace_re is not None:
                replace = replace_re.group(1)
                bbu_info[adp]['replace'] = replace
            low_re = re.search('Remaining Capacity Low\s*:\s*(\S+)', line)
            if low_re is not None:
                low = low_re.group(1)
                bbu_info[adp]['low'] = low
            relative_re = re.search('Relative State of Charge:\s*(\d+)', line)
            if relative_re is not None:
                relative = relative_re.group(1)
                if 'relative' not in bbu_info[adp].keys():
                    bbu_info[adp]['relative'] = relative
            absolute_re = re.search('Absolute state of charge:\s*(\d+)', line)
            if absolute_re is not None:
                absolute = absolute_re.group(1)
                if 'absolute' not in bbu_info[adp].keys():
                    bbu_info[adp]['absolute'] = absolute
            remaining_re = re.search('Remaining Capacity:\s*(\d+)', line)
            if remaining_re is not None:
                remaining = remaining_re.group(1)
                bbu_info[adp]['remaining'] = remaining
            full_re = re.search('Full Charge Capacity:\s*(\d+)', line)
            if full_re is not None:
                full = full_re.group(1)
                bbu_info[adp]['full'] = full
            period_re = re.search('Auto Learn Period:\s*(.*)', line)
            if period_re is not None:
                period = period_re.group(1).strip()
                if period.endswith('Days'):
                    bbu_info[adp]['learn_period'] = string.atoi(period.split()[0])
                else:
                    bbu_info[adp]['learn_period'] = string.atoi(period.split()[0])/(24*60*60)
            next_re = re.search('Next Learn time:\s*(.*)', line)
            if next_re is not None:
                next = next_re.group(1)
                if next.strip().endswith('Sec'):
                     next = next.split(' ')[0]
                bbu_info[adp]['learn_next'] = next
            auto_re = re.search('Auto-Learn Mode:\s*(.*)', line)
            if auto_re is not None:
                auto = auto_re.group(1)
                bbu_info[adp]['auto_learn'] = auto

        for adp in bbu_info:
            '''
            Real Time = '2000-01-01' start + (Next Learn Time + (System Time - RAID Adapter Time))
            '''
            date_time = self.get_adp_time(adp)
            now_time = time.time()
            if bbu_info[adp]['learn_next'].isdigit():
                start_time = time.mktime(time.strptime("2000-01-01", "%Y-%m-%d"))
                real_time = time.localtime(start_time + string.atoi(bbu_info[adp]['learn_next']) + (now_time - date_time))
            else:
                start_time = time.mktime(time.strptime(bbu_info[adp]['learn_next'], "%a %b %d %H:%M:%S %Y"))
                real_time = time.localtime(start_time + (now_time - date_time))
            bbu_info[adp]['learn_next'] = time.strftime("%Y-%m-%d %H:%M:%S", real_time)

        for line in self.res.splitlines():
            adp_re = re.search('^Adapter:\s*(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
            bbu_re = re.search('^BBU:\s*(\S+)', line)
            if bbu_re is not None:
                bbu = bbu_re.group(1)
                if adp not in bbu_info.keys():
                    bbu_info[adp] = {}
                bbu_info[adp]['bbu'] = bbu

        return bbu_info

    def get_adp_time(self, adp):
        d = None
        t = None
        sec = 0

        res = _exec_pipe(self.cmd + ['-AdpGetTime', '-a' + adp, '-Nolog'], 0, False)
        for line in res.splitlines():
            d_re = re.search('Date:\s*(\S+)', line)
            if d_re is not None:
                d = d_re.group(1)
            t_re = re.search('Time:\s*(\S+)', line)
            if t_re is not None:
                t = t_re.group(1)

        if d is None or t is None:
            sec = time.time()
        else:
            sec = time.mktime(time.strptime(d + " " + t, "%m/%d/%Y %H:%M:%S"))

        return sec

    def add_raid0(self, disk, force):
        is_new = False
        new_disk = self.get_new_disk()
        for adp, disks in new_disk.iteritems():
            if disk in disks:
                is_new = True
        if not is_new:
            raise Exp(errno.EINVAL, "'" + disk + "' not a new disk")

        disk_info = self.get_disk_info(disk)

        if force:
            self.del_raid_foreign()
        try:
            res = _exec_pipe(self.cmd + [
                '-CfgLdAdd',
                '-r0',
                '['+disk_info['encl']+':'+disk_info['slot']+']',
                'WB',
                'RA',
                'Cached',
                'NoCachedBadBBU',
                '-a'+disk_info['adp'],
                '-Nolog'
            ], 0, True)
        except Exception, e:
            raise Exp(errno.EPERM, 'can not add %s, try use --force' % disk)

        #(disk_adp, disk_vd) = self.get_disk_vd(disk)
        #self.__set_raid_cache('-DisDskCache', disk_vd, disk_adp)

    def del_raid0(self, dev):
        (dev_adp, dev_vd) = self.get_dev_vd(dev)
        res = _exec_pipe(self.cmd + [
            '-CfgLdDel',
            '-L' + dev_vd,
            '-a' + dev_adp,
            '-Nolog'
        ], 0, True)

    def del_raid0_force(self, dev):
        (dev_adp, dev_vd) = self.get_dev_vd(dev)
        res = _exec_pipe(self.cmd + [
             '-CfgLdDel',
             '-L' + dev_vd,
             '-Force',
             '-a' + dev_adp,
             '-Nolog'
        ], 0, True)

    def get_raid_missing(self, p=True):
        raid_missing = {}
        res = _exec_pipe(self.cmd + [
            '-GetPreservedCacheList',
            '-aAll',
            '-Nolog'
        ], 0, p)

        for line in res.splitlines():
            adp_re = re.search('^Adapter [#]?(\d+)', line)
            if adp_re is not None:
                adp = adp_re.group(1)
                raid_missing[adp] = []
            vd_re = re.search('Virtual Drive\(Target ID (\d+)\):', line)
            if vd_re is not None:
                vd = vd_re.group(1)
                raid_missing[adp].append(str(string.atoi(vd)))

        return raid_missing

    def del_raid_missing(self):
        raid_missing = self.get_raid_missing()
        for adp in raid_missing:
            for vd in raid_missing[adp]:
                res = _exec_pipe(self.cmd + [
                    '-DiscardPreservedCache',
                    '-L' + vd,
                    '-force',
                    '-a' + adp,
                    '-Nolog'
                ], 0, True)

    def del_raid_foreign(self):
        all_pdinfo = self.get_all_pdinfo()
        for adp in all_pdinfo:
            for pd in all_pdinfo[adp]:
                if all_pdinfo[adp][pd]['stat'] == 'Unconfigured(bad)':
                    res = _exec_pipe(self.cmd + [
                        '-PDMakeGood',
                        '-PhysDrv[' + all_pdinfo[adp][pd]['encl'] + ':' + all_pdinfo[adp][pd]['slot'] + ']',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)
                    res = _exec_pipe(self.cmd + [
                        '-CfgForeign',
                        '-clear',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)
                if all_pdinfo[adp][pd]['foreign'] == 'Foreign':
                    res = _exec_pipe(self.cmd + [
                        '-CfgForeign',
                        '-clear',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)

    def import_raid_foreign(self):
        all_pdinfo = self.get_all_pdinfo()
        found = False
        for adp in all_pdinfo:
            for pd in all_pdinfo[adp]:
                if all_pdinfo[adp][pd]['stat'] == 'Unconfigured(bad)':
                    res = _exec_pipe(self.cmd + [
                        '-PDMakeGood',
                        '-PhysDrv[' + all_pdinfo[adp][pd]['encl'] + ':' + all_pdinfo[adp][pd]['slot'] + ']',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)
                    res = _exec_pipe(self.cmd + [
                        '-CfgForeign',
                        '-Import',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)
                    found = True
                if all_pdinfo[adp][pd]['foreign'] == 'Foreign':
                    res = _exec_pipe(self.cmd + [
                        '-CfgForeign',
                        '-Import',
                        '-a' + adp,
                        '-Nolog'
                    ], 0, False)
                    found = True
        if not found:
            dwarn("no Device state 'Foreign'")

    def raid_cache_flush(self):
        res = _exec_pipe(self.cmd + [
            '-GetPreservedCacheList',
            '-aAll',
            '-Nolog'
        ], 0, True)
        res = _exec_pipe(self.cmd + [
            '-AdpCacheFlush',
            '-aAll',
            '-Nolog'
        ], 0, True)

    def del_raid_force(self, dev):
        raid_miss = self.get_raid_missing()
        for adp in raid_miss:
            if len(raid_miss[adp]) != 0:
                raise Exp(errno.EPERM, "Virtual Drive %s in adapter %s missing!" %(raid_miss[adp], adp))

        self.del_raid_foreign()
        self.del_raid0_force(dev)

    def __set_raid_cache(self, prop, l, a):
        res = _exec_pipe(self.cmd + [
            '-LDSetProp',
            prop,
            '-L' + l,
            '-a' + a,
            '-Nolog'
        ], 0, False)

        if "will not come into effect immediately" in res:
            return False

        return True

    def set_raid_cache(self, dev, cache):
        (dev_adp, dev_vd) = self.get_dev_vd(dev)
        dev_info = self.get_dev_info(dev)

        if 'raid_cache' in cache:
            if cache['raid_cache'] == 'enable':
                self.__set_raid_cache('RA', dev_vd, dev_adp)
                self.__set_raid_cache('-Cached', dev_vd, dev_adp)
                self.__set_raid_cache('-NoCachedBadBBU', dev_vd, dev_adp)
                if not self.__set_raid_cache('WB', dev_vd, dev_adp):
                    raid_miss = self.get_raid_missing(False)
                    msg = ''
                    for adp in raid_miss:
                        if len(raid_miss[adp]) != 0:
                            msg += "set %s cache fail, Virtual Drive %s in adapter %s missing!\n" %(dev, raid_miss[adp], adp)
                    if msg == '' and dev_info['bbu_info']['bbu'] == 'Present':
                        msg = "set %s cache fail, Adapter:%s "\
                                "Learn cycle is active currently, So policy Change to WB will not come into effect immediately"\
                                %(dev, dev_adp)
                    elif msg == '':
                        msg = "set %s cache fail, Adapter:%s bbu:%s" % (dev, dev_adp, dev_info['bbu_info']['bbu'])
                    _dwarn(msg)
            elif cache['raid_cache'] == 'disable':
                self.__set_raid_cache('WT', dev_vd, dev_adp)
                self.__set_raid_cache('RA', dev_vd, dev_adp)
                self.__set_raid_cache('-Direct', dev_vd, dev_adp)
                self.__set_raid_cache('-NoCachedBadBBU', dev_vd, dev_adp)
            elif cache['raid_cache'] == 'policy':
                for policy in cache['cache_policy']:
                    self.__set_raid_cache(policy, dev_vd, dev_adp)

        if 'disk_cache' in cache:
            if cache['disk_cache'] == 'enable':
                self.__set_raid_cache('-EnDskCache', dev_vd, dev_adp)
            else:
                self.__set_raid_cache('-DisDskCache', dev_vd, dev_adp)

    def __set_raid_policy_check(self, cachestr):
        if not cachestr.startswith('[') or not cachestr.endswith(']'):
            raise Exp(errno.EINVAL, 'cache policy must with [], eg [wb,ra,direct]')

        cachearr = [x.upper() for x in cachestr[1:-1].split(',')]
        for i in cachearr:
            if i not in self.policy:
                raise Exp(errno.EINVAL, 'unknow cache policy %s for LSI RAID' % i)

        if 'WT' in cachearr and 'WB' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('WB', 'WT'))

        if ('RA' in cachearr and 'NORA' in cachearr) or \
                ('RA' in cachearr and 'ADRA' in cachearr) or \
                ('NORA' in cachearr and 'ADRA' in cachearr):
            raise Exp(errno.EINVAL, '%s %s %s can not be used together' % ('RA', 'NORA', 'ADRA'))

        if 'CACHED' in cachearr and 'DIRECT' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('Cached', 'Direct'))
        if 'NOCACHEDBADBBU' in cachearr and 'CACHEDBADBBU' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('CachedBadBBU', 'NoCachedBadBBU'))
        if 'ENDSKCACHE' in cachearr and 'DISDSKCACHE' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('EnDskCache', 'DisDskCache'))

        return cachearr

    def set_raid_policy(self, dev, cache):
        cachearr = self.__set_raid_policy_check(cache)
        (dev_adp, dev_vd) = self.get_dev_vd(dev)

        print 'set %s cache to %s' %(dev, cachearr)
        for cache in cachearr:
            if cache in ('WT', 'WB', 'RA', 'NORA', 'ADRA'):
                self.__set_raid_cache(cache, dev_vd, dev_adp)
            elif cache == 'CACHED':
                self.__set_raid_cache('-Cached', dev_vd, dev_adp)
            elif cache == 'DIRECT':
                self.__set_raid_cache('-Direct', dev_vd, dev_adp)
            elif cache == 'NOCACHEDBADBBU':
                self.__set_raid_cache('-NoCachedBadBBU', dev_vd, dev_adp)
            elif cache == 'NOCACHEDBADBBU':
                self.__set_raid_cache('-NoCachedBadBBU', dev_vd, dev_adp)
            elif cache == 'CACHEDBADBBU':
                self.__set_raid_cache('-CachedBadBBU', dev_vd, dev_adp)
            elif cache == 'ENDSKCACHE':
                self.__set_raid_cache('-EnDskCache', dev_vd, dev_adp)
            elif cache == 'DISDSKCACHE':
                self.__set_raid_cache('-DisDskCache', dev_vd, dev_adp)

    def get_raid_cache(self, dev):
        raid_cache = {}

        raid_info = self.get_dev_info(dev)
        raid_cache['raid_cache'] = raid_info['cache_policy']
        raid_cache['disk_cache'] = raid_info['disk_cache']

        return raid_cache

    def set_raid_ratio(self, dev):
        pass

    def __policy2str(self, cachearr):
        strarr = []
        if len(cachearr) != 4:
            return ""
        for item in cachearr:
            if item not in self.policy:
                return ""
            strarr.append(self.policy[item])
        return ', '.join(strarr)

    def __policy_check(self, cachestr):
        cachearr = [x.upper() for x in cachestr.split(',')]
        if len(cachearr) != 3 and len(cachearr) != 4:
            return []
        elif cachearr[0] != 'WT' and cachearr[0] != 'WB':
            return []
        elif cachearr[1] != 'RA' and cachearr[1] != 'NORA' and cachearr[1] != 'ADRA':
            return []
        elif cachearr[2] != 'CACHED' and cachearr[2] != 'DIRECT':
            return []
        elif len(cachearr) == 4 and cachearr[3] != 'NOCACHEDBADBBU' and cachearr[3] != 'CACHEDBADBBU':
            return []
        elif len(cachearr) == 3:
            cachearr.append('NOCACHEDBADBBU')

        return cachearr

    def check_raid_cache(self, dev, cacheconf, setcache=True):
        enable_default = 'WriteBack, ReadAhead, Cached, No Write Cache if Bad BBU'
        disable_default = 'WriteThrough, ReadAhead, Direct, No Write Cache if Bad BBU'
        cache = {}

        try:
            raid_info = self.get_dev_info(dev)
            disk_info = self.get_disk_info(raid_info['disk'][0])
        except:
            return cache

        if dev in cacheconf:
            disk = dev
        else:
            disk = disk_info['media_type'].lower()

        if 'skip'in cacheconf[disk] and cacheconf[disk]['skip']:
            return cache

        if 'raid_cache' in cacheconf[disk]:
            raid_cache = cacheconf[disk]['raid_cache']
            if raid_cache == 'policy':
                policy = self.__policy_check(cacheconf[disk]['cache_policy'])
                if (len(policy) == 0):
                    raid_cache = 'ebable'
        else:
            raid_cache = 'enable'

        if 'disk_cache' in cacheconf[disk]:
            disk_cache = cacheconf[disk]['disk_cache']
        else:
            disk_cache = 'disable'

        msg = ''
        if raid_cache == 'enable':
            if raid_info['cache_policy'] != enable_default:
                msg += 'set %s raid cache: %s to: %s' % (dev, raid_info['cache_policy'], enable_default)
                cache['raid_cache'] = raid_cache
        elif raid_cache == 'disable':
            if raid_info['cache_policy'] != disable_default:
                msg += 'set %s raid cache: %s to: %s' % (dev, raid_info['cache_policy'], disable_default)
                cache['raid_cache'] = raid_cache
        elif raid_cache == 'policy':
            if raid_info['cache_policy'] != self.__policy2str(policy):
                msg += 'set %s raid cache: %s to: %s' % (dev, raid_info['cache_policy'], self.__policy2str(policy))
                cache['raid_cache'] = raid_cache
                cache['cache_policy'] = policy

        if not raid_info['disk_cache'].lower().startswith(disk_cache):
            if msg != '':
                msg += '\n'
            msg += 'set %s disk cache to %s' % (dev, disk_cache)
            cache['disk_cache'] = disk_cache

        if msg != '' and setcache:
            _dmsg(msg)

        return cache

    def get_dev_health(self, dev):
        dev_health = ''
        smart = SMART()
        dev_info = self.get_dev_info(dev)
        for disk in dev_info['disk']:
            disk_info = self.get_disk_info(disk)
            arg = ''
            if disk_info['type'] == 'SATA':
                arg = 'sat+'
            arg += 'megaraid,' + disk_info['devid']

            dev_health += smart.get_dev_health(dev, arg)

        return dev_health

    def set_light_flash(self, switch, dev):
        if switch not in ['start', 'stop', 'stat']:
            raise Exp(errno.EINVAL, 'light switch must be start|stop|stat')

        if dev.startswith('/dev/'):
            dev_info = self.get_dev_info(dev)
            res = _exec_pipe(self.cmd + [
                '-AdpSetProp',
                'UseDiskActivityforLocate',
                '-0',
                '-a' + dev_info['adp'],
                '-Nolog'
            ], 0, False)

            start_cmd = ''
            stop_cmd = ''
            disk = dev_info['disk'][0]
            disk_info = self.get_disk_info(disk)
            start_cmd += self.cmd_str + ' -PdLocate -start -physdrv[' + disk_info['encl'] + ':' + disk_info['slot'] + '] -a' + dev_info['adp'] + ' -Nolog 1>/dev/null;'
            stop_cmd += self.cmd_str + ' -PdLocate -stop -physdrv[' + disk_info['encl'] + ':' + disk_info['slot'] + '] -a' + dev_info['adp'] + ' -Nolog 1>/dev/null;'

        else:
            disk_info = self.get_disk_info(dev)
            if len(disk_info) == 0:
                raise Exp(errno.EINVAL, '%s not found' % dev)

            start_cmd = self.cmd_str + ' -PdLocate -start -physdrv[' + disk_info['encl'] + ':' + disk_info['slot'] + '] -a' + disk_info['adp'] + ' -Nolog 1>/dev/null;'
            stop_cmd = self.cmd_str + ' -PdLocate -stop -physdrv[' + disk_info['encl'] + ':' + disk_info['slot'] + '] -a' + disk_info['adp'] + ' -Nolog 1>/dev/null;'

        disk_light = DiskLight(disk_info['inq'], 'MegaRAID', start_cmd, stop_cmd)
        if switch == 'start':
            #os.system(start_cmd)
            try:
                disk_light.start(True)
            except Exception, e:
                _dmsg(dev + " already started")
        elif switch == 'stop':
            try:
                disk_light.stop(True)
            except Exception, e:
                _dmsg(dev + " already stopped")
            #os.system(stop_cmd)
        else:
            if disk_light.stat():
                print 'on'
            else:
                print 'off'

    def get_light_flash(self):
        disk_light = DiskLight()
        return disk_light.list()
