#!/usr/bin/env python2

import errno
import re
import os

from disk_light import DiskLight
from utils import Exp, dmsg, _exec_pipe, _exec_pipe2, lock_file1, unlock_file1

class HPRAID:
    def __init__(self):
        (self.cmd, self.res) = self.get_hpcmd()
        #self.res = self.get_show_all()
        self.all_raid = self.get_all_raid()
        self.policy = {'DIRECT' : 'Disable RAID cache',
                       'CACHED' : 'Enable RAID Cache',
                       'NOCACHEDBADBBU' : 'No Write Cache if Bad BBU',
                       'CACHEDBADBBU' : 'Write Cache OK if Bad BBU',
                       'ENDSKCACHE' : 'Enable Disk Cache',
                       'DISDSKCACHE' : 'Disable Disk Cache',
                       'SMARTPATH' : 'No Write Cache if Bad BBU',
                       'RATIO' : 'Write Cache OK if Bad BBU'}

    def get_hpcmd(self):
        cmd = 'hpssacli'
        res = ''
        fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
        try:
            res = _exec_pipe([cmd, 'ctrl', 'all', 'show', 'config', 'detail'], 0, False)
        except Exception, e:
            cmd = 'hpacucli'
            try:
                res = _exec_pipe([cmd, 'ctrl', 'all', 'show', 'config', 'detail'], 0, False)
                if 'APPLICATION UPGRADE REQUIRED:' in res:
                    raise Exp(errno.EPERM, "please update raid card tools")
            except Exception, e:
                raise Exp(errno.EPERM, "please install raid card tools")
        _unlock_file1(fd)

        return cmd, res


    def raid_refresh(self):
        self.res = self.get_show_all()
        self.all_raid = self.get_all_raid()

    def get_show_all(self):
        fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
        res = _exec_pipe([self.cmd, 'ctrl', 'all', 'show', 'config', 'detail'], 0, False)
        _unlock_file1(fd)

        return res

    def get_all_raid(self):
        '''
        {'0': {'A': {'logical': {'1': {'raid': '0', 'dev': '/dev/sda'}},
                    'physical': {'1I:2:1': {'box': '2', 'serial': '8310A0JSFTM91331', 'type': 'SAS', 'port': '1I', 'bay': '1'}}},
      'unassigned': {'1I:2:4': {'box': '2', 'serial': '73O0A008FTN11330', 'type': 'SAS', 'port': '1I', 'bay': '4'},
                     '1I:2:3': {'box': '2', 'serial': '73I0A02SFTN11329', 'type': 'SAS', 'port': '1I', 'bay': '3'}},
               'B': {'logical': {'2': {'raid': '0', 'dev': '/dev/sdb'}},
                    'physical': {'1I:2:2': {'box': '2', 'serial': '73V0A1CDFTM91331', 'type': 'SAS', 'port': '1I', 'bay': '2'}}}}}
        '''
        all_raid = {}
        raid_miss = self.get_raid_miss()
        is_unassigned = 0
        is_phys = -1
        raid_ratio = None
        cache_size = 'UNKNOW'
        cache_avail = 'UNKNOW'
        cache_detail = None

        for line in self.res.splitlines():
            adp_re = re.search('^(Smart Array .*) in Slot\s+(\d+)', line)
            if adp_re is not None:
                (adp_name, adp) = adp_re.group(1, 2)
                all_raid[adp] = {}
                all_raid[adp]['bbu_info'] = {}
                is_unassigned = 0
                is_phys = -1
                cache = None
                smart_support = 'False'
                if len(raid_miss[adp]) != 0:
                    adp_name += '(%s missing)' % raid_miss[adp].values()
            cache_re = re.search('^\s+Cache Status:\s+(.*)', line)
            if cache_re is not None:
                cache = cache_re.group(1)
            cache_detail_re = re.search('^\s+Cache Status Details:\s+(.*)', line)
            if cache_detail_re is not None:
                cache_detail = cache_detail_re.group(1)
                all_raid[adp]['bbu_info']['low'] = 'Yes' if \
                    'capacitor charge is low' in cache_detail or 'low batteries' in cache_detail else 'No'
            ratio_re = re.search('^\s+Cache Ratio:\s+(.*)', line)
            if ratio_re is not None:
                raid_ratio = ratio_re.group(1)
            disk_cache_re = re.search('^\s+Drive Write Cache:\s+(\S+)', line)
            if disk_cache_re is not None:
                disk_cache = disk_cache_re.group(1)
            nobbu_cache_re = re.search('^\s+No-Battery Write Cache:\s+(\S+)', line)
            nobbu_cache = None
            if nobbu_cache_re is not None:
                nobbu_cache = nobbu_cache_re.group(1)
            bbu_present_re = re.search('^\s+Cache Board Present:\s+(\S+)', line)
            if bbu_present_re is not None:
                bbu_present = bbu_present_re.group(1)
                all_raid[adp]['bbu_info']['bbu'] = 'Present' if bbu_present == 'True' else 'Absent'
            bbu_type_re = re.search('^\s+Cache Backup Power Source:\s+(\S+)', line)
            if bbu_type_re is not None:
                bbu_type = bbu_type_re.group(1)
                all_raid[adp]['bbu_info']['type'] = bbu_type
            bbu_count_re = re.search('^\s+Battery/Capacitor Count:\s+(\d+)', line)
            if bbu_count_re is not None:
                bbu_count = bbu_count_re.group(1)
                all_raid[adp]['bbu_info']['count'] = bbu_count
            bbu_status_re = re.search('^\s+Battery/Capacitor Status:\s+(\S+)', line)
            if bbu_status_re is not None:
                bbu_status = bbu_status_re.group(1)
                all_raid[adp]['bbu_info']['status'] = bbu_status
            bbu_temperature_re = re.search('^\s+Capacitor Temperature  \(C\):\s+(\S+)', line)
            if bbu_temperature_re is not None:
                bbu_temperature = bbu_temperature_re.group(1)
                all_raid[adp]['bbu_info']['temperature'] = bbu_temperature
            cache_size_re = re.search('^\s+Total Cache Size:\s+(.*)', line)
            if cache_size_re is not None:
                cache_size = cache_size_re.group(1).replace(' ', '')
            cache_avail_re = re.search('^\s+Total Cache Memory Available:\s+(.*)', line)
            if cache_avail_re is not None:
                cache_avail = cache_avail_re.group(1).replace(' ', '')
            nobbu_cache_re = re.search('^\s+No-Battery Write Cache:\s+(\S+)', line)
            nobbu_cache = None
            if nobbu_cache_re is not None:
                nobbu_cache = nobbu_cache_re.group(1)
            smart_support_re = re.search('^\s+Driver Supports HPE SSD Smart Path:\s+(\S+)', line)
            if smart_support_re is not None:
                smart_support = smart_support_re.group(1)

            array_re = re.search('^\s+Array:\s*(\S+)', line)
            if array_re is not None:
                array = array_re.group(1)
                all_raid[adp][array] = {}
                all_raid[adp][array]['smart_support'] = smart_support
                is_phys = -1

            smart_re = re.search('Array Type:.*\s+HPE SSD Smart Path:\s*(\S+)', line)
            if smart_re is not None:
                smart = smart_re.group(1)
                all_raid[adp][array]['smart_path'] = smart

            logical_re = re.search('^\s+Logical Drive:\s*(\d+)', line)
            if logical_re is not None:
                logical = logical_re.group(1)
                all_raid[adp][array]['logical'] = {}
                all_raid[adp][array]['logical'][logical] = {}
                all_raid[adp][array]['logical'][logical]['adp'] = adp
                all_raid[adp][array]['logical'][logical]['adp_name'] = adp_name
                all_raid[adp][array]['logical'][logical]['adp_type'] = 'HP'
                is_phys = 0
            raid_re = re.search('^\s+Fault Tolerance: (RAID)*\s*(\d+)', line)
            if raid_re is not None:
                raid = raid_re.groups()[-1]
                all_raid[adp][array]['logical'][logical]['raid'] = raid
            status_re = re.search('^\s+Status:\s*(\S+)', line)
            if status_re is not None and is_phys == 0:
                status = status_re.group(1)
                all_raid[adp][array]['logical'][logical]['stat'] = status
            raid_cache_re = re.search('^\s+Caching:\s*(\S+)', line)
            if raid_cache_re is not None:
                raid_cache = raid_cache_re.group(1)
                all_raid[adp][array]['logical'][logical]['raid_cache'] = raid_cache
                all_raid[adp][array]['logical'][logical]['raid_ratio'] = raid_ratio
                all_raid[adp][array]['logical'][logical]['adp_memory'] = cache_avail + ' / ' + cache_size
                cache_warn = ''
                cache_right = ''
                if not cache:
                    cache_warn += 'No cache found '
                    cache_right += 'Cache Enabled '
                elif 'Disabled' in cache:
                    cache_warn += 'Cache Disabled '
                    cache_right += 'Cache Enabled '
                if nobbu_cache == 'Enabled':
                    cache_warn += 'Write Cache OK if Bad BBU'
                    cache_right += 'No Write Cache if Bad BBU'
                if cache_warn != '':
                    all_raid[adp][array]['logical'][logical]['cache_warn'] = cache_warn.strip() + ' / ' + cache_right.strip()
                all_raid[adp][array]['logical'][logical]['disk_cache'] = disk_cache
                all_raid[adp][array]['logical'][logical]['cache_status'] = cache
                all_raid[adp][array]['logical'][logical]['cache_detail'] = cache_detail
            dev_re = re.search('^\s+Disk Name:\s*(\S+)', line)
            if dev_re is not None:
                dev = dev_re.group(1)
                all_raid[adp][array]['logical'][logical]['dev'] = dev

            physical_re = re.search('^\s+physicaldrive\s*(\S+)$', line)
            if physical_re is not None and is_unassigned == 0:
                physical = physical_re.group(1)
                all_raid[adp][array]['physical'] = {}
                all_raid[adp][array]['physical'][physical] = {}
                all_raid[adp][array]['physical'][physical]['slot'] = physical
                all_raid[adp][array]['physical'][physical]['adp'] = adp
                all_raid[adp][array]['physical'][physical]['adp_name'] = adp_name
                all_raid[adp][array]['physical'][physical]['adp_type'] = 'HP'
                is_phys = 1
            status_re = re.search('^\s+Status:\s*(\S+)', line)
            if status_re is not None and is_unassigned == 0 and is_phys == 1:
                status = status_re.group(1)
                all_raid[adp][array]['physical'][physical]['stat'] = status
            interface_re = re.search('^\s+Interface Type:\s*(.*)', line)
            if interface_re is not None and is_unassigned == 0 and is_phys == 1:
                interface = interface_re.group(1)
                all_raid[adp][array]['physical'][physical]['interface'] = interface.split()[-1]
                if 'Solid State' in interface:
                    all_raid[adp][array]['physical'][physical]['media_type'] = 'SSD'
                else:
                    all_raid[adp][array]['physical'][physical]['media_type'] = 'HDD'
            size_re = re.search('^\s+Size:\s*(\S+)', line)
            if size_re is not None and is_unassigned == 0 and is_phys == 1:
                size = size_re.group(1)
                all_raid[adp][array]['physical'][physical]['size'] = size.replace(' ', '')
            rotation_re = re.search('^\s+Rotational Speed:\s*(\d+)', line)
            if rotation_re is not None and is_unassigned == 0 and is_phys == 1:
                rotation = rotation_re.group(1)
                all_raid[adp][array]['physical'][physical]['rotation'] = int(rotation)
            serial_re = re.search('^\s+Serial Number:\s*(\S+)', line)
            if serial_re is not None and is_unassigned == 0 and is_phys == 1:
                serial = serial_re.group(1)
                all_raid[adp][array]['physical'][physical]['inq'] = serial

                all_raid[adp][array]['physical'][physical]['foreign'] = 'None'
            model_re = re.search('^\s+Model:\s*(.*)', line)
            if model_re is not None and is_unassigned == 0 and is_phys == 1:
                model = model_re.group(1)
                all_raid[adp][array]['physical'][physical]['model'] = model
            curr_temp_re = re.search('^\s+Current Temperature.*:\s*(\d+)', line)
            if curr_temp_re is not None and is_unassigned == 0 and is_phys == 1:
                curr_temp = curr_temp_re.group(1)
                all_raid[adp][array]['physical'][physical]['curr_temp'] = curr_temp
            max_temp_re = re.search('^\s+Maximum Temperature.*:\s*(\d+)', line)
            if max_temp_re is not None and is_unassigned == 0 and is_phys == 1:
                max_temp = max_temp_re.group(1)
                all_raid[adp][array]['physical'][physical]['max_temp'] = max_temp

            unassigned_re = re.search('^\s+unassigned', line)
            if unassigned_re is not None:
                all_raid[adp]['unassigned'] = {}
                is_unassigned = 1

            physical_re = re.search('^\s+physicaldrive\s*(\S+)$', line)
            if physical_re is not None and is_unassigned == 1:
                physical = physical_re.group(1)
                all_raid[adp]['unassigned'][physical] = {}
                all_raid[adp]['unassigned'][physical]['slot'] = physical
                all_raid[adp]['unassigned'][physical]['adp'] = adp
                all_raid[adp]['unassigned'][physical]['adp_name'] = adp_name
                all_raid[adp]['unassigned'][physical]['adp_type'] = 'HP'
                is_phys = 1
            interface_re = re.search('^\s+Interface Type:\s*(.*)', line)
            if interface_re is not None and is_unassigned == 1 and is_phys == 1:
                interface = interface_re.group(1)
                all_raid[adp]['unassigned'][physical]['interface'] = interface.split()[-1]
                if 'Solid State' in interface:
                    all_raid[adp]['unassigned'][physical]['media_type'] = 'SSD'
                else:
                    all_raid[adp]['unassigned'][physical]['media_type'] = 'HDD'
            size_re = re.search('^\s+Size:\s*(.*)', line)
            if size_re is not None and is_unassigned == 1 and is_phys == 1:
                size = size_re.group(1)
                all_raid[adp]['unassigned'][physical]['size'] = size.replace(' ', '')
            rotation_re = re.search('^\s+Rotational Speed:\s*(\d+)', line)
            if rotation_re is not None and is_unassigned == 1 and is_phys == 1:
                rotation = rotation_re.group(1)
                all_raid[adp]['unassigned'][physical]['rotation'] = int(rotation)
            serial_re = re.search('^\s+Serial Number:\s*(\S+)', line)
            if serial_re is not None and is_unassigned == 1 and is_phys == 1:
                serial = serial_re.group(1)
                all_raid[adp]['unassigned'][physical]['inq'] = serial

                all_raid[adp]['unassigned'][physical]['foreign'] = 'None'
            model_re = re.search('^\s+Model:\s*(.*)', line)
            if model_re is not None and is_unassigned == 1 and is_phys == 1:
                model = model_re.group(1)
                all_raid[adp]['unassigned'][physical]['model'] = model

        return all_raid

    def get_raid_miss(self):
        raid_miss = {}
        is_phys = -1

        for line in self.res.splitlines():
            adp_re = re.search('^(Smart Array .*) in Slot\s+(\d+)', line)
            if adp_re is not None:
                (adp_name, adp) = adp_re.group(1, 2)
                raid_miss[adp] = {}
                is_phys = -1

            array_re = re.search('^\s+Array:\s*(\S+)', line)
            if array_re is not None:
                array = array_re.group(1)
                is_phys = -1

            logical_re = re.search('^\s+Logical Drive:\s*(\d+)', line)
            if logical_re is not None:
                is_phys = 0
            status_re = re.search('^\s+Status:\s*(\S+)', line)
            if status_re is not None and is_phys == 0:
                status = status_re.group(1)
                if status == 'Failed':
                    raid_miss[adp][array] = None

            physical_re = re.search('^\s+physicaldrive\s*(\S+)', line)
            if physical_re is not None:
                is_phys = 1
            dev_re = re.search('^\s+Disk Name:\s*(\S+)', line)
            if dev_re is not None:
                dev = dev_re.group(1)
                if adp in raid_miss:
                    if array in raid_miss[adp]:
                        raid_miss[adp][array] = dev

        return raid_miss

    def get_new_disk(self):
        '''
        {'0': {'1I:2:4': {'box': '2', 'serial': '73O0A008FTN11330', 'type': 'SAS', 'port': '1I', 'bay': '4'},
               '1I:2:3': {'box': '2', 'serial': '73I0A02SFTN11329', 'type': 'SAS', 'port': '1I', 'bay': '3'}}}
        '''
        new_disk = {}
        all_raid = self.all_raid
        for adp in all_raid:
            for array in all_raid[adp]:
                if array == 'bbu_info':
                    continue
                if array == 'unassigned':
                    new_disk[adp] = {}
                    for physical in all_raid[adp][array]:
                        new_disk[adp][physical] = all_raid[adp][array][physical]
                        new_disk[adp][physical]['bbu_info'] = all_raid[adp]['bbu_info']

        return new_disk

    def get_dev_info(self, dev):
        '''
        {'pds': '1', 'raid': '0', 'dev': '/dev/sda'}
        '''
        dev_info = {}
        all_raid = self.all_raid
        for adp in all_raid:
            for array in all_raid[adp]:
                if array == 'bbu_info':
                    continue
                for type in all_raid[adp][array]:
                    if type == 'logical':
                        for drive, drive_info in all_raid[adp][array]['logical'].iteritems():
                            if dev in drive_info.values():
                                for key in drive_info:
                                    dev_info[key] = drive_info[key]
                                dev_info['pds'] = str(len(all_raid[adp][array]['physical']))
                                dev_info['array'] = array
                                dev_info['smart_support']= all_raid[adp][array]['smart_support']
                                if 'smart_path' in all_raid[adp][array]:
                                    dev_info['smart_path'] = all_raid[adp][array]['smart_path']
                                dev_info['ld'] = drive
                                dev_info['bbu_info'] = all_raid[adp]['bbu_info']
                                dev_info['disk'] = []
                                disk_list = all_raid[adp][array]['physical']
                                for disk in disk_list:
                                    disk_info = disk_list.get(disk)
                                    #dev_info['disk'][disk_info['inq']] = disk_info
                                    dev_info['disk'].append(disk_info['inq'])

        if len(dev_info) == 0:
            raise Exp(errno.EINVAL, "%s in Adapter not found" % (dev))

        return dev_info

    def get_disk_info(self, dev):
        '''
        {'pds': '1', 'raid': '0', 'dev': '/dev/sda'}
        '''
        disk_info = {}
        all_raid = self.all_raid
        for adp in all_raid:
            for array in all_raid[adp]:
                if array == 'bbu_info':
                    continue
                if array == 'unassigned':
                    for drive, drive_info in all_raid[adp][array].iteritems():
                        if dev in drive_info.values():
                            for key in drive_info:
                                disk_info[key] = drive_info[key]
                for type in all_raid[adp][array]:
                    if type == 'physical':
                        for drive, drive_info in all_raid[adp][array]['physical'].iteritems():
                            if dev in drive_info.values():
                                for key in drive_info:
                                    disk_info[key] = drive_info[key]

        return disk_info

    def get_dev_device_model(self, dev):
        model = ''
        dev_info = self.get_dev_info(dev)
        disk_info = self.get_disk_info(dev_info['disk'][0])
        if disk_info['media_type'].lower() == 'ssd':
            model = disk_info['model']
        return model

    def get_disk_rotation(self, raid_type, dev):
        dev_info = self.get_dev_info(dev)
        disk_info = self.get_disk_info(dev_info['disk'][0])
        if 'rotation' in disk_info:
            return disk_info['rotation']
        return None

    def add_raid0(self, disk, force):
        is_new = False
        disk_info = None
        new_disk = self.get_new_disk()
        for adp in new_disk:
            for phy in new_disk[adp]:
                if disk == new_disk[adp][phy]['inq']:
                    is_new = True
                    disk_info = new_disk[adp][phy]
                    disk_info['adp'] = adp
                    disk_info['phy'] = phy
        if not is_new:
            raise Exp(errno.EINVAL, "'" + disk + "' not a new disk")

        fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
        res = _exec_pipe2([
            self.cmd,
            'ctrl',
            'slot=' + disk_info['adp'],
            'create',
            'type=ld',
            'drives=' + disk_info['phy'],
            'raid=0'
        ], 0, True, stdin='y')
        _unlock_file1(fd)

    def del_raid0(self, dev):
        dev_info = self.get_dev_info(dev)
        fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
        res = _exec_pipe([
            self.cmd,
            'ctrl',
            'slot=' + dev_info['adp'],
            'logicaldrive',
            dev_info['ld'],
            'delete',
            'forced'
        ], 0, False)
        _unlock_file1(fd)

    def del_raid_missing(self):
        all_raid = self.all_raid
        for adp in all_raid:
            for array in all_raid[adp]:
                if array == 'bbu_info':
                    continue
                if array != 'unassigned':
                    for logical in all_raid[adp][array]['logical']:
                        if all_raid[adp][array]['logical'][logical]['stat'] == 'Failed':
                            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                            res = _exec_pipe([
                                self.cmd,
                                'ctrl',
                                'slot=' + adp,
                                'logicaldrive',
                                logical,
                                'delete',
                                'forced'
                            ], 0, False)
                            _unlock_file1(fd)

    def del_raid_force(self, dev):
        all_raid = self.all_raid
        for adp in all_raid:
            for array in all_raid[adp]:
                if array == 'bbu_info':
                    continue
                if array != 'unassigned':
                    for logical in all_raid[adp][array]['logical']:
                        if all_raid[adp][array]['logical'][logical]['stat'] == 'Failed':
                            if dev is None or dev is not None and all_raid[adp][array]['logical'][logical]['dev'] == dev:
                                while True:
                                    fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
                                    res = _exec_pipe([
                                        self.cmd,
                                        'ctrl',
                                        'slot=' + adp,
                                        'logicaldrive',
                                        logical,
                                        'delete',
                                        'forced'
                                    ], 0, False)
                                    _unlock_file1(fd)
                                    break

    def __set_raid_cache(self, dev_info, smartpath, raid_cache, disk_cache, ratio, badbbu_cache):
        if smartpath:
            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
            res = _exec_pipe([
                self.cmd,
                'ctrl',
                'slot=' + dev_info['adp'],
                'array',
                dev_info['array'],
                'modify',
                'ssdsmartpath=' + smartpath
            ], 0, False)
            _unlock_file1(fd)

        if raid_cache:
            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
            try:
                res = _exec_pipe([
                    self.cmd,
                    'ctrl',
                    'slot=' + dev_info['adp'],
                    'logicaldrive',
                    dev_info['ld'],
                    'modify',
                    'caching=' + raid_cache,
                    'forced'
                ], 0, False)
            except Exp, e:
                raise
                '''
                if e.errno == errno.EPERM and self.cmd == 'hpssacli':
                    res = _exec_pipe([
                        'hpacucli',
                        'ctrl',
                        'slot=' + dev_info['adp'],
                        'logicaldrive',
                        dev_info['ld'],
                        'modify',
                        'caching=' + raid_cache,
                        'forced'
                    ], 0, False)
                else:
                    raise
                '''
            _unlock_file1(fd)

        if ratio:
            self.set_raid_ratio(dev_info['dev'], ratio)

        if disk_cache:
            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
            res = _exec_pipe([
                self.cmd,
                'ctrl',
                'slot=' + dev_info['adp'],
                'modify',
                'drivewritecache=' + disk_cache,
                'forced'
            ], 0, False)
            _unlock_file1(fd)

        if badbbu_cache:
            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
            try:
                res = _exec_pipe([
                    self.cmd,
                    'ctrl',
                    'slot=' + dev_info['adp'],
                    'modify',
                    'nobatterywritecache=' + badbbu_cache,
                    'forced'
                ], 0, False)
            except Exp, e:
                if e.errno == errno.EPERM and self.cmd == 'hpssacli':
                    res = _exec_pipe([
                        'hpacucli',
                        'ctrl',
                        'slot=' + dev_info['adp'],
                        'modify',
                        'nobatterywritecache=' + badbbu_cache,
                        'forced'
                    ], 0, False)
                else:
                    raise
            _unlock_file1(fd)

    def set_raid_cache(self, dev, cache):
        dev_info = self.get_dev_info(dev)
        smartpath = None
        raid_cache = None
        disk_cache = None
        ratio = None
        badbbu_cache = 'disable'

        if 'raid_cache' in cache:
            if cache['raid_cache'] == 'policy':
                if 'smartpath' in cache['cache_policy']:
                    if dev_info['smart_support'] == 'True':
                        if 'smart_path' not in dev_info:
                            smartpath = 'enable'
                        elif dev_info['smart_path'] == 'disable':
                            smartpath = 'enable'
                else:
                    if dev_info['smart_support'] == 'True':
                        if 'smart_path' in dev_info and dev_info['smart_path'] == 'enable':
                            smartpath = 'disable'

                    raid_cache = 'enable'
                    ratio = cache['cache_policy']
            else:
                if dev_info['smart_support'] == 'True':
                    if 'smart_path' in dev_info and dev_info['smart_path'] == 'enable':
                        smartpath = 'disable'
                    raid_cache = cache['raid_cache']
        if 'disk_cache' in cache:
            disk_cache = cache['disk_cache']

        self.__set_raid_cache(dev_info, smartpath, raid_cache, disk_cache, ratio, badbbu_cache)

    def __set_raid_policy_check(self, cachestr):
        ratio = None
        if not cachestr.startswith('[') or not cachestr.endswith(']'):
            raise Exp(errno.EINVAL, 'cache policy must with [], eg [wb,ra,direct]')

        cachearr = [x.upper() for x in cachestr[1:-1].split(',')]
        for i in cachearr:
            if '/' in i:
                ratio = i
                continue
            if i not in self.policy:
                raise Exp(errno.EINVAL, 'unknow cache policy %s for HP RAID' % i)

        if 'SMARTPATH' in cachearr:
            if len(cachearr) != 1:
                raise Exp(errno.EINVAL, 'Smartpath only be used alone')

        if 'CACHED' in cachearr and 'DIRECT' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('Cached', 'Direct'))
        if 'ENDSKCACHE' in cachearr and 'DISDSKCACHE' in cachearr:
            raise Exp(errno.EINVAL, '%s and %s can not be used together' % ('EnDskCache', 'DisDskCache'))
        if 'DIRECT' in cachearr and ratio:
            raise Exp(errno.EINVAL, 'ratio %s and %s can not be used together' % (ratio, 'Direct'))

        return cachearr

    def set_raid_policy(self, dev, cache):
        cachearr = self.__set_raid_policy_check(cache)
        dev_info = self.get_dev_info(dev)
        smartpath = None
        raid_cache = None
        disk_cache = None
        ratio = None
        badbbu_cache = None

        for i in cachearr:
            if i == 'SMARTPATH':
                if dev_info['smart_support'] == 'True':
                    if 'smart_path' not in dev_info:
                        smartpath = 'enable'
                    elif dev_info['smart_path'] == 'disable':
                        smartpath = 'enable'
                else:
                    raise Exp(errno.EINVAL, 'cannot suport smartpath')
            elif i == 'CACHED' or i == 'DIRECT' or '/' in i:
                if dev_info['smart_support'] == 'True':
                    if 'smart_path' in dev_info and dev_info['smart_path'] == 'enable':
                        smartpath = 'disable'

                if i == 'CACHED':
                    raid_cache = 'enable'
                elif i == 'DIRECT':
                    raid_cache = 'disable'
                else:
                    raid_cache = 'enable'
                    ratio = i.split('/')
            elif i == 'ENDSKCACHE' or i == 'DISDSKCACHE':
                disk_cache = 'enable' if i == 'ENDSKCACHE' else 'disable'
            elif i == 'NOCACHEDBADBBU' or i == 'CACHEDBADBBU':
                badbbu_cache = 'enable' if i == 'CACHEDBADBBU' else 'disable'

        self.__set_raid_cache(dev_info, smartpath, raid_cache, disk_cache, ratio, badbbu_cache)

    def get_raid_cache(self, dev):
        raid_cache = {}

        raid_info = self.get_dev_info(dev)
        raid_cache['raid_cache'] = raid_info['raid_cache']
        if raid_info['smart_support'] == 'True':
            if 'smart_path' not in raid_info:
                raid_cache['smart_path'] = 'disable'
            else:
                raid_cache['smart_path'] = raid_info['smart_path']
        raid_cache['raid_ratio'] = raid_info['raid_ratio']
        raid_cache['disk_cache'] = raid_info['disk_cache']

        return raid_cache

    def set_raid_ratio(self, dev, ratio=None):
        raid_info = self.get_dev_info(dev)

        if ratio is None:
            ratio = ['10', '90']
        elif len(ratio) != 2:
            ratio = ['10', '90']
        elif int(ratio[0]) + int(ratio[1]) != 100:
            ratio = ['10', '90']

        #hpacucli ctrl slot=0 modify cacheratio=10/90
        fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
        res = _exec_pipe([
            self.cmd,
            'ctrl',
            'slot=' + raid_info['adp'],
            'modify',
            'cacheratio=' + ratio[0] + '/' + ratio[1],
            'forced'
        ], 0, False)
        _unlock_file1(fd)

    def __policy2str(self, cachearr):
        if 'smartpath' in cachearr:
            return "smartpath"
        else:
            ratioarr = cachearr[0].split('/')
            return ratioarr[0] + '% Read / '+ ratioarr[1] + '% Write'

    def __policy_check(self, cachestr):
        cachearr = []
        if '/' in cachestr:
            ratioarr = cachestr.split('/')
            if not ratioarr[0].isdigit():
                cachearr.append('10/90')
            elif not ratioarr[1].isdigit():
                cachearr.append('10/90')
            elif int(ratioarr[0]) + int(ratioarr[1]) != 100:
                cachearr.append('10/90')
            else:
                cachearr.append(cachestr)
        elif cachestr == 'smartpath':
                cachearr.append(cachestr)
        return cachearr

    def check_raid_cache(self, dev, cacheconf, setcache=True):
        raid_info = self.get_dev_info(dev)
        disk_info = self.get_disk_info(raid_info['disk'][0])
        cache = {}

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
        if raid_cache == 'policy':
            if 'smartpath' in policy:
                if raid_info['smart_support'] != 'True' or  disk_info['media_type'] != 'SSD':
                    msg += 'cant not set %s raid cache to: %s' % (dev, self.__policy2str(policy))
                elif 'smart_path' not in raid_info:
                    msg += 'set %s raid cache to: %s' % (dev, self.__policy2str(policy))
                    cache['raid_cache'] = raid_cache
                    cache['cache_policy'] = policy
                elif raid_info['smart_path'] != 'enable':
                    msg += 'set %s raid cache to: %s' % (dev, self.__policy2str(policy))
                    cache['raid_cache'] = raid_cache
                    cache['cache_policy'] = policy
            elif not raid_info['raid_cache'].lower().startswith('enable') or \
                    raid_info['raid_ratio'] != self.__policy2str(policy):
                msg += 'set %s raid cache to: %s' % (dev, self.__policy2str(policy))
                cache['raid_cache'] = raid_cache
                cache['cache_policy'] = policy
        elif 'smart_path' in raid_info and raid_info['smart_path'] == 'enable':
            msg += 'set %s raid cache to %s' % (dev, raid_cache)
            cache['raid_cache'] = raid_cache
        elif not raid_info['raid_cache'].lower().startswith(raid_cache):
            msg += 'set %s raid cache to %s' % (dev, raid_cache)
            cache['raid_cache'] = raid_cache
        if not raid_info['disk_cache'].lower().startswith(disk_cache):
            if msg != '':
                msg += '\n'
            msg += 'set %s disk cache to %s' % (dev, disk_cache)
            cache['disk_cache'] = disk_cache
        if msg != '' and setcache:
            _dmsg(msg)

        return cache

    def set_light_flash(self, switch, dev):
        if switch not in ['start', 'stop', 'stat']:
            raise Exp(errno.EINVAL, 'light switch must be start|stop|stat')

        if dev.startswith('/dev/'):
            dev_info = self.get_dev_info(dev)
            start_cmd = self.cmd + ' ctrl slot=' + dev_info['adp'] + ' logicaldrive ' + dev_info['ld'] + ' modify led=on'
            stop_cmd = self.cmd + ' ctrl slot=' + dev_info['adp'] + ' logicaldrive ' + dev_info['ld'] + ' modify led=off'
            disk_inq = dev_info['disk'][0]
        else:
            disk_info = self.get_disk_info(dev)
            if len(disk_info) == 0:
                raise Exp(errno.EINVAL, dev + ' not found')
            start_cmd = self.cmd + ' ctrl slot=' + disk_info['adp'] + ' physicaldrive ' + disk_info['slot'] + ' modify led=on'
            stop_cmd = self.cmd + ' ctrl slot=' + disk_info['adp'] + ' physicaldrive ' + disk_info['slot'] + ' modify led=off'
            disk_inq = disk_info['inq']

        disk_light = DiskLight(disk_inq, 'HPRAID', start_cmd, stop_cmd)
        if switch == 'start':
            try:
                disk_light.start(True)
            except Exception, e:
                _dmsg(dev + " already started")
        elif switch == 'stop':
            try:
                disk_light.stop(True)
            except Exception, e:
                _dmsg(dev + " already stopped")
            fd = _lock_file1("/var/run/fusionstack_raid_hpacucli.lock")
            os.system(stop_cmd)
            _unlock_file1(fd)
        else:
            if disk_light.stat():
                print 'on'
            else:
                print 'off'

    def get_light_flash(self):
        disk_light = DiskLight()
        return disk_light.list()
