#!/usr/bin/env python2

import os
import time
import errno

from utils import _exec_pipe1, Exp, getlunattr

def create_new_snapshot(home, lun, snapname):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    snapshot = "%s@%s" % (lun, snapname)
    try:
        _exec_pipe1([cmd, '--create', snapshot], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def remove_snapshot(home, snapshot):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        _exec_pipe1([cmd, '--remove', snapshot], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def list_snapshot(home, lun):
    res = []

    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        (out_msg, err_msg) = _exec_pipe1([cmd, '--list', lun], 1, False)
        res = out_msg.split('\n')
        return res[0:len(res)-1]
    except Exp, e:
        raise Exp(e.errno, e.err)

def clone_snapshot(home, snapshot, lun):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        _exec_pipe1([cmd, '--clone', snapshot, lun], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def rollback_snapshot(home, snapshot):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        _exec_pipe1([cmd, '--rollback', snapshot], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def protect_snapshot(home, snapshot):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        _exec_pipe1([cmd, '--protect', snapshot], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def unprotect_snapshot(home, snapshot):
    cmd = os.path.join(home, "libexec/lich.snapshot")
    try:
        _exec_pipe1([cmd, '--unprotect', snapshot], 1, False)
    except Exp, e:
        raise Exp(e.errno, e.err)

def remove_old_snapshot(home, lun, reserve_type, reserve_num):
    snapshot_list= []
    reserve_time_list = []

    snapshot_list = list_snapshot(home, lun)
    for snapshot in snapshot_list:
        if snapshot.startswith(reserve_type):
             reserve_time_list.append(int(snapshot.split('-')[1]))

    print "type : %s, list : %s, len:%d" % (reserve_type, reserve_time_list, len(reserve_time_list))
    now = int(time.time())
    snapshot_count = len(reserve_time_list)
    if int(snapshot_count) <= int(reserve_num)+1:
        print "lun:%s, snapshot count:%d, no need to remove..." % (lun, snapshot_count)
    else:
        reserve_time_list.sort()
        for i in range(0, (int(snapshot_count) - int(reserve_num) - 1)):
            snaptime = reserve_time_list[i]
            #snap name : week_1456970810, day_1456970810, hour_1456970810
            old_snapshot_name = str(lun) + '@' + reserve_type + '-' + str(snaptime)
            print "remove snapshot : %s" % old_snapshot_name
            remove_snapshot(home, old_snapshot_name)


def check_snapshot_interval(home, lun, reserve_type, interval):
    snapshot_list= []
    reserve_time_list = []
    real_interval = 0

    snapshot_list = list_snapshot(home, lun)
    for snapshot in snapshot_list:
        if snapshot.startswith(reserve_type):
            reserve_time_list.append(int(snapshot.split('-')[1]))

    print "type : %s, list : %s, len:%d" % (reserve_type, reserve_time_list, len(reserve_time_list))
    now = int(time.time())
    snapshot_count = len(reserve_time_list)
    if snapshot_count == 0:
        return True
    elif snapshot_count == 1:
        real_interval = now - reserve_time_list[0]
    elif snapshot_count > 1:
        reserve_time_list.sort()
        latest_snapshot_time = reserve_time_list[snapshot_count - 1]
        real_interval = now - latest_snapshot_time
	
    print "real_interval:%s, interval:%s" % (real_interval, interval)
    if (real_interval >= interval):
        return True
    else:
        return False


def _snapshot_manage(home, lun):
    print "---------------------------------------------------"
    ''' get snapshot_switch and snapshot_reserve '''
    snapshot_switch = getlunattr(home, lun, "snapshot_switch").split('\n')[0]
    snapshot_reserve = getlunattr(home, lun, "snapshot_reserve").split('\n')[0]

    if snapshot_switch != "on":
        print "lun:%s, snapshot switch off" % lun
        return

    ''' check interval, need create new snapshot or not '''
    for reserve in snapshot_reserve.split(','):
        print "lun:%s, reserve:%s" % (lun, reserve)
        #"1h-2": interval = 1h / 2

        if (len(reserve.split('-')) != 2):
            raise Exp(errno.EINVAL, 'The value of snapshot_reserve is invalid : %s' % reserve)

        reserve_time = reserve.split('-')[0]
        reserve_num = reserve.split('-')[1]
        interval = float(reserve_time[0:len(reserve_time)-1]) / float(reserve_num)
        if 'h' in reserve:
            reserve_type = "hour"
            sec_interval = interval * 60 * 60
        elif 'd' in reserve:
            reserve_type = "day"
            sec_interval = interval * 60 * 60 * 24
        elif 'w' in reserve:
            reserve_type = "week"
            sec_interval = interval * 60 * 60 * 24 * 7
        elif 'm' in reserve:
            reserve_type = "month"
            sec_interval = interval * 60 * 60 * 24 * 30
        elif 'y' in reserve:
            reserve_type = "year"
            sec_interval = interval * 60 * 60 * 24 * 30 * 12
        else:
            raise Exp(errno.EINVAL, 'unknown reserve type %s' % reserve)

        sec_interval = int(sec_interval)
        now = int(time.time())
        snapname = reserve_type + '-' + str(now)

        ''' check snapshot interval,  now time compare with the latest snapshot time '''
        if (check_snapshot_interval(home, lun, reserve_type, sec_interval)):
            print "lun:%s, create snapshot:%s" % (lun, snapname)
            create_new_snapshot(home, lun, snapname)
        else:
            print "type : %s, lun : %s, no need to create new snapshot" % (reserve_type, lun)

        ''' remove old snapshot '''
        remove_old_snapshot(home, lun, reserve_type, reserve_num)
