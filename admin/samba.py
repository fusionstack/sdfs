#!/usr/bin/env python
# -*- coding:utf-8 -*-
import subprocess
import os
import sys
import argparse
import errno
import socket

from utils import exec_shell, Exp, dwarn

def get_netbios():
    hostname = socket.gethostname()
    netbios = hostname

    if hostname.endswith('.com'):
        netbios = hostname.strip('.com')

    return netbios

def configure_global():
    content = """
[global]
{net}
server string = this is centos
security = {mode}
{realm}
encrypt passwords = yes
workgroup = {workgroup}

winbind enum groups = yes
winbind enum users = yes
winbind separator = /
winbind use default domain = yes
template homedir = /home/%U
template shell = /bin/bash
{imap_range}
{imap_domain}

{passdb_backend}
log level = 3
max log size = 500000
aio read size = 16384
aio write size = 16384
cache directory = /dev/shm/sdfs/locks/cache
lock directory = /dev/shm/sdfs/lock
max connections = 0

"""
    mode = None
    real = ''
    errcode = 0
    real_realm = ''
    netbios = get_netbios()

    _exec_attr = "sdfs.attr -g ad /system"
    try:
        out, __ = exec_shell(_exec_attr, need_return=True)
    except Exp, e:
        errcode = e.errno
        if errcode != 126 and errcode != 2:
            raise Exp(1, "sdfs.attr get ad info failed\n")

    if errcode == 126 or errcode == 2:
        content = content.format(mode='user',
                net="",
                realm="",
                workgroup="FUSIONNAS",
                imap_range="",
                imap_domain="",
                cluster="",
                passdb_backend="passdb backend = tdbsam")
    elif errcode == 0:
        original_realm = out.split(';')[1]
        mode = out.split(';')[-1].strip('\n')
        real_realm = original_realm.upper()

        if original_realm.endswith('.com'):
            workgroup = original_realm.strip('.com').upper()
        else:
            workgroup = "FUSIONNAS"

        if mode == 'users':
            mode = 'user'
            content = content.format(mode='user',
                    net="",
                    realm="",
                    workgroup="FUSIONNAS",
                    imap_range="",
                    imap_domain="",
                    cluster="",
                    passdb_backend="passdb backend = tdbsam")
        elif mode == 'ads':
            mode = 'ads'
            content = content.format(mode='ads',
                    net="netbios name = %s" % (netbios),
                    realm="realm = %s" % (real_realm),
                    workgroup='%s' % (workgroup),
                    imap_range="idmap config *:range = 10000-15000",
                    imap_domain="idmap config %s:backend = ad" % (workgroup),
                    cluster="clustering = yes",
                    passdb_backend="")
        else:
            raise Exp(errno.EINVAL, "unsupport %s" % (mode))

    return mode, content, real_realm

class CIFSShare(object):

    def __init__(self, share_map, domain):
        self.share_name = None
        self.dir_name = None
        self.write_users = []
        self.read_users = []
        self.adwrite_users = []
        self.adread_users = []
        self.every_read = []
        self.every_write = []
        self.domain = domain

        if share_map["mode"] not in ["read-write", "read-only"]:
            return
        self.share_name = share_map["share_name"]
        self.dir_name = share_map["directory"]
        user = None
        is_ad = False
        is_user = False
        is_everyone = False
        if share_map.get("user_name") != None:
            user = share_map["user_name"]
            if len(user)>3 and user[0:2] == "AD":
                is_ad = True
                user = user[3:]
            elif len(user)>6 and user[0:5] == 'LOCAL':
                is_user = True
                user = user[6:]

        elif share_map.get("group_name") != None:
            user = share_map["group_name"]
            if len(user)>3 and user[0:2] == "AD":
                is_ad = True
                user = user[3:]
                user = '@'+'\"%s@%s\"' %(user,self.domain)
            elif len(user)>6 and user[0:6] == 'LOCAL':
                is_user = True
                user = user[6:]
                user = '@'+user
            elif user == 'everyone':
                is_everyone = True
                user = user

        if is_ad:
            self.adread_users.append(user)
        elif is_user:
            self.read_users.append(user)
        elif is_everyone:
            self.every_read.append(user)
        if share_map["mode"] == "read-write":
            if is_ad:
                self.adwrite_users.append(user)
            elif is_user:
                self.write_users.append(user)
            elif is_everyone:
                self.every_write.append(user)
    def merge(self, cifs_share):
        if self.share_name != cifs_share.share_name:
            return

        if self.dir_name != cifs_share.dir_name:
            return
        self.write_users = self.write_users +  cifs_share.write_users
        self.read_users = self.read_users + cifs_share.read_users
        self.adwrite_users = self.adwrite_users + cifs_share.adwrite_users
        self.adread_users = self.adread_users + cifs_share.adread_users
    
def gen_conf(share_obj, is_ad=False):
    every_exist = False
    pri_exist = False
    if is_ad:
        if len(share_obj.adread_users) == 0 and len(share_obj.adwrite_users) == 0 and len(share_obj.every_read) == 0 and len(share_obj.every_write) == 0:
            return None, None
    else:
        if  len(share_obj.read_users) == 0 and len(share_obj.write_users) == 0 and len(share_obj.every_read) == 0 and len(share_obj.every_write) == 0:
            return None,None
    RW_LIST = ''
    privilege = ''
    content = ''
    every_content = ''
    if is_ad:
        read_str = " ".join(share_obj.adread_users)
        write_str = " ".join(share_obj.adwrite_users)

    else:
        read_str = " ".join(share_obj.read_users)
        write_str = " ".join(share_obj.write_users)
    if read_str and write_str:
        pri_exist = True
        RW_LIST = RW_LIST+"valid users = %s \nwrite list = %s\nread list = %s" % (read_str, write_str, read_str)
    else:
        pri_exist = True
        RW_LIST = RW_LIST+"valid users = %s \nread list = %s" % (read_str +" "+write_str, read_str)
    every_read_str = share_obj.every_read
    every_write_str = share_obj.every_write
    if every_read_str and every_write_str:
        every_exist = True
        privilege = "yes"
    elif every_read_str:
        every_exist = True
        privilege = "no"
    if pri_exist:
        content = '''
[%s]
path = %s
''' % (share_obj.share_name, share_obj.dir_name) + RW_LIST + '''
sync always = yes
directory mode = 0755
create mode = 0644
browsable = yes 
available = yes
vfs objects = uss\n '''
    if every_exist:
        every_content = '''
[%s] 
admin users = everyone
create mask = 0644
directory mask = 0755
path = %s
browsable = yes
available = yes \n
'''  % (share_obj.share_name, share_obj.dir_name)
    return content,every_content

class GeneralSambaConf(object):
    def __init__(self, domain):
        self.domain = domain

    def gen_shared_map(self, listofmap):
        '''
        Parse the content of output sdfs.share -p cifs -l 
        chang it to dict foramt key is share name value is the left values
        example:
         protocol   : cifs
         directory  : /cifs-mount-dir-2
         dirid      :  26_v26
         group_name    : AD#test
         share_name : cifs-mount-dir-2
         mode       : read-write
        '''
        shared_map = {}
        for orig_map in listofmap:
            if shared_map.get(orig_map["share_name"], None) is None:
                share_obj = CIFSShare(orig_map, self.domain)
                if share_obj.share_name is not None:
                    shared_map[share_obj.share_name] = share_obj
            else:
                share_obj = CIFSShare(orig_map, self.domain)
                imap_obj = shared_map[orig_map["share_name"]]
                imap_obj.merge(share_obj)
        return shared_map

    def read_file_to_list(self, filename):
        smb_list = []
        tmp = {}
        with open(filename, 'r') as fs:
            for line in fs.readlines():
                if '---' in line:
                    continue
                else:
                    key, value = line.split(':')
                    key = key.strip()
                    value = value.strip()
                    tmp[key] = value
                    if len(tmp) == 8:
                        smb_list.append(tmp)
                        tmp = {}
        if os.path.exists(filename):
            os.remove(filename)
        return smb_list

    def write_to_file(self, command, filename):
        result = os.popen(command)
        with open(filename,'w') as f:
            f.write(result.read())

        share_list = self.read_file_to_list(filename)
        share_obj = self.gen_shared_map(share_list)
        return share_obj

    def write_to_samba_file(self, share_obj, is_ad=None, samba_file=None, content=None):
        if os.path.exists(samba_file):
            os.remove(samba_file)
        with open(samba_file, 'a') as smb:
            smb.write(content)
            for share in share_obj:
                pri_content,every_content = gen_conf(share_obj[share], is_ad=is_ad)
                if pri_content:
                    smb.write(pri_content)
                elif every_content:
                    smb.write(every_content)

def check_cluster_running():
    cmd = "sdfs.mdstat"
    retry = 0
    max_retry = 100

    while True:
        try:
            exec_shell(cmd, p=False)
            break
        except Exception as e:
            if e.errno == errno.ENONET:
                retry = retry + 1
                if retry > max_retry:
                    raise Exp(errno.ENONET, "admin not found in cluster, cmd:(%s)" % (cmd))
                else:
                    pass
            else:
                raise Exp(e.errno, "cluster check fail, cmd:(%s)" % (cmd))


#load smb.conf from leveldb
def load_samba_conf(is_ad=False):
    dwarn("mdstat disabled")
    #check_cluster_running()

    mode, content, domain = configure_global()
    if mode == 'ads':
        is_ad = True
    out_file = "result.txt"
    samba_file = "/usr/local/samba/etc/smb.conf"
    samba_file_tmp = "%s.tmp" % (samba_file)

    if not os.path.exists("/usr/local/samba"):
        sys.exit(1)

    gensmb =  GeneralSambaConf(domain)
    share_obj = gensmb.write_to_file('sdfs.share -p cifs -l', out_file)
    gensmb.write_to_samba_file(share_obj, is_ad=is_ad, samba_file=samba_file_tmp, content=content)
    os.rename(samba_file_tmp, samba_file)

def samba_start():
    smbd_location = "/usr/local/samba/sbin/smbd"
    nmbd_location = "/usr/local/samba/sbin/nmbd"

    if not os.path.isfile(smbd_location) or not os.path.isfile(nmbd_location):
        return;

    cmd = "mkdir -p /dev/shm/sdfs/locks/cache && mkdir -p /dev/shm/sdfs/lock"
    exec_shell(cmd)

    load_samba_conf()

    start_smbd_cmd = "%s -D" % (smbd_location)
    exec_shell(start_smbd_cmd)
    start_nmbd_cmd = "%s -D" % (nmbd_location)
    exec_shell(start_nmbd_cmd)

def samba_stop():
    smbd_location = "/usr/local/samba/sbin/smbd"
    nmbd_location = "/usr/local/samba/sbin/nmbd"

    if os.path.isfile(smbd_location) and os.path.isfile(nmbd_location):
        try:
            exec_shell("pkill -9 smbd")
            exec_shell("pkill -9 nmbd")
        except Exception as e:
            pass

def samba_restart():
    samba_stop()
    samba_start()

def samba_reload_conf():
    cmd = "/usr/local/samba/bin/smbcontrol -s /usr/local/samba/etc/smb.conf smbd reload-config"
    exec_shell(cmd)

if __name__ == '__main__':
    if sys.argv[1] == 'reconf':
        load_samba_conf()
    elif sys.argv[1] == 'reload':
        samba_reload_conf()
    elif sys.argv[1] == 'start':
        samba_start()
    elif sys.argv[1] == 'stop':
        samba_stop()
    elif sys.argv[1] == 'restart':
        samba_stop()
        samba_start()
