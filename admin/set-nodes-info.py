#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os

def exec_cmd(cmd):
    ip_list = []
    r = os.popen(cmd).readlines()
    for item in r:
        if item != "\n":
            if item.split(":")[0] == "stat host":
                ip_list.append(item.split(":")[1].strip("\n").strip())
    return ip_list

def write_data():
    for i in result:
        with open("/usr/local/samba/etc/ctdb/nodes",mode="a") as fw:
            fw.write(i)
            fw.write("\n")

if __name__ == "__main__":
    result=exec_cmd("uss.cluster list")
    if os.path.exists("/usr/local/samba/etc/ctdb/nodes"):
        os.system('rm -rf /usr/local/samba/etc/ctdb/nodes')
        write_data()
    else:
        write_data()