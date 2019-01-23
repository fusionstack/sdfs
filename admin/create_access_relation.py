#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
def create_access_relation():
    if os.path.exists("/lib64/libnss_winbind.so"):
		os.remove("/lib64/libnss_winbind.so")
		os.system("ln -s /usr/local/samba/lib/libnss_winbind.so  /lib64")
		print "recreate libnss_winbind.so ok"
    else:
		os.system("ln -s /usr/local/samba/lib/libnss_winbind.so  /lib64")
		print "create libnss_winbind.so ok"
    if os.path.exists("/lib64/libnss_winbind.so.2"):
		os.remove("/lib64/libnss_winbind.so.2")
		os.system("ln -s /lib64/libnss_winbind.so   /lib64/libnss_winbind.so.2")
		print "recreate libnss_winbind.so.2 ok"
    else:
		os.system("ln -s /lib64/libnss_winbind.so   /lib64/libnss_winbind.so.2")
		print "create libnss_winbind.so.2"
    if os.path.exists("/lib64/security/pam_winbind.so"):
		os.remove("/lib64/security/pam_winbind.so")
		os.system("ln -s /usr/local/samba/lib/security/pam_winbind.so /lib64/security")
		print "recreate security ok"
    else:
		os.system("ln -s /usr/local/samba/lib/security/pam_winbind.so /lib64/security")
		print "create security ok"
if __name__ == "__main__":
    create_access_relation()