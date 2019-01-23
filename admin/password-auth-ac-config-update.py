#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
default_config = """#%PAM-1.0
# This file is auto-generated.
# User changes will be destroyed the next time authconfig is run.
auth        required      pam_env.so
auth        sufficient    pam_unix.so nullok try_first_pass
auth        requisite     pam_succeed_if.so uid >= 500 quiet
auth        sufficient    pam_winbind.so use_first_pass
auth        required      pam_deny.so

account     required      pam_unix.so broken_shadow
account     sufficient    pam_localuser.so
account     sufficient    pam_succeed_if.so uid < 500 quiet
account     [default=bad success=ok user_unknown=ignore] pam_winbind.so
account     required      pam_permit.so

#password    requisite     pam_pwquality.so try_first_pass local_users_only retry=3 authtok_type=
password    sufficient    pam_cracklib.so try_first_pass retry=3 type=
password    sufficient    pam_unix.so sha512 shadow nis nullok try_first_pass use_authtok
password    sufficient    pam_winbind.so use_authtok
password    required      pam_deny.so

session     optional      pam_keyinit.so revoke
session     required      pam_limits.so
-session     optional      pam_systemd.so
session     optional      pam_oddjob_mkhomedir.so umask=0077
session     [success=1 default=ignore] pam_succeed_if.so service in crond quiet use_uid
session     required      pam_unix.so
"""
def update_config():
    if os.path.exists("/etc/pam.d/"):
        with open("/etc/pam.d/password-auth-ac",mode="w+") as fw:
            fw.write(default_config)
    else:
        raise SystemExit("update password-auth-ac failed,please check the path!\n")
if __name__ == "__main__":
    update_config()
