#!/bin/bash

SSHKEY_FILE="/root/.ssh/id_dsa"

if [ "`whoami`" = "root" ];then
    ssh-keygen -t dsa -P '' -f $SSHKEY_FILE
    echo "public ssh key generate in $SSHKEY_FILE.pub, now paste it in the tail of '/root/.ssh/authorized_keys"
else
    echo "Error: Please run this programm as root"
fi
