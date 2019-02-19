#!/bin/bash

nodes=`cat /opt/sdfs/etc/cluster.conf |awk '{print $1}'`

/opt/sdfs/app/admin/cluster.py  stop

for n in $nodes
do
    echo "clean " $n
    ssh $n 'systemctl stop etcd'
    ssh $n 'pkill -9 sdfs'
    ssh $n 'pkill -9 redis'
    ssh $n 'pkill -9 python'
    ssh $n 'rm -rf /opt/sdfs.mond/*/*'
    ssh $n 'rm -rf /opt/sdfs/etcd/*'
    ssh $n 'rm -rf /opt/sdfs/cds/*/*'
    ssh $n 'rm -rf /opt/sdfs/fuse/*'
    ssh $n 'rm -rf /opt/sdfs/fuse3/*'
    ssh $n 'rm -rf /opt/sdfs/ftp/*'
    ssh $n 'rm -rf /opt/sdfs/nfs/*'
    ssh $n 'rm -rf /opt/sdfs/fsal_sdfs/*'
    ssh $n 'rm -rf /opt/sdfs/core/*'
    ssh $n 'rm -rf /opt/sdfs/log/*'
    ssh $n 'rm -rf /opt/sdfs/etc/cluster.conf'
    ssh $n 'rm -rf /dev/shm/sdfs/*'
done


