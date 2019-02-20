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
    ssh $n 'rm -rf /opt/sdfs/data/mond/*/*'
    ssh $n 'rm -rf /opt/sdfs/data/etcd/*'
    ssh $n 'rm -rf /opt/sdfs/data/redis/*/*'
    ssh $n 'rm -rf /opt/sdfs/data/cds/*/*'
    ssh $n 'rm -rf /opt/sdfs/data/fuse/*'
    ssh $n 'rm -rf /opt/sdfs/data/fuse3/*'
    ssh $n 'rm -rf /opt/sdfs/data/ftp/*'
    ssh $n 'rm -rf /opt/sdfs/data/nfs/*'
    ssh $n 'rm -rf /opt/sdfs/data/fsal_sdfs/*'
    ssh $n 'rm -rf /opt/sdfs/core/*'
    ssh $n 'rm -rf /opt/sdfs/log/*'
    ssh $n 'rm -rf /opt/sdfs/etc/cluster.conf'
    ssh $n 'rm -rf /dev/shm/sdfs/*'
done


