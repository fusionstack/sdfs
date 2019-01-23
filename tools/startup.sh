#!/bin/bash

START_MDS1="/sysy/yfs/app/sbin/yfs_mds -n 1 "
START_CDS="/sysy/yfs/app/sbin/yfs_cds"
START_NFS="/sysy/yfs/app/sbin/ynfs_server"
START_OSS="/sysy/yfs/app/sbin/start_oss"
NFS_FLAG=0
OSS_FLAG=0

show_help()
{
    echo "Usage: $0 [OPTIONS]"
    echo " -c NUMBER"
    echo "    Start cds numbers"
    echo " -m NUMBER"
    echo "    Start mds id (0-2)"
    echo " -n"
    echo "    Start without nfs_server on this machine"
    echo " -o"
    echo "    Start without Oss"
    echo " -w path"
    echo "    Start nfs for windows on path"

    exit
}


if [ $# -lt 1 ]
then
    show_help
fi

while getopts :c:m:now: options
do
    case $options in
        c)
            CDS_NUM=$OPTARG
        ;;
        m)
            MDS_ID=$OPTARG
        ;;
        n)
            NFS_FLAG=1
        ;;
        o)
            OSS_FLAG=1
        ;;
        w)
            WIN_FLAG=1
            NFS_PATH=$OPTARG
        ;;
        \?)
            show_help
        ;;
    esac
done

if [ x$MDS_ID != "x" ]
then
   $START_MDS1 -t $MDS_ID 
   sleep 1
fi

if [ x$CDS_NUM != "x" ]
then
    T=1
    while [ $T -le $CDS_NUM ]
    do
        $START_CDS -n $T
        sleep 1
        let T+=1
    done
fi

if ! [ x$NFS_FLAG = "x1" ]
then
    $START_NFS
fi

if ! [ x$OSS_FLAG = "x1" ]
then
    $START_OSS
    sleep 1
    echo "OSS started"
fi

if [ x$WIN_FLAG = "x1" ]
then
    $START_NFS "-e $NFS_PATH"
fi
