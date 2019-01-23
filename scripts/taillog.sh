#!/bin/bash

LOG_DIR="/var/log/uss"
LINE=100

if [ $# -lt 1 ]
then
    echo "Usage: "
    echo "      $0 -t         tail log"
    echo "      $0 -s         scp  log"
fi

while getopts :tsl options
do
    case $options in
	t)
	    for i in `awk '{print $1}' /sysy/yfs/etc/cluster.conf `
	    do
	        echo $i
	        ssh $i "/sysy/yfs/app/bin/taillog.sh -l"
	    done
	;;
	s)
            for i in `awk '{print $1}' /sysy/yfs/etc/cluster.conf `
            do
                echo $i
                scp $i:/var/log/uss/linex100.log /var/log/uss/"$i"_linex100.log
                echo scp $i:/var/log/uss/linex100.log /var/log/uss/"$i"_linex100.log
            done
	;;
	l)
         
            if [ -d "$LOG_DIR" ];then
                echo > /var/log/uss/linex100.log
                date >> /var/log/uss/linex100.log
                for log in `find "$LOG_DIR" -type f `
                do
                    echo ===========  $log  =========== >> /var/log/uss/linex100.log
                    tail -n $LINE $log >> /var/log/uss/linex100.log
                done
            else
                echo "No such directory" 
            fi
	;;
    esac
done
