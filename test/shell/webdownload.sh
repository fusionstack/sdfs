#!/bin/bash

function download
{
        count=0
        while [ $count -lt $1 ] ; do
                # wget --timeout=8 http://192.168.1.200/zzn/mk2list.sh -O /dev/null
                # wget --timeout=8 http://192.168.1.200/zzn/gcc-2.95.3.tar.gz -O /dev/null
                time wget --timeout=8 http://192.168.1.200/zzn/RFC-all.tar.gz -O /dev/null
                let "count+=1"
        done
        echo $1
}

time download 2
