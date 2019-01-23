#!/bin/bash

CONF_DIR="/sysy/yfs/etc"
YFSCONF="yfs.conf"
CLUSTER="cluster.conf"

usage()
{
    echo "Usage: $0 [OPTIONS]"
    echo "-i install"
    echo "-e remove"

    exit 1
}

install()
{

    if [ -e $CONF_DIR ];
    then
        echo "$CONF_DIR exists, ingore"
    else
        mkdir -p $CONF_DIR

        cp $YFSCONF $CONF_DIR

        cp $CLUSTER $CONF_DIR
    fi
}

remove()
{
    echo "remove $CONF_DIR"
    rm -fr $CONF_DIR
}

if [ $# -lt 1 ]
then 
    usage
fi

while getopts ieh options
do
    case $options in
        i)
        echo "install conf"
        install
        ;;
        e)
        echo "remove conf"
        remove
        ;; 
        h)
        usage
        ;;
        \?)
        usage
        ;;
    esac
done
        
