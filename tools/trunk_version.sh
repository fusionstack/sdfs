#!/bin/bash

PREFIX=/sysy/yfs/app
cd ../          # yfs/trunk
echo `svn info |sed -n '2p' |awk -F/ '{print $NF}'`  `svn info |sed -n '5p' |awk '{print " r"$2}'` > $PREFIX"/VERSION"

