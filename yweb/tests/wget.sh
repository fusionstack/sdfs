#!/bin/sh

while [ 1 ]; do
    curl --connect-timeout 8 --tcp-nodelay --output /dev/null \
         --url http://192.168.1.1[3-5]:8080/[1-9].jpg
done
