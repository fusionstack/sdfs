#! /bin/bash
while [ 1 ]
do
	find  "$1" -type f|while read file 
	do
        	if ! [ -L "$file" ];then

   	        	 echo "http://192.168.1.124/mds/data/$file"
        	fi
	done >> /dev/null 
done
