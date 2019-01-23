#!/bin/bash
help()
{
 echo usage:
 echo     -m \[read\|write\]
 echo     -d  dir
 echo     -s  file size  
 echo     '-n  file number'
 exit
}
writ()
{
 echo begin to write file ... 
  count=0
  sec=`date +%s` 
  echo  >$dir/filelist
  for i in `seq 0 $number`
  do
     dd if=/dev/zero of=$dir/$i bs=1 count=$size 2>/dev/null
     if [ $? == 0 ];then
     echo $dir/$i>>$dir/filelist
     count=`expr $count + 1`
     fi
  done
  nsec=`date +%s` 
  tsec=`expr $nsec - $sec`
  echo  write file :   $count
  echo  time       :   $tsec
}
rad()
{
 echo begin to read file ...
 count=0
 [ -z "$number" ]||number=`ls $dir -l |wc -l`
 sec=`date +%s`
 for j in `cat $dir/filelist`
 do
 cat $j >/dev/null&&count=`expr $count + 1`
 done
   nsec=`date +%s`
   tsec=`expr $nsec - $sec`
   echo  read file :   $count
   echo  time       :   $tsec
}
while getopts :m:d:s:n: OPTION
do
  case  $OPTION in 
       m)model=$OPTARG;;
       d)dir=$OPTARG;;
       s)size=$OPTARG;;
       n)number=$OPTARG;;
       *)help;;
  esac
done
dir=`echo $dir |sed 's/\/$//'`
if echo $dir |grep -v ^\/;then

ndir=`echo $dir |sed 's/^\.\///'`
dir=`pwd`/$ndir
fi
if [ "$model" == "write" ];then
[  -z "$model" ]||[ -z "$dir" ]||[ -z "$size" ]||[ -z "$number" ] &&help
if [ ! -d "$dir" ];then
  echo the $dir not exist!!
  exit
fi
writ
else
[  -z "$model" ]||[ -z "$dir" ] &&help
if [ ! -d "$dir" ];then
  echo the $dir not exist!!
  exit
fi
rad
fi

