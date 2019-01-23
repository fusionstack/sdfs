#!/bin/bash

if [ $# != 1 ] ; then
echo "USAGE: $0 BRANCH"
echo " e.g.: $0 testing"
exit 1;
fi

BRANCH=$1
echo "will build ${BRANCH}"

git checkout ${BRANCH}
ret=$?
if [ ${ret} != 0 ]
then
	echo "git check fail"
	exit 1
fi

git pull
ret=$?
if [ ${ret} != 0 ]
then
	echo "git pull fail"
	exit 1
fi

basepath=$(cd `dirname $0`; pwd)
ussdoc="ussdoc"
rm -rf doc/_build
cp -n docx/* doc/_static/docs/
cd doc
make html
rm -rf /var/www/html/ussdoc/
cp -r _build/html /var/www/html/ussdoc
cd ..

v_1=`cat /data/uss/v_num | cut -d _ -f 1`
vnum_old=`cat /data/uss/v_num | cut -d _ -f 2`
vnum_new=`expr $vnum_old + 1`
Vnum=$v_1"_"$vnum_new
echo $Vnum  > /data/uss/v_num
echo $Vnum

home="/opt/sdfs"
rm -rf $home/app
rm -rf $home/etc

#rm -rf ./build
mkdir -p ./build
cd build
cmake ..
make
make install

COMMIT=`git log |sed -n '/commit/p'|sed -n '1p'|awk '{print $2}' | cut -c -5`
#BRANCH=`git branch|sed -n '/\*/p' |awk '{print $2}' `

echo $Vnum
NOW=`date --rfc-3339=seconds | sed 's/\://g' | sed 's/\-//g' | sed 's/\ //g' | sed 's/\+.*//g'`
Time=`date +%y%m%d%H`
project='uss'

if `lsb_release -a|grep -q 'CentOS Linux release 7'`;then
sys="C7"
echo $sys
elif `lsb_release -a|grep -q 'CentOS release 6'`;then
sys="C6"
echo $sys
elif `lsb_release -a|grep -q Ubuntu`;then
sys="U"
echo $sys
fi

USS_APP=uss-app-$BRANCH-${Vnum}-$COMMIT-$Time-$sys.tar.gz
USS_ETC=uss-etc-$BRANCH-${Vnum}-$COMMIT-$Time-$sys.tar.gz
#USS_APP=uss-app-$BRANCH-$COMMIT-$Time-$sys.tar.gz
#USS_ETC=uss-etc-$BRANCH-$COMMIT-$Time-$sys.tar.gz
DIST=/tmp/$USS_APP
ETC=/tmp/$USS_ETC
TMP=`mktemp -d`

#NOW=$YEAR$MONTH$DAY$TIME

rm -rf $DIST
rm -rf $ETC

cd $home/app && tar czvf  $DIST * > /dev/null
cd $home/etc && tar czvf  $ETC * --exclude cluster.conf > /dev/null

PUBLIC="/var/www/html/sdfs/$BRANCH/$NOW/"
PUBLIC2="/var/www/html/sdfs/$BRANCH"

sudo mkdir -p $PUBLIC
sudo cp $DIST $PUBLIC
sudo cp $ETC $PUBLIC 
sudo ln -sf $PUBLIC/$USS_APP $PUBLIC2/$BRANCH-lastest-app.tar.gz
sudo ln -sf $PUBLIC/$USS_ETC $PUBLIC2/$BRANCH-lastest-etc.tar.gz
#sudo cp $CGRP  /var/www/html/$project/$BRANCH/$NOW
sudo cp $basepath/build/uss.license_gen $PUBLIC2

echo /var/www/html/sdfs/$BRANCH/$NOW
PACKAGE=$(ls $PUBLIC | grep project-$BRANCH)
for i in `ifconfig | grep 'inet '  | grep -v 'inet .*127' | awk '{if (index($2, ":")) {split($2, array, ":"); printf array[2]"\n"} else {print $2}}'`; do
    echo http://$i/sdfs/$BRANCH/uss-$BRANCH-lastest-app.tar.gz;
    echo http://$i/sdfs/$BRANCH/uss-$BRANCH-lastest-etc.tar.gz;
    echo http://$i/sdfs/$BRANCH/$NOW/$PACKAGE;
done

bash ../samba/app_tar.sh $BRANCH $NOW
bash ../nfs-ganesha/app_tar.sh $BRANCH $NOW
