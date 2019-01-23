#!/bin/sh

## http://www.amath.washington.edu/~lf/tutorials/autoconf/toolsmanual.html

aclocal --version | exit
autoheader --version | exit
autoconf --version | exit
automake --version | exit
libtool --version | exit
libtoolize --version | exit

rm -f ltconfig ltmain.sh config.cache aclocal.m4

# remove the autoconf cache
rm -rf autom4te*.cache

touch NEWS README AUTHORS ChangeLog

set -e

echo "aclocal... "
${ACLOCAL:-aclocal} -I .
echo "autoheader... "
${AUTOHEADER:-autoheader}

echo "libtoolize... "
${LIBTOOLIZE:-libtoolize} --automake -c -f

echo "autoconf... "
${AUTOCONF:-autoconf}
echo "automake... "
${AUTOMAKE:-automake} -a -c

# remove the autoconf cache
rm -rf autom4te*.cache

echo "Now type './configure ...' and 'make' to compile."
