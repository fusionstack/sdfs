#!/bin/bash

###################
# Usage: ./replace.sh  dirname  s_syscall  d_syscall
###################

for file in `find "$1" -name '*.[c,h]'`
do
        eval    sed -i 's/"$2"/"$3"/g' "$file"
done


for file in `find "$1" -name '*.py'`
do
        eval    sed -i 's/"$2"/"$3"/g' "$file"
done

for file in `find "$1" -name '*.sh'`
do
        eval    sed -i 's/"$2"/"$3"/g' "$file"
done

for file in `find "$1" -name '*.rst'`
do
        eval    sed -i 's/"$2"/"$3"/g' "$file"
done

for file in `find "$1" -name '*.conf'`
do
        eval    sed -i 's/"$2"/"$3"/g' "$file"
done

