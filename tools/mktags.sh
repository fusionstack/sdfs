#!/bin/bash

rm -f tags
rm -f cscope*
rm -f filenametags

echo -e "!_TAG_FILE_SORTED\t2\t/2=foldcase/" > filenametags
# find . -not -regex '.*\.\(Plo\|lo\|o\|png\|gif\)' -type f -printf "%f\t%p\t1\n" | sort -f >> filenametags 
find $(pwd) -name '*.[ch]' -type f -printf "%f\t%p\t1\n" | sort -f >> filenametags 

ctags -R --fields=+lS .

find $(pwd) -name "*.[ch]" > cscope.files
cscope -bq
