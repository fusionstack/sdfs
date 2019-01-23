PYLIB = `python-config --libs`
PYINC = `python-config --includes`
CFLAGS = -g -Werror -D_FILE_OFFSET_BITS=64 -D_REENTRANT -I../../../ylib/include -I ../../../ynet/include -I../../../yfs/include -I../../mdc
CLIBS = -L/sysy/yfs/app/lib/ -lyfs -lynet -lylib -lcrypt -lrt

all: yfs.so

yfs.so: yfs.o
	gcc -shared -o $@ $^ $(PYLIB) $(CLIBS)

yfs.o: yfs.c
	gcc -fpic -c $(CFLAGS) $(PYINC) $<

clean:
	rm -f yfs.so yfs.o

install:
	cp yfs.so /var/lib/python-support/python2.6
