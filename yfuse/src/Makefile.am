bin_PROGRAMS=yfuse

yfuse_SOURCES = \
			main.c

AM_LDFLAGS = @LDFLAGS@ \
   -L../../parser/lib -lparser \
   -L../../yfs/build -lyfs \
   -L../../ynet/build -lynet \
   -L../../ylib/lib -lylib \
   -L/usr/local/lib -lfuse3 \
   -lpthread -lcrypt -lcrypto -luuid -laio -lssl
