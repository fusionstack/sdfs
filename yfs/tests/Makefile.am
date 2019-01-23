#noinst_PROGRAMS = yfs_sanity

#yfs_sanity_SOURCES = yfs_sanity.c

AM_LDFLAGS = @LDFLAGS@ \
	-L../../ylib/lib -lylib \
	-L../../parser/lib -lparser \
	-L../../ynet/build -lynet \
	-L../build -lyfs \
	-lpthread -lcrypt -luuid -laio -lssl
