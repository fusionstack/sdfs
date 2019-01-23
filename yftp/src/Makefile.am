sbin_PROGRAMS=yftp_server

yftp_server_SOURCES = \
	main.c \
	ftpfeatures.c \
	prelogin.c \
	postlogin.c \
	cmdio.c \
	dataio.c \
	session.c

AM_LDFLAGS = @LDFLAGS@ \
   -L../../parser/lib -lparser \
   -L../../yfs/build -lyfs \
   -L../../ynet/build -lynet \
   -L../../ylib/lib -lylib \
   -lpthread -lcrypt -lcrypto -luuid -laio -lssl
