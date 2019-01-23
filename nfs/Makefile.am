sbin_PROGRAMS=ynfs_server\
		nlm_server

ynfs_server_SOURCES = \
	attr.c \
	error.c \
	readdir.c \
	nfs_events.c \
	mountlist.c \
	nfs_state_machine.c \
	xdr_nfs.c \
	nfs_proc.c \
	main.c
nlm_server_SOURCES = \
	nlm_events.c\
	nlm_state_machine.c\
	xdr_nlm.c\
	nlm_lkcache.c\
	nlm_nsm.c\
	hostcache.c\
	mini_nlm.c

AM_LDFLAGS = @LDFLAGS@ \
   -L../../parser/lib -lparser \
   -L../../yfs/build -lyfs \
   -L../../ynet/build -lynet \
   -L../../ylib/lib -lylib \
   -lpthread -lcrypt -lcrypto -luuid -laio -lssl
