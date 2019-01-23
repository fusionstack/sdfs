sbin_PROGRAMS=yfs_mds

yfs_mds_SOURCES = \
	inode_proto.c \
	mds_inode.c \
	namei.c \
	main.c \
	mds.c \
	mds_events.c \
	mds_state_machine.c \
	mirror.c \
	dir.c \
	disk.c \
	super.c \
	object.c \
	img.c \
	diskpool.c \
	nodepool.c \
	lvs.c \
	namei_cache.c \
	container.c \
	election.c

AM_LDFLAGS = @LDFLAGS@ \
        -L../../parser/lib -lparser \
	-L../../yfs/build -lyfs \
	-L../../ynet/build -lynet \
	-L../../ylib/lib -lylib \
	-lpthread -lcrypt -lcrypto -luuid -laio -lssl
