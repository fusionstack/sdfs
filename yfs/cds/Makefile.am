sbin_PROGRAMS=yfs_cds

yfs_cds_SOURCES = \
	main.c \
	cds.c \
	chunk.c \
	cds_robot.c \
	cds_volume.c \
	cds_proto.c \
	cds_hb.c \
	cds_lib.c \
	cds_events.c \
	cds_state_machine.c \
	cds_leveldb.c\
	objs_stm.c \
	objs.c \
	objs_event.c \
	inode_stm.c \
	inodes_event.c \
	inodes.c \
	disk.c \
	watchdog.c \
	rangelock.c \
	chkinfo.c \
	dpool.c

AM_LDFLAGS = @LDFLAGS@ \
	-L../../ylib/lib -lylib \
	-L../../parser/lib -lparser \
	-L../../ynet/build -lynet \
	-L../build -lyfs \
	-lpthread -lcrypt -lcrypto -luuid -laio -lssl -lattr
