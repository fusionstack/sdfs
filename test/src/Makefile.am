noinst_PROGRAMS=\
	test_skiplist \
	test_bitmap \
	ynfs_iotest \
	testlock \
	io_test

io_test_SOURCES = io_test.c

#bigfile_4g+_SOURCES = \
#	bigfile_4g+.c

#testyfscli_SOURCES = \
#	yfsclitest.c

# testftp_SOURCES = \
#     yftptest.c

# test_fuse_SOURCES = \
#     test_fuse.c

# aio_test_SOURCES= \
#     aio_test.c

# pread_test_SOURCES= \
#     pread_test.c

# test_seek_SOURCES= test_seek.c

test_skiplist_SOURCES=test_skiplist.c
test_bitmap_SOURCES=test_bitmap.c
testlock_SOURCES=testlock.c
#ynfs_functest_SOURCES=md5.c main.c functest.c
ynfs_iotest_SOURCES=md5.c ynfs_iotest.c

AM_LDFLAGS = @LDFLAGS@ \
   -L../../parser/lib -lparser\
   -L../../yfs/build -lyfs \
   -L../../ynet/build -lynet \
   -L../../ylib/lib -lylib\
   -lpthread -lcrypto -laio -lssl
