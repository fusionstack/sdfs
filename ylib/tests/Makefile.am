noinst_PROGRAMS = \
	path \
	crc32 \
	cmd_test

crc32_SOURCES = crc32.c

path_SOURCES = path.c

cmd_test_SOURCES = cmd_test.c
#splice_SOURCES = splice.c

heap_test_SOURCES = heap_test.c

AM_LDFLAGS = @LDFLAGS@ \
	-L../../parser/lib -lparser \
        -L../lib -lylib \
        -lpthread -lcrypto -lcrypt -luuid -laio -lssl
