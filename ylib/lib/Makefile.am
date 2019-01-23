lib_LTLIBRARIES = libylib.la

libylib_la_SOURCES = \
	job_dock.c \
	config.c \
	auth.c \
	array_table.c \
	buffer.c \
	ec_isa_l.c \
	bitmap.c \
	cmp.c \
	crc32.c \
	crcrs.c \
	daemon.c \
	dbg.c \
	hash.c \
	hash_table.c \
	htable.c \
	cache.c \
	job_tracker.c \
	journal.c \
	lock.c \
	mem.c \
	nls.c \
	nls/nls_cp936.c \
	path.c \
	pipe_pool.c \
	skiplist.c \
	shm.c \
	stat.c \
	str.c \
	sysutil.c \
	timer.c \
	xdr.c \
	ylog.c \
	tpool.c \
	ytime.c \
	bmap.c \
	dynarray.c \
	privilege.c \
	md5.c  \
	squeue.c \
	round_journal.c \
	analysis.c \
	heap.c \
	mini_hashtb.c\
	bh.c

AM_LDFLAGS = @LDFLAGS@ \
	     -L../../parser/lib

libylib_la_LIBADD = -lparser -laio -lisal

