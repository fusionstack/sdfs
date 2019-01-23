lib_LTLIBRARIES = libparser.la

BUILT_SOURCES = confl.c confy.c confy.h

libparser_la_SOURCES = \
	confl.l \
	confy.y \
	configure.c

AM_YFLAGS = -d

clean-local:
	-rm -f confl.c confy.c confy.h

