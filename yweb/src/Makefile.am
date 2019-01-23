sbin_PROGRAMS=yweb_server

yweb_server_SOURCES = \
	mime.c \
	str.c \
	match.c \
	tdate_parse.c \
	request.c \
	response.c \
	http_ops.c \
	http_proto.c \
	http_readdir.c \
	main.c \
	http_state_machine.c

AM_LDFLAGS = @LDFLAGS@ \
   -L../../parser/lib -lparser \
   -L../../yfs/build -lyfs \
   -L../../ynet/build -lynet \
   -L../../ylib/lib -lylib \
   -lpthread -lcrypt -luuid -laio -lssl

mime.c: mime_encodings.h mime_types.h

mime_encodings.h: mime_encodings.txt
	rm -f mime_encodings.h
	sed < mime_encodings.txt > mime_encodings.h \
	  -e 's/#.*//' -e 's/[ 	]*$$//' -e '/^$$/d' \
	  -e 's/[ 	][ 	]*/", 0, "/' -e 's/^/{ "/' -e 's/$$/", 0 },/'

mime_types.h: mime_types.txt
	rm -f mime_types.h
	sed < mime_types.txt > mime_types.h \
	  -e 's/#.*//' -e 's/[ 	]*$$//' -e '/^$$/d' \
	  -e 's/[ 	][ 	]*/", 0, "/' -e 's/^/{ "/' -e 's/$$/", 0 },/'
