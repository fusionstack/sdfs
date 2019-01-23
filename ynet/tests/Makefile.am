noinst_PROGRAMS = \
	msgqueue_cp \
	msgqueue_write \
	msgqueue_read


#test_proxy_SOURCES = proxy.c 
#test_listener_SOURCES = listener.c 
#test_speaker_SOURCES = speaker.c 
msgqueue_cp_SOURCES = msgqueue_cp.c
msgqueue_write_SOURCES = msgqueue_write.c
msgqueue_read_SOURCES = msgqueue_read.c

AM_LDFLAGS = @LDFLAGS@ \
	-L../../ynet/build -lynet \
	-L../../ylib/lib -lylib \
	-lpthread -lcrypt -luuid -laio -lssl
