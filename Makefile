conf_list = ylib ynet yfs ynfs yftp yiscsi yfuse mds

program_list = parser
program_list += $(conf_list)
PLATFORM=`uname -m`
PLATFORM_OS=`lsb_release -d |awk '{print $$2}'`
OS_VERSION=`lsb_release -r | awk -F'.' '{print $$1}' |awk '{print $$2}'`

all: version 
	for i in $(program_list); do \
		make -j3 -C $$i; \
		if [ $$? != 0 ]; then \
			break; \
		fi \
	done

install: version
	lsb_release; \
	if [ $$? != 0 ]; then \
		echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!; \
		exit 1; \
	fi \

	for i in $(program_list); do \
		make -j3 -C $$i install; \
		if [ $$? != 0 ]; then \
			break; \
		fi \
	done
	make -C etc install
	make -C scripts install
#	make -C doc install
#	make -C oss install
#	make -C ydog install
	cp -f utils/startup.sh /sysy/yfs/app/sbin/
	cp -f yfs/utils/rebud_cds_jnl.py /sysy/yfs/app/bin/
	cp -dPR qa/auto/ /sysy/yfs/

version:
	for i in $(program_list); do \
		cd $$i && ../utils/get_version.sh && cd ..; \
	done

conf:
	cd parser && ../utils/autogen.sh && ../utils/conf.sh "-g" && cd ..;
	for i in $(conf_list); do \
		echo $$i && cd $$i && ../utils/autogen.sh && ../utils/conf.sh "-g -Werror" && cd ..; \
	done

prof:
	cd parser && ../utils/autogen.sh && ../utils/conf.sh "-pg -g" && cd ..;
	cd yfuse && ../utils/autogen.sh && ../utils/conf.sh "-pg -g" && cd ..;
	cd ylib && ../utils/autogen.sh && ../utils/conf.sh "-pg -g" && cd ..;
	cd ynet && ../utils/autogen.sh && ../utils/conf.sh "-pg -g" && cd ..;
	cd yfs && ../utils/autogen.sh && ../utils/conf.sh "-pg -g" && cd ..;
	cd ynfs && ../utils/autogen.sh && ../utils/conf.sh "-pg -g -static" && cd ..;
	cd yftp && ../utils/autogen.sh && ../utils/conf.sh "-pg -g -static" && cd ..;
	cd yweb && ../utils/autogen.sh && ../utils/conf.sh "-pg -g -static" && cd ..
	cd mds && ../utils/autogen.sh && ../utils/conf.sh "-pg -g -static" && cd ..; \

release:
	for i in $(program_list); do \
		echo $$i && cd $$i && ../utils/autogen.sh && ../utils/conf.sh "-O2" && cd ..; \
	done

clean:
	for i in $(program_list); do \
		make -C $$i clean; \
	done

distclean:
	for i in $(program_list); do \
		make -C $$i distclean; \
	done

wss:
	make -C wss_clib install
	make -C ftproxy install

wssconf:
	cd wss_clib && ../utils/autogen.sh && ../utils/conf.sh && cd ..
	cd ftproxy && ../utils/autogen.sh && ../utils/conf.sh && cd ..

tests:
	cd ylib && make test
tar:
	utils/app_tar.sh
