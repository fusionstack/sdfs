project (sdfs)

cmake_minimum_required(VERSION 2.8)
set(CMAKE_VERBOSE_MAKEFILE OFF)

set (USS_VERSION_MAJOR 1)
set (USS_VERSION_MINOR 0)

set(CMAKE_INSTALL_PREFIX "/opt/sdfs")
set(SDFS_HOME "/opt/sdfs")
set(USE_EPOLL 1)

if(VALGRIND)
    ADD_DEFINITIONS(-DCONFIG_VALGRIND_H)
endif(VALGRIND)

if(PERF_MDS)
    ADD_DEFINITIONS(-DCONFIG_PERF_MDS_H)
endif(PERF_MDS)

#set(CMAKE_INSTALL_PREFIX "/tmp/yfs/")
#set(CMAKE_CURRENT_SOURCE_DIR "./")

configure_file (
    "${CMAKE_CURRENT_SOURCE_DIR}/include/cmakeconfig.h.ini"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/cmakeconfig.h"
)

execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tools/get_version.sh ${CMAKE_CURRENT_SOURCE_DIR})

find_package(BISON)
find_package(FLEX)
BISON_TARGET(MyBison ${CMAKE_CURRENT_SOURCE_DIR}/parser/lib/confy.y ${CMAKE_CURRENT_BINARY_DIR}/confy.c)
FLEX_TARGET(MyFlex ${CMAKE_CURRENT_SOURCE_DIR}/parser/lib/confl.l ${CMAKE_CURRENT_BINARY_DIR}/confl.c)
#add_executable(Foo main.cpp ${BISON_MyParser_OUTPUTS})
#ADD_FLEX_BISON_DEPENDENCY(MyBison MyFlex)
#set(CMAKE_BUILD_TYPE Release)
set(CMAKE_C_FLAGS_PARSER "-W -Wall -DDEBUG -g -fPIC -Werror=return-type -D_GNU_SOURCE -D_REENTRANT")

set(CMAKE_C_FLAGS "-W -Wall -DDEBUG -g  -fPIC -Werror -Wno-implicit-fallthrough -Werror=return-type -Wno-format-truncation -Wno-format-overflow -Wno-misleading-indentation -Wno-deprecated-declarations -Wno-cast-function-type -Wno-int-in-bool-context -Wno-pointer-compare -D_GNU_SOURCE -D_REENTRANT -D_FILE_OFFSET_BITS=64 -std=c99 -fms-extensions -gsplit-dwarf")
set(CMAKE_C_LIBS sdfs pthread crypt crypto uuid ssl isal z jemalloc m curl yajl attr hiredis tirpc)

include_directories(
    /usr/local/include
    #/usr/include/tirpc/
    #${CMAKE_CURRENT_SOURCE_DIR}/license/include
    ${CMAKE_CURRENT_SOURCE_DIR}/itest/include
    ${CMAKE_CURRENT_SOURCE_DIR}/yfuse/include
    ${CMAKE_CURRENT_SOURCE_DIR}/yfuse3/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test/include
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/include
    ${CMAKE_CURRENT_SOURCE_DIR}/yftp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule
    ${CMAKE_CURRENT_SOURCE_DIR}/mond
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs
    ${CMAKE_CURRENT_SOURCE_DIR}/cds
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/include
    ${CMAKE_CURRENT_SOURCE_DIR}/yweb/include
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/include
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/include
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/include
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/include
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/
    ${CMAKE_CURRENT_BINARY_DIR}/
)

add_library(sdfs SHARED
    ${CMAKE_CURRENT_SOURCE_DIR}/parser/lib/configure.c
    ${CMAKE_CURRENT_BINARY_DIR}/confy.c
    ${CMAKE_CURRENT_BINARY_DIR}/confl.c
    ${CMAKE_CURRENT_BINARY_DIR}/confy.h

    #------ylib------

    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/timer.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/redis_util.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/worker.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/job_dock.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/config.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/auth.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/array_table.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/buffer.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/ec_isa_l.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/cmp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/crc32.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/crcrs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/daemon.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/dbg.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/hash.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/hash_table.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/htable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/job_tracker.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/journal.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/lock.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/mem.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/mem_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/mem_hugepage.c
    #${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/nls.c
    #${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/nls/nls_cp936.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/path.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/skiplist.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/stat.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/str.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/sysutil.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/timer.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/ylog.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/plock.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/tpool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/ytime.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/bmap.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/privilege.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/md5.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/squeue.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/analysis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/mini_hashtb.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/bh.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/fnotify.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/nodectl.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/removed.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/etcd.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/etcd-api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/base64_urlsafe.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/base64.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/cJSON.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/aio.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/core.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/corenet_tcp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/corenet_connect_tcp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/corenet_maping.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/corerpc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/cpuset.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/schedule.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/gettime.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/schedule_thread.c
    ${CMAKE_CURRENT_SOURCE_DIR}/schedule/variable.c
    #${CMAKE_CURRENT_SOURCE_DIR}/schedule/vm.c

    #------ynet------

    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/nodeid.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_buffer.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_passive.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_tcp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_udp.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_xnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sock_xmit.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/sock/sdevent.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_crc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_events.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_lib.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/network.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/maping.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_passive.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_table.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/heartbeat.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/xnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_xnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_xmit.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/net_rpc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/conn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/net/main_loop.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_lib.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/msgqueue.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_proto.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_passive.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_reply.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_xnect.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_request.c
    #${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_request_old.c
    #${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_reply_old.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_xmit.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ynet/rpc/rpc_table.c

    #------sdfs------

    #${CMAKE_CURRENT_SOURCE_DIR}/license/src/aes.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/redis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/redis_co.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/redis_pipeline.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/redis_vol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/redis_conn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_attr.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/dir_redis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/chunk_redis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/inode_redis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/kv_redis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/attr_queue.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/sdfs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/allocator.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/io_analysis.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/sdfs_inode.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/sdfs_dir.c
    ${CMAKE_CURRENT_SOURCE_DIR}/cds/cds_rpc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/sdfs_chunk.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sdfs/sdfs_chunk_recovery.c
    #${CMAKE_CURRENT_SOURCE_DIR}/license/src/license_helper.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_dir.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_vol.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_node.c
    #${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_super.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_chunk.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_file.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_user.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_group.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_quota.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_root.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_share.c
    #${CMAKE_CURRENT_SOURCE_DIR}/metadata/md_lvm_qos.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata/md.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/dir.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/file.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/node.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/super.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/raw.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/file_table.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/yfs_lib.c
    #${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/worm_cli_lib.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/quota.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/user.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/group.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/posix_acl.c
    #${CMAKE_CURRENT_SOURCE_DIR}/yfs/libyfs/flock.c

    #------cds------

    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/cds.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/cds_volume.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/cds_hb.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/cds_lib.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/disk.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/chkinfo.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/dpool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/cds/replica.c
    ${CMAKE_CURRENT_SOURCE_DIR}/cds/diskio.c

    #------mond------
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_main.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/disk.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/diskpool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool_hdd.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool_ssd.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mond/mond_rpc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mond/mond_kv.c
)

set (MDS_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/main.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_main.c
)

set (CDS_SRC_LIST
	${CMAKE_CURRENT_SOURCE_DIR}/yfs/cds/main.c
)

set (YNFS_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/xdr_nlm.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_events.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm4.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_async.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_lkcache.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_nsm.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/hostcache.c

    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/xdr.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/sunrpc_passive.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/sunrpc_reply.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/sunrpc_proto.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/attr.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/error.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/readdir.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs_events.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/mountlist.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs_state_machine.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs3.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs_mount.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs_remove.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/xdr_nfs.c
    #${CMAKE_CURRENT_SOURCE_DIR}/nfs/nfs_proc.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/main.c
)

set (YNLM_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_events.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_state_machine.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/xdr_nlm.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_lkcache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/nlm_nsm.c
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs/hostcache.c
)

set (YFTP_SRC_LIST
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/main.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/ftpfeatures.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/prelogin.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/postlogin.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/cmdio.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/dataio.c
	${CMAKE_CURRENT_SOURCE_DIR}/yftp/src/session.c
	${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/md5.c
)
set (YWEB_SRC_LIST
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/main.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/http_ops.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/http_proto.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/http_readdir.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/http_state_machine.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/match.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/mime.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/request.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/response.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/str.c
	${CMAKE_CURRENT_SOURCE_DIR}/yweb/src/tdate_parse.c
)

set (YFUSE_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/yfuse/src/main.c
)

LINK_DIRECTORIES("/usr/local/lib")
set (YFUSE3_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/yfuse3/src/main.c
)

set (YLICENSE_SRC_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.license.c
    ${CMAKE_CURRENT_SOURCE_DIR}/license/src/license_helper.c
    ${CMAKE_CURRENT_SOURCE_DIR}/license/src/aes.c
)

add_executable(sdfs.mond ${MDS_SRC_LIST})
target_link_libraries(sdfs.mond ${CMAKE_C_LIBS} attr hiredis)

add_executable(sdfs.cds ${CDS_SRC_LIST})
target_link_libraries(sdfs.cds ${CMAKE_C_LIBS} attr)

add_executable(sdfs.nfs ${YNFS_SRC_LIST})
target_link_libraries(sdfs.nfs ${CMAKE_C_LIBS})

#add_executable(sdfs.nlm ${YNLM_SRC_LIST})
#target_link_libraries(sdfs.nlm ${CMAKE_C_LIBS})

if(${USE_EPOLL})
add_executable(sdfs.ftp ${YFTP_SRC_LIST})
target_link_libraries(sdfs.ftp ${CMAKE_C_LIBS})
endif(${USE_EPOLL})

#add_executable(sdfs.web ${YWEB_SRC_LIST})
#target_link_libraries(sdfs.web ${CMAKE_C_LIBS})

LINK_DIRECTORIES("/usr/local/lib")
add_executable(sdfs.fuse ${YFUSE_SRC_LIST})
target_link_libraries(sdfs.fuse ${CMAKE_C_LIBS} attr fuse)

#add_executable(sdfs.fuse3 ${YFUSE3_SRC_LIST})
#target_link_libraries(sdfs.fuse3 ${CMAKE_C_LIBS} attr fuse3)

add_executable(sdfs.configdump ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.configdump.c)
target_link_libraries(sdfs.configdump ${CMAKE_C_LIBS})

add_executable(sdfs.write ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.write.c)
target_link_libraries(sdfs.write ${CMAKE_C_LIBS})

add_executable(sdfs.mv ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.mv.c)
target_link_libraries(sdfs.mv ${CMAKE_C_LIBS})

#add_executable(sdfs.zk ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.zk.c)
#target_link_libraries(sdfs.zk ${CMAKE_C_LIBS})

add_executable(sdfs.cat ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.cat.c)
target_link_libraries(sdfs.cat ${CMAKE_C_LIBS})

add_executable(sdfs.attr ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.attr.c)
target_link_libraries(sdfs.attr ${CMAKE_C_LIBS})

#add_executable(sdfs.objmv ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.objmv.c)
#target_link_libraries(sdfs.objmv ${CMAKE_C_LIBS})

add_executable(sdfs.mkdir ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.mkdir.c)
target_link_libraries(sdfs.mkdir ${CMAKE_C_LIBS})

add_executable(sdfs.truncate ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.truncate.c)
target_link_libraries(sdfs.truncate ${CMAKE_C_LIBS})

#add_executable(sdfs.mdstat ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.mdstat.c)
#target_link_libraries(sdfs.mdstat ${CMAKE_C_LIBS})

add_executable(sdfs.touch ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.touch.c)
target_link_libraries(sdfs.touch ${CMAKE_C_LIBS})

add_executable(sdfs.perf ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.perf.c)
target_link_libraries(sdfs.perf ${CMAKE_C_LIBS})

add_executable(test_aio ${CMAKE_CURRENT_SOURCE_DIR}/test/src/test_aio.c ${CMAKE_CURRENT_SOURCE_DIR}/test/src/test_aio_lib.c)
target_link_libraries(test_aio ${CMAKE_C_LIBS})

set (MDS_SRC_PERF_LIST
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_main.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_events.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_state_machine.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/disk.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/diskpool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool_hdd.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/nodepool_ssd.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/election.c
    ${CMAKE_CURRENT_SOURCE_DIR}/license/src/license_helper.c
    ${CMAKE_CURRENT_SOURCE_DIR}/mds/mds/mds_license.c
)

#add_executable(sdfs.perf_mds ${MDS_SRC_PERF_LIST} ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.perf_mds.c)
#target_link_libraries(sdfs.perf_mds ${CMAKE_C_LIBS} attr)

#add_executable(sdfs.prof ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.prof.c)
#target_link_libraries(sdfs.prof ${CMAKE_C_LIBS})

add_executable(sdfs.ls ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.ls.c)
target_link_libraries(sdfs.ls ${CMAKE_C_LIBS})

#add_executable(sdfs.license ${YLICENSE_SRC_LIST})
#target_link_libraries(sdfs.license ${CMAKE_C_LIBS})

set (USS_USER_SRC
	${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.user.c
	${CMAKE_CURRENT_SOURCE_DIR}/ylib/lib/md5.c
)
add_executable(sdfs.user ${USS_USER_SRC})
target_link_libraries(sdfs.user ${CMAKE_C_LIBS})

add_executable(sdfs.group ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.group.c)
target_link_libraries(sdfs.group ${CMAKE_C_LIBS})

add_executable(sdfs.health ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/sdfs.health.c)
target_link_libraries(sdfs.health ${CMAKE_C_LIBS})

add_executable(sdfs.mon ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/sdfs.mon.c)
target_link_libraries(sdfs.mon ${CMAKE_C_LIBS})

add_executable(sdfs.ln ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.ln.c)
target_link_libraries(sdfs.ln ${CMAKE_C_LIBS})

add_executable(sdfs.rmdir ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.rmdir.c)
target_link_libraries(sdfs.rmdir ${CMAKE_C_LIBS})

add_executable(sdfs.rm ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.rm.c)
target_link_libraries(sdfs.rm ${CMAKE_C_LIBS})

add_executable(sdfs.cp ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.cp.c)
target_link_libraries(sdfs.cp ${CMAKE_C_LIBS})

#add_executable(sdfs.chkstat ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.chkstat.c)
#target_link_libraries(sdfs.chkstat ${CMAKE_C_LIBS})

add_executable(sdfs.stat ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.stat.c)
target_link_libraries(sdfs.stat ${CMAKE_C_LIBS})

add_executable(sdfs.chmod ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.chmod.c)
target_link_libraries(sdfs.chmod ${CMAKE_C_LIBS})

add_executable(sdfs.clone ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.clone.c)
target_link_libraries(sdfs.clone ${CMAKE_C_LIBS})

add_executable(sdfs.md5sum ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.md5sum.c)
target_link_libraries(sdfs.md5sum ${CMAKE_C_LIBS})

#add_executable(sdfs.sysop ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.sysop.c)
#target_link_libraries(sdfs.sysop ${CMAKE_C_LIBS})

#add_executable(sdfs.mdsdrop ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.mdsdrop.c)
#target_link_libraries(sdfs.mdsdrop ${CMAKE_C_LIBS})

#add_executable(sdfs.cdstat ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.cdstat.c)
#target_link_libraries(sdfs.cdstat ${CMAKE_C_LIBS})

#add_executable(sdfs.ffc ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.ffc.c)
#target_link_libraries(sdfs.ffc ${CMAKE_C_LIBS})

#add_executable(sdfs.hsm ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.hsm.c)
#target_link_libraries(sdfs.hsm ${CMAKE_C_LIBS})

#add_executable(sdfs.fallocate ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.fallocate.c)
#target_link_libraries(sdfs.fallocate ${CMAKE_C_LIBS})

add_executable(sdfs.du ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.du.c)
target_link_libraries(sdfs.du ${CMAKE_C_LIBS})

add_executable(sdfs.statvfs ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.statvfs.c)
target_link_libraries(sdfs.statvfs ${CMAKE_C_LIBS})

set (USS_WORM_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.worm.c
)
add_executable(sdfs.worm ${USS_WORM_SRC})
target_link_libraries(sdfs.worm ${CMAKE_C_LIBS})

add_executable(sdfs.chown ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.chown.c)
target_link_libraries(sdfs.chown ${CMAKE_C_LIBS})

add_executable(sdfs.quota ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.quota.c)
target_link_libraries(sdfs.quota ${CMAKE_C_LIBS})

add_executable(sdfs.share ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.share.c)
target_link_libraries(sdfs.share ${CMAKE_C_LIBS})

set (USS_ADMIN_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/uss.admin.c
    ${CMAKE_CURRENT_SOURCE_DIR}/yfs/utils/option.c
)
add_executable(sdfs.admin ${USS_ADMIN_SRC})
target_link_libraries(sdfs.admin ${CMAKE_C_LIBS} attr hiredis)

install(TARGETS sdfs LIBRARY DESTINATION app/lib)

install(TARGETS sdfs.mond RUNTIME DESTINATION app/sbin)
install(TARGETS sdfs.cds RUNTIME DESTINATION app/sbin)
install(TARGETS sdfs.nfs RUNTIME DESTINATION app/sbin)
#install(TARGETS sdfs.nlm RUNTIME DESTINATION app/sbin)
if(${USE_EPOLL})
#install(TARGETS sdfs.iscsi RUNTIME DESTINATION app/sbin)
install(TARGETS sdfs.ftp RUNTIME DESTINATION app/sbin)
endif(${USE_EPOLL})
#install(TARGETS sdfs.web RUNTIME DESTINATION app/sbin)
install(TARGETS sdfs.fuse RUNTIME DESTINATION app/sbin)
#install(TARGETS sdfs.fuse3 RUNTIME DESTINATION app/sbin)

install(TARGETS sdfs.configdump RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.write RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.mv RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.zk RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.cat RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.attr RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.objmv RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.mkdir RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.truncate RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.touch RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.perf RUNTIME DESTINATION app/bin)
# install(TARGETS sdfs.perf_mds RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.prof RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.ls RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.health RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.mon RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.ln RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.rmdir RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.rm RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.cp RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.chkstat RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.chmod RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.clone RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.md5sum RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.sysop RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.mdsdrop RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.ffc RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.hsm RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.fallocate RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.du RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.license RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.mdstat RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.stat RUNTIME DESTINATION app/bin)
#install(TARGETS sdfs.cdstat RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.statvfs RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.admin RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.worm RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.chown RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.quota RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.share RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.user RUNTIME DESTINATION app/bin)
install(TARGETS sdfs.group RUNTIME DESTINATION app/bin)

install(TARGETS test_aio RUNTIME DESTINATION app/test)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/admin DESTINATION app/ FILE_PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include DESTINATION app/ FILE_PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)

#execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/include/install.sh -i ${CMAKE_INSTALL_PREFIX}/include)

#SET(CMAKE_ALLOW_LOOSE_LOOP_CONSTRUCTS ON)
IF (EXISTS ${CMAKE_INSTALL_PREFIX}/etc)
    MESSAGE("etc exists, ignore")
ELSE()
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/sdfs.conf DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/named.conf DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/exports.conf DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/cluster.conf DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/redis.conf.tpl DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/ftp.conf DESTINATION etc/)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/etc/clean_cluster.sh DESTINATION etc/)
ENDIF()
#install(TARGETS ../admin/utils.py RUNTIME DESTINATION admin/)
#install(TARGETS ../admin/instence.py RUNTIME DESTINATION admin/)
#install(TARGETS ../admin/node.py RUNTIME DESTINATION admin/)

#execute_process(COMMAND ln -s CMAKE_INSTALL_PREFIX/app/admin/cluster.py CMAKE_INSTALL_PREFIX/app/bin/uss.cluster)

# build a CPack driven installer package
include (InstallRequiredSystemLibraries)
set (CPACK_RESOURCE_FILE_LICENSE  "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
#set (CPACK_SET_DESTDIR true)
#set (CPACK_PACKAGE_INSTALL_DIRECTORY "./sdfs")
set (CPACK_PACKAGE_VERSION_MAJOR "${USS_VERSION_MAJOR}")
set (CPACK_PACKAGE_VERSION_MINOR "${USS_VERSION_MINOR}")
include (CPack)
