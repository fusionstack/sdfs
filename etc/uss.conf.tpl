globals {
        chunk_rep 2;
        rpc_timeout 20;
        network 192.168.1.0;
        mask 255.255.255.0;
        performance_analysis 1;

        #io_mode random;
        #dir_refresh 300;
        #cache_size 256M; 
        #home /sysy/yfs;
        #check_mountpoint on;
        #check_version off;
        #maxcore on;
        #master_vip "192.168.1.1/24:eth2;192.168.1.1.2/24:eth2;192.168.1.100/24"; #后面的掩码可以省略
        #zk_hosts "192.168.1.1:1111;192.168.1.1.2:1111;192.168.1.100:1111";

        #/*If more than Max_reboot number of restarts occur in the last Max_date seconds,*/
        #/*then the supervisor terminates all the child processes and then itself.*/
        #restart 1;

        networks {
                192.168.1.0/24;
                192.168.2.0/24;
        }
}

mds {
    object_hardend on;
    disk_keep 10G;

    #redis 启动有加载时间，需要等待。默认2000秒。
    #redis_wait 2000;

    #redis 线程数, 默认是20个
    #redis_thread 20;
}

cds {
    unlink_async on;
    ha_mode 0;#0:recover manually, 1:recover when write, 2:recover when access
    queue_depth 127;
    prealloc_max 256;
}

yiscsi {
        iqn iqn.2001-04.com.meidisen;
 #       lun_blk_shift 12;       # 1 << 12 = 4096
}

log {
#    log_ylib off;
#    log_yliblock off;
#    log_ylibmem off;
#    log_ylibskiplist off;
#    log_ylibnls off;
#    log_ysock off;
#    log_ynet off;
#    log_yrpc off;
#    log_yfscdc off;
#    log_yfsmdc off;
#    log_fsmachine off;
#    log_yfslib off;
#    log_yiscsi off;
#    log_ynfs off;
#    log_yfsmds off;
#    log_cdsmachine off;
#    log_yfscds off;
#    log_yfscds_robot off;
#    log_proxy off;
}
