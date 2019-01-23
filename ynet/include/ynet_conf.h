#ifndef __YNET_CONF_H__
#define __YNET_CONF_H__

#define MAX_NODENAME_LEN    16
#define MAX_SERVICENAME_LEN 16

#define YNET_QLEN 256

#define RPC_EPOLL_SIZE 8192

#define YNET_LOCALHOST "127.0.0.1"

#define YNET_TRANSPORT "tcp"

#define YNET_RPC_NONBLOCK 1
#define YNET_RPC_BLOCK 0

#define YNET_RPC_TUNNING 1
#define YNET_RPC_NONTUNNING 0

#define YNET_SOCK_MAX 10

#define YNET_NID_NULL      0
#define YNET_VERSION_NULL  0
#define YNET_NID_ERROR     (-2)

/*
 * http://www.iana.org/assignments/port-numbers
 *
 * The Dynamic and/or Private Ports are those from 49152 through 65535
 */

#define YNET_SERVICE_BASE 49152

#define YNET_SERVICE_RANGE (65535 - 49152)

#endif
