#ifndef __CORENET_CONNECT_H__
#define __CORENET_CONNECT_H__


int corenet_tcp_connect(const nid_t *nid, uint32_t addr, sockid_t *sockid);
int corenet_tcp_passive();
int corenet_rdma_connect(const nid_t *nid, uint32_t addr, sockid_t *sockid);
int corenet_rdma_passive();
/** @file 不同节点上多个core间的RPC.
 *
 * CORE地址： <nid, core hash>
 *
 * 本地缓存了CORE地址到sockid的映射关系。
 *
 * 如需要支持跨集群，还需把集群ID编入CORE地址。
 */

#if 0
int corenet_connect_host(const char *host, sockid_t *sockid);
#endif


#endif
