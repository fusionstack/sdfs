

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <semaphore.h>
#include <signal.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#define DBG_SUBSYS S_LIBSCHEDULE

#include "../net/xnect.h"
#include "adt.h"
#include "bh.h"
#include "cluster.h"
#include "configure.h"
#include "core.h"
#include "corenet.h"
#include "corenet_maping.h"
#include "corerpc.h"
#include "dbg.h"
#include "job_dock.h"
#include "mem_hugepage.h"
#include "msgarray.h"
#include "net_global.h"
#include "net_proto.h"
#include "net_table.h"
#include "rdma_event.h"
#include "rpc_table.h"
#include "schedule.h"
#include "sysutil.h"
#include "ylib.h"
#include "timer.h"
#include "variable.h"
#include "types.h"

#define MAX_SEG_COUNT 2

#define RDMA_CQ_POLL_COMP_OK 0
#define RDMA_CQ_POLL_COMP_EMPTY 1
#define RDMA_CQ_POLL_COMP_ERROR 2

/* poll CQ timeout in millisec (2 seconds) */
#define MAX_POLL_CQ_SIZE (512 * 1024)
#define MAX_POLL_CQ_TIMEOUT 2000
#define MAX_BUF_SIZE 512

//static __thread struct ibv_mr *gmr = NULL;
static struct rdma_event_channel **corenet_rdma_evt_channel;

typedef struct {
        struct list_head hook;
        sockid_t sockid;
        int cur_index;
        int end_index;
        int seg_count[MAX_BUF_SIZE];
        buffer_t buf_list[MAX_BUF_SIZE];
} corenet_rdma_fwd_t;

typedef corenet_rdma_node_t corenet_node_t;

static void *__corenet_get()
{
        return variable_get(VARIABLE_CORENET_RDMA);
}

static void __corenet_rdma_checklist_add(corenet_rdma_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->corenet.lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_add_tail(&node->hook, &corenet->corenet.check_list);

        sy_spin_unlock(&corenet->corenet.lock);
}

static void __corenet_rdma_checklist_del(corenet_rdma_t *corenet, corenet_node_t *node)
{
        int ret;

        ret = sy_spin_lock(&corenet->corenet.lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_del(&node->hook);

        sy_spin_unlock(&corenet->corenet.lock);
}

void corenet_rdma_check()
{
        int ret;
        time_t now;
        struct list_head *pos;
        corenet_node_t *node;
        corenet_rdma_t *__corenet_rdma__ = __corenet_get();

        now = gettime();
        if (now - __corenet_rdma__->corenet.last_check < 30) {
                return;
        }

        __corenet_rdma__->corenet.last_check = now;

        DINFO("corenet check\n");

        ret = sy_spin_lock(&__corenet_rdma__->corenet.lock);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        list_for_each(pos, &__corenet_rdma__->corenet.check_list)
        {
                node = (void *)pos;
                node->check(node->ctx);
        }

        sy_spin_unlock(&__corenet_rdma__->corenet.lock);
}

static void __corenet_rdma_free_node(corenet_rdma_t *rdma_net, corenet_node_t *node)
{

        if (!list_empty(&node->hook)) {
                __corenet_rdma_checklist_del(rdma_net, node);
        }

        YASSERT(node->queue_buf.len == 0);

        DINFO("rdma socket is closed %d, node->ctx %p node %p\n", node->sockid.sd, node->ctx, node);
        mbuffer_init(&node->queue_buf, 0);
        //  yfree((void **)&node->ctx);
        node->ev = 0;
        node->ctx = NULL;
        node->exec = NULL;
        node->exec1 = NULL;
        node->reset = NULL;
        node->recv = NULL;
        node->sockid.sd = -1;
        node->closed = 1;
        node->total_seg_count = 0;
        memset(&node->handler, 0x00, sizeof(rdma_conn_t));
}

static void __corenet_rdma_free_node1(core_t *_core, sockid_t *sockid)
{
        corenet_node_t *node;
        corenet_rdma_t *corenet;

        YASSERT(sockid->addr);
        YASSERT(sockid->type == SOCKID_CORENET);

        corenet = _core->rdma_net ? _core->rdma_net : __corenet_get();

        node = &corenet->array[sockid->sd];
        __corenet_rdma_free_node(corenet, node);
}

static int __corenet_get_free_node(corenet_node_t array[], int size)
{
        int i;
        corenet_node_t *node;

        for (i = 0; i < size; i++) {
                node = &array[i];
                sy_spin_lock(&node->lock);
                if (node->closed) {
                        node->closed = 0;
                        sy_spin_unlock(&node->lock);
                        return i;
                }
                sy_spin_unlock(&node->lock);
        }

        return -EBUSY;
}

int corenet_rdma_add(core_t *_core, sockid_t *sockid, void *ctx, core_exec exec, core_exec1 exec1, func_t reset, func_t check, func_t recv,
                     rdma_conn_t **_handler)
{
        int ret = 0, event, loc;
        corenet_node_t *node;
        corenet_rdma_t *corenet;
        rdma_conn_t *handler = NULL;
        struct in_addr sin_addr;
        nid_t from;
        char peer_addr[MAX_NAME_LEN] = "";

        YASSERT(sockid->addr);
        YASSERT(sockid->type == SOCKID_CORENET);
        YASSERT(_core->rdma_net);
        corenet = _core->rdma_net;

        event = EPOLLIN;

        loc = __corenet_get_free_node(&corenet->array[0], corenet->corenet.size);
        if (loc < 0) {
                ret = -loc;
                GOTO(err_ret, ret);
        }

        node = &corenet->array[loc];

        handler = &node->handler;
        sockid->sd = loc;
        handler->node_loc = loc;

        sin_addr.s_addr = sockid->addr;
        strcpy(peer_addr, inet_ntoa(sin_addr));
        ret = maping_addr2nid(peer_addr, &from);
        if (unlikely(ret)) {
                DERROR("hostname %s trans to nid failret (%u) %s\n", peer_addr, ret, strerror(ret));
                YASSERT(0);
        }

        DINFO("add host:%s sd %d, ev %o:%o, rdma handler:%p\n",
                                network_rname(&from),
                                loc, node->ev, event, handler);

        YASSERT((event & EPOLLOUT) == 0);

        if (check) {
                __corenet_rdma_checklist_add(corenet, node);
        } else {
                INIT_LIST_HEAD(&node->hook);
        }

        node->ev = event;
        node->ctx = ctx;
        node->exec = exec;
        node->exec1 = exec1;
        node->reset = reset;
        node->recv = recv;
        node->check = check;
        handler->core = _core;
        sockid->rdma_handler = (uintptr_t)handler;
        node->sockid = *sockid;

        ret = corenet_maping_loading(&from);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        handler->is_connected = 0;
        corenet_rdma_get(handler, 1);
        *_handler = handler;

        return 0;
err_ret:
        return ret;
}

static void __corenet_rdma_close(rdma_conn_t *rdma_handler)
{
        corenet_node_t *node = NULL;
        corenet_rdma_t *__corenet_rdma__ = rdma_handler->core->rdma_net;
        struct rdma_cm_id *cm_id;

        cm_id = rdma_handler->cm_id;
        cm_id->context = NULL;
        yfree(&rdma_handler->iov_addr);
        rdma_destroy_qp(rdma_handler->cm_id);
        // rdma_destroy_id(rdma_handler->cm_id);

        node = &__corenet_rdma__->array[rdma_handler->node_loc];
        DINFO("corenet rdma close %d, node:%p\n", rdma_handler->node_loc, node);
        if (node->reset)
                node->reset(node->ctx);

        YASSERT(rdma_handler == &node->handler);

        __corenet_rdma_free_node(__corenet_rdma__, node);
}

void corenet_rdma_put(rdma_conn_t *rdma_handler)
{
        rdma_handler->ref--;
        if (unlikely(rdma_handler->ref == 0)) {
                __corenet_rdma_close(rdma_handler);
        }
}

void corenet_rdma_get(rdma_conn_t *rdma_handler, int n)
{
        rdma_handler->ref += n;
}

void corenet_rdma_close(rdma_conn_t *rdma_handler)
{
        corenet_node_t *node = container_of(rdma_handler, corenet_node_t, handler);

        if (node->sockid.sd == -1)
                return;

        if (rdma_handler->is_closing == 0) {
                DWARN("rdma sokcet closing %p\n", rdma_handler);
                rdma_disconnect(rdma_handler->cm_id);
                rdma_handler->is_closing = 1;

                mbuffer_free(&node->queue_buf);
                list_del_init(&node->send_list);

                mbuffer_free(&node->queue_buf);
                corerpc_rdma_reset(&node->sockid);
        }
}

void __iovs_post_recv_init(hyw_iovec_t *hyw_iovs, void *ptr)
{
        struct ibv_sge *sge;

        hyw_iovs->mode = RDMA_RECV_MSG;
        hyw_iovs->err = 0;

        sge = &hyw_iovs->sge;
        sge->addr = (uintptr_t)ptr;
        sge->length = 256;
        sge->lkey = hyw_iovs->rdma_handler->iov_mr->lkey;

        memset(&hyw_iovs->rr, 0, sizeof(struct ibv_recv_wr));
        hyw_iovs->rr.next = NULL;
        hyw_iovs->rr.wr_id = (uint64_t)hyw_iovs;
        hyw_iovs->rr.sg_list = sge;
        hyw_iovs->rr.num_sge = 1;
}

int corenet_rdma_post_recv(void *ptr)
{
        int ret;
        hyw_iovec_t *hyw_iovs = ptr;
        rdma_conn_t *rdma_handler = NULL;
        struct ibv_recv_wr *bad_wr;

        // check flag, 0 post recv, other close
        if (unlikely(hyw_iovs == NULL))
                return 0;

        rdma_handler = hyw_iovs->rdma_handler;

        if (rdma_handler && rdma_handler->is_closing == 0 && rdma_handler->qp) {

                if (hyw_iovs->mode == RDMA_READ)
                        __iovs_post_recv_init(hyw_iovs, ptr - 256);
                else
                        YASSERT(hyw_iovs->mode == RDMA_RECV_MSG);

                ret = ibv_post_recv(hyw_iovs->rdma_handler->qp, &hyw_iovs->rr, &bad_wr);
                if (ret)
                        GOTO(err_ret, ret);

                corenet_rdma_get(rdma_handler, 1);
        }

        return 0;
err_ret:
        return ret;
}

static struct ibv_send_wr *__rdma_post_send_prep(rdma_conn_t *rdma_handler, seg_t *seg)
{
        struct ibv_send_wr *sr = NULL;
        hyw_iovec_t *hyw_iov = NULL;

        YASSERT(seg->len <= 256);

        hyw_iov = (hyw_iovec_t *)(seg->handler.ptr + seg->len);

        mbuffer_init(&hyw_iov->msg_buf, 0);

        mbuffer_trans_sge(&hyw_iov->sge, seg->len, seg, &hyw_iov->msg_buf);
        hyw_iov->sge.lkey = rdma_handler->mr->lkey;

        hyw_iov->mode = RDMA_SEND_MSG;
        hyw_iov->rdma_handler = rdma_handler;
        sr = &hyw_iov->sr;

        memset(sr, 0x00, sizeof(struct ibv_send_wr));
        sr->next = NULL;
        sr->wr_id = (uint64_t)hyw_iov;
        sr->sg_list = &hyw_iov->sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_SEND;
        sr->send_flags = IBV_SEND_SIGNALED;

        return sr;
}

static struct ibv_send_wr *__rdma_post_read_prep(rdma_conn_t *rdma_handler, ynet_net_head_t *net_head, seg_t *seg)
{
        struct ibv_send_wr *sr = NULL;
        hyw_iovec_t *hyw_iov = NULL;

        YASSERT(net_head->blocks <= BUFFER_SEG_MAX);
        YASSERT(seg->len <= 256);

        hyw_iov = (hyw_iovec_t *)(seg->handler.ptr + 256);

        hyw_iov->mode = RDMA_READ;
        hyw_iov->n = 0;
        hyw_iov->rdma_handler = rdma_handler;
        mbuffer_init(&hyw_iov->msg_buf, 0);
        list_add_tail(&seg->hook, &hyw_iov->msg_buf.list);
        hyw_iov->msg_buf.len += seg->len;
        sr = &hyw_iov->sr;

        mbuffer_init(&hyw_iov->data_buf, net_head->blocks);
        //  DWARN("for bug test rdma read size %u msg size %u\n", net_head->blocks, seg->len);
        hyw_iov->sge.addr = (uintptr_t)mbuffer_head(&hyw_iov->data_buf);
        hyw_iov->sge.length = net_head->blocks;
        hyw_iov->sge.lkey = rdma_handler->mr->lkey;

        memset(sr, 0x00, sizeof(struct ibv_send_wr));
        sr->next = NULL;
        sr->wr_id = (uint64_t)hyw_iov;
        sr->sg_list = &hyw_iov->sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_RDMA_READ;
        sr->send_flags = IBV_SEND_SIGNALED;
        sr->wr.rdma.remote_addr = net_head->msgid.data_prop.remote_addr;
        sr->wr.rdma.rkey = net_head->msgid.data_prop.rkey;

        return sr;
}

static struct ibv_send_wr *__rdma_post_write_prep(rdma_conn_t *rdma_handler, ynet_net_head_t *net_head, seg_t *seg)
{
        struct ibv_send_wr *sr = NULL;
        hyw_iovec_t *hyw_iov = NULL;
        seg_t *_seg;

        YASSERT(net_head->blocks <= BUFFER_SEG_MAX);

        hyw_iov = (hyw_iovec_t *)(seg->handler.ptr + seg->len);

        hyw_iov->mode = RDMA_WRITE;
        hyw_iov->n = 0;
        hyw_iov->rdma_handler = rdma_handler;
        mbuffer_init(&hyw_iov->msg_buf, 0);
        /*mbuffer_merge(&hyw_iov->msg_buf, msg_buf); */
        list_add_tail(&seg->hook, &hyw_iov->msg_buf.list);
        hyw_iov->msg_buf.len += seg->len;
        sr = &hyw_iov->sr;

        mbuffer_init(&hyw_iov->data_buf, 0);

        _seg = (seg_t *)net_head->reply_buf.list.next;
        list_del(&_seg->hook);
        net_head->reply_buf.len -= _seg->len;
        YASSERT(net_head->reply_buf.len == 0);

        BUFFER_CHECK(&net_head->reply_buf);

        mbuffer_trans_sge(&hyw_iov->sge, _seg->len, _seg, &hyw_iov->data_buf);
        hyw_iov->sge.lkey = rdma_handler->mr->lkey;

        //struct ibv_mr *mr = rdma_get_mr();
        //YASSERT(mr->lkey == rdma_handler->mr->lkey);

        memset(sr, 0x00, sizeof(struct ibv_send_wr));
        sr->next = NULL;
        sr->wr_id = (uint64_t)hyw_iov;
        sr->sg_list = &hyw_iov->sge;
        sr->num_sge = 1;
        sr->opcode = IBV_WR_RDMA_WRITE;
        sr->send_flags = IBV_SEND_SIGNALED;
        sr->wr.rdma.remote_addr = net_head->msgid.data_prop.remote_addr;
        sr->wr.rdma.rkey = net_head->msgid.data_prop.rkey;

        return sr;
}

static int __corenet_rdma_post_send(rdma_conn_t *rdma_handler, corenet_node_t *node, int outstanding)
{
        int ret, max_req = 0;
        struct ibv_send_wr *sr = NULL, *last_sr = NULL, *bad_wr = NULL, *head_sr = NULL;
        ynet_net_head_t *net_head;
        seg_t *seg;
        struct list_head *pos, *n;
        (void)outstanding;
        //        max_req = MAX_REQ_NUM - outstanding;

        list_for_each_safe(pos, n, &node->queue_buf.list)
        {
                seg = (seg_t *)pos;
                list_del(&seg->hook);
                node->queue_buf.len -= seg->len;

                BUFFER_CHECK(&node->queue_buf);

                net_head = seg->handler.ptr;

                switch (net_head->type) {
                case YNET_MSG_REQ:
                case YNET_MSG_REP:
                        sr = __rdma_post_send_prep(rdma_handler, seg);
                        break;
                case YNET_MSG_RECV:
                        YASSERT(net_head->msgid.opcode == CORERPC_WRITE);
                        sr = __rdma_post_read_prep(rdma_handler, net_head, seg);
                        break;
                case YNET_DATA_REP:
                        YASSERT(net_head->msgid.opcode == CORERPC_READ);
                        sr = __rdma_post_write_prep(rdma_handler, net_head, seg);
                        break;
                default:
                        DERROR("bad opcode :%d\n", net_head->msgid.opcode);
                        YASSERT(0);
                }

                if (unlikely(sr == NULL))
                        break;

                if (last_sr)
                        last_sr->next = sr;
                else
                        head_sr = sr;

                last_sr = sr;
                sr->next = NULL;

                node->total_seg_count--;
                max_req++;
        }

        if (likely(last_sr) && rdma_running) {
                ret = ibv_post_send(rdma_handler->qp, head_sr, &bad_wr);
                if (unlikely(ret)) {
                        DERROR("ibv_post_send fail, QP_NUM:0x%x, bad_wr:%p, errno:%d, errmsg:%s, \n", rdma_handler->qp->qp_num, bad_wr, ret, strerror(ret));
                        YASSERT(0);
                }

                corenet_rdma_get(rdma_handler, max_req);
        }

        return 0;
}

static int __corenet_rdma_handle_wc(struct ibv_wc *wc, core_t *core)
{
        rdma_conn_t *rdma_handler;
        corenet_rdma_t *__corenet_rdma__ = core->rdma_net;
        corenet_node_t *node;
        hyw_iovec_t *iovs;
        int count = 0;

        iovs = (hyw_iovec_t *)wc->wr_id;
        rdma_handler = iovs->rdma_handler;
        node = &__corenet_rdma__->array[rdma_handler->node_loc];

        switch (iovs->mode) {
        case RDMA_RECV_MSG:
                count = wc->byte_len;
                node->exec(node->ctx, (void *)iovs, &count);
                break;
        case RDMA_SEND_MSG:
                mbuffer_free(&iovs->msg_buf);
                break;
        case RDMA_READ:
                node->exec1(node->ctx, &iovs->data_buf, &iovs->msg_buf);
                YASSERT(iovs->msg_buf.len == 0);
                YASSERT(iovs->data_buf.len == 0);
                //        mbuffer_free(&iovs->msg_buf);
                //        mbuffer_free(&iovs->data_buf);
                break;
        case RDMA_WRITE:
                mbuffer_free(&iovs->data_buf);
                mbuffer_free(&iovs->msg_buf);
                break;
        default:
                DERROR("bad mode:%d\n", iovs->mode);
                YASSERT(0);
        }

        corenet_rdma_put(rdma_handler);

        return 0;
}

static int __corenet_rdma_handle_wc_error(struct ibv_wc *wc, core_t *core)
{
        rdma_info_t *dev;
        hyw_iovec_t *iovs;
        rdma_conn_t *rdma_handler;
        //   void *ptr;

        dev = core->active_dev;
        iovs = (hyw_iovec_t *)wc->wr_id;
        rdma_handler = iovs->rdma_handler;

        switch (iovs->mode) {
        case RDMA_RECV_MSG:
                /*     ptr = ((void *)iovs) - 256;
                     mem_cache_free(MEM_CACHE_512, ptr);*/
                break;
        case RDMA_SEND_MSG:
                mbuffer_free(&iovs->msg_buf);
                break;
        case RDMA_READ:
                mbuffer_free(&iovs->msg_buf);
                mbuffer_free(&iovs->data_buf);
                /*                        ptr = ((void *)iovs) - 256;
                                        mem_cache_free(MEM_CACHE_512, ptr);*/
                break;
        case RDMA_WRITE:
                mbuffer_free(&iovs->data_buf);
                mbuffer_free(&iovs->msg_buf);
                break;
        default:
                DERROR("bad mode:%d\n", iovs->mode);
                YASSERT(0);
        }

        if (wc->status == IBV_WC_LOC_PROT_ERR) {
                YASSERT(0);
        } else if (wc->status != IBV_WC_WR_FLUSH_ERR){
                DWARN("poll error!!!!!! wc status:%s(%d), CQ:%p\n", ibv_wc_status_str(wc->status), (int)wc->status, dev->cq);
        }

        corenet_rdma_close(rdma_handler);
        corenet_rdma_put(rdma_handler);

        return 0;
}

#define MAX_POLLING 64

int corenet_rdma_poll(core_t *core)
{
        int ret = 0, i;
        struct ibv_wc wc;
        rdma_info_t *dev = core->active_dev;

        if (unlikely(dev == NULL || rdma_running == 0)) {
                return 0;
        }

        for (i = 0; i < MAX_POLLING; i++) {
                ret = ibv_poll_cq(dev->cq, 1, &wc);
                if (unlikely(ret == 0)) {
                        break;
                } else if (unlikely(ret < 0)) {
                        YASSERT(0);
                }

                if (likely(wc.status == IBV_WC_SUCCESS)) {
                        __corenet_rdma_handle_wc(&wc, core);
                } else {
                        __corenet_rdma_handle_wc_error(&wc, core);
                }
        }

        return 0;
}

static inline int __get_buffer_seg_count(buffer_t *buf)
{
        int count = 0;
        struct list_head *pos;

        list_for_each(pos, &buf->list) { count++; }

        return count;
}

static int __corenet_rdma_queue(corenet_node_t *node, buffer_t *src_buf, corenet_rdma_t *__corenet_rdma__)
{
        int src_seg_count = 0;

        src_seg_count = __get_buffer_seg_count(src_buf);
        YASSERT(src_seg_count == 1);

        if (likely(node->total_seg_count == 0)) {
                YASSERT(node->queue_buf.list.next == &node->queue_buf.list);
                list_add_tail(&node->send_list, &__corenet_rdma__->corenet.forward_list);
        }

        mbuffer_merge(&node->queue_buf, src_buf);

        node->total_seg_count++;

        return 0;
}

int corenet_rdma_send(const sockid_t *sockid, buffer_t *buf, int flag)
{
        int ret;
        corenet_node_t *node;
        // corenet_rdma_t *__corenet_rdma__ = get_static_type_addr(buf->private_mem, VARIABLE_CORENET_RDMA);
        corenet_rdma_t *__corenet_rdma__ = __corenet_get();

        (void)flag;
        YASSERT(sockid->type == SOCKID_CORENET);

        node = &__corenet_rdma__->array[sockid->sd];
        if (unlikely(node->handler.is_closing == 1 || node->sockid.seq != sockid->seq || node->sockid.sd == -1)) {

                ret = ECONNRESET;
                GOTO(err_lock, ret);
        }

        ret = __corenet_rdma_queue(node, buf, __corenet_rdma__);
        if (unlikely(ret))
                GOTO(err_lock, ret);

#if 1
        if (node->total_seg_count >= 4)
                corenet_rdma_commit();
#endif

        return 0;
err_lock:
        return ret;
}

static inline int __corenet_rdma_commit(corenet_node_t *node)
{
        rdma_conn_t *handler;
        handler = &node->handler;

        (void)__corenet_rdma_post_send(handler, node, 0);

        return 0;
}

void corenet_rdma_commit()
{
        struct list_head *pos, *n;
        corenet_rdma_t *__corenet_rdma__ = __corenet_get();
        ;
        corenet_node_t *node;

        list_for_each_safe(pos, n, &__corenet_rdma__->corenet.forward_list)
        {

                node = container_of(pos, corenet_node_t, send_list);
                YASSERT(node->closed == 0);
                __corenet_rdma_commit(node);

                if (likely(node->total_seg_count == 0)) {
                        list_del_init(&node->send_list);
                }
        }
}

int corenet_rdma_connected(const sockid_t *sockid)
{
        corenet_node_t *node;
        int ret;

        corenet_rdma_t *__corenet_rdma__ = __corenet_get();

        node = &__corenet_rdma__->array[sockid->sd];

        if (unlikely(node->sockid.seq != sockid->seq || node->sockid.sd == -1
        || !node->handler.is_connected || node->handler.is_closing == 1 || node->closed == 1)) {
                ret = ECONNRESET;
                // DWARN("for bug test the node sockid is %d sockid seq is %d\n",node->sockid.sd,sockid->seq);
                GOTO(err_lock, ret);
        }

        YASSERT(node->closed == 0);
        
        return 1;
err_lock:
        return 0;
}

int corenet_rdma_init(int max, corenet_rdma_t **_corenet, void *private_mem)
{
        int ret, len, i, size;
        corenet_rdma_t *corenet;
        corenet_node_t *node;

        YASSERT(private_mem);

        size = max;

        DINFO("rdma malloc %llu\n", (LLU)sizeof(corenet_node_t) * size);
        len = sizeof(corenet_rdma_t) + sizeof(corenet_node_t) * size;

        corenet = (corenet_rdma_t *)register_private_static_stor_area(private_mem, len, VARIABLE_CORENET_RDMA);
        DBUG("corenet use %u\n", len);

        _memset(corenet, 0x0, len);
        corenet->corenet.size = size;

        for (i = 0; i < size; i++) {
                node = &corenet->array[i];

                mbuffer_init(&node->queue_buf, 0);

                node->sockid.sd = -1;
                node->closed = 1;
                node->total_seg_count = 0;
                sy_spin_init(&node->lock);
                INIT_LIST_HEAD(&node->send_list);
        }

        INIT_LIST_HEAD(&corenet->corenet.forward_list);
        INIT_LIST_HEAD(&corenet->corenet.check_list);
        INIT_LIST_HEAD(&corenet->corenet.add_list);

        ret = sy_spin_init(&corenet->corenet.lock);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        variable_set(VARIABLE_CORENET_RDMA, corenet);
        //       core_register_tls(VARIABLE_CORENET_RDMA, (void *)corenet);

        if (_corenet)
                *_corenet = corenet;

        DBUG("corenet init done\n");

        return 0;

err_ret:
        return ret;
}

/*********************************************************/
int corenet_rdma_evt_channel_init()
{
        int ret;
        uint64_t size = sizeof(struct rdma_event_channel *) * cpuset_useable();

        ret = ymalloc((void **)&corenet_rdma_evt_channel, size);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}

struct rdma_event_channel *corenet_rdma_get_evt_channel(int cpu_idx) { return corenet_rdma_evt_channel[cpu_idx]; }

int corenet_rdma_create_channel(int cpu_idx)
{
        int ret;

        corenet_rdma_evt_channel[cpu_idx] = rdma_create_event_channel();
        if (!corenet_rdma_evt_channel[cpu_idx]) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int corenet_rdma_get_event(int cpu_idx, struct rdma_cm_event **ev)
{
        int ret;

        ret = rdma_get_cm_event(corenet_rdma_get_evt_channel(cpu_idx), ev);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int __rdma_create_cq(rdma_info_t **_dev, struct rdma_cm_id *cm_id, int ib_port, core_t *core)
{
        int cq_size = 0;
        int ret = 0;
        struct ibv_port_attr port_attr;
        rdma_info_t *dev;
        void *private_mem;
        uint64_t private_mem_size = 0;

        ret = ymalloc((void **)&dev, sizeof(rdma_info_t));
        if (ret) {
                ret = ENOMEM;
                GOTO(err_ret, ret);
        }

        dev->ibv_verbs = cm_id->verbs;
        /* query port properties */
        if (ibv_query_port(cm_id->verbs, ib_port, &port_attr)) {
                DERROR("ibv_query_port on port %u failed, errno:%d, errmsg:%s\n", ib_port, errno, strerror(errno));
                ret = errno;
                GOTO(err_free, ret);
        }

        /* query device properties */
        ret = ibv_query_device(dev->ibv_verbs, &dev->device_attr);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        cq_size = min_t(uint32_t, dev->device_attr.max_cqe, MAX_POLL_CQ_SIZE); /*a bug fix, 512k may be too large*/

        DINFO("max %d CQEs\n", cq_size);

        /* each side will send only one WR, so Completion
         * Queue with 1 entry is enough
         */
        dev->cq = ibv_create_cq(cm_id->verbs, cq_size, NULL, NULL, 0);
        if (!dev->cq) {
                DERROR("failed to create CQ with %u entries, errno:%d, errmsg:%s\n", cq_size, errno, strerror(errno));
                ret = errno;
                GOTO(err_free, ret);
        }

        // TODO by core?
        get_global_private_mem(&private_mem, &private_mem_size);
        dev->mr = (struct ibv_mr *)rdma_register_mgr(cm_id->pd, private_mem, private_mem_size);
        if (dev->mr == NULL)
                YASSERT(0);

        //gmr = dev->mr;
        dev->pd = cm_id->pd;
        DINFO("CQ was created OK, cq:%p, mr %p\n", dev->cq, dev->mr);

        *_dev = dev;

        list_add_tail(&dev->list, &core->rdma_dev_list);

        return 0;
err_free:
        yfree((void **)&dev);
err_ret:
        return ret;
}

/*
struct ibv_mr *rdma_get_mr()
{ 
        return gmr;
}*/

void *rdma_get_mr_addr()
{
        core_t *core = core_self();

        if (core)
                return core->tls[VARIABLE_CORE];
        else
                return NULL;
}

void *rdma_register_mgr(void *pd, void *buf, size_t size)
{
        int mr_flags = 0;
        struct ibv_mr *mr = NULL;

        /* register the memory buffer */
        mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
        // mem_hugepage_init here?
        mr = ibv_reg_mr((struct ibv_pd *)pd, buf, size, mr_flags);
        if (!mr) {
                DERROR("ibv_reg_mr failed with mr_flags=0x%x, errno:%d, errmsg:%s %p\n", mr_flags, errno, strerror(errno), buf);
                goto err_ret;
        }

        return mr;
err_ret:
        return NULL;
}

/*
struct rdma_cm_id {
        struct ibv_context	*verbs;
        struct rdma_event_channel *channel;
        void			*context;
        struct ibv_qp		*qp;
        struct rdma_route	 route;
        enum rdma_port_space	 ps;
        uint8_t			 port_num;
        struct rdma_cm_event	*event;
        struct ibv_comp_channel *send_cq_channel;
        struct ibv_cq		*send_cq;
        struct ibv_comp_channel *recv_cq_channel;
        struct ibv_cq		*recv_cq;
        struct ibv_srq		*srq;
        struct ibv_pd		*pd;
        enum ibv_qp_type	qp_type;
};
*/

static int __rdma_dev_find(rdma_info_t **_dev, core_t *core, struct rdma_cm_id *cm_id)
{
        rdma_info_t *dev;

        list_for_each_entry(dev, &core->rdma_dev_list, list) {
                if (dev->ibv_verbs == cm_id->verbs) {
                        *_dev = dev;
                        return 1;
                        break;
                }
        }

        return 0;
}

static int __corenet_rdma_create_qp__(struct rdma_cm_id *cm_id, core_t *core, rdma_conn_t *handler)
{
        int ret;
        struct ibv_qp_init_attr qp_init_attr;
        rdma_info_t *dev;

        if (!__rdma_dev_find(&dev, core, cm_id)) {
                ret = __rdma_create_cq(&dev, cm_id, 1, core);
                if (ret) {
                        DERROR("rdma create cq fail, errno:%d, errmsg:%s\n", errno, strerror(errno));
                        GOTO(err_ret, ret);
                }
        }

        core->active_dev = dev;

        YASSERT(dev->pd == cm_id->pd);

        handler->mr = dev->mr;
        /* create qp */
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        /* both send and recv to the same CQ */
        qp_init_attr.send_cq = dev->cq;
        qp_init_attr.recv_cq = dev->cq;
        qp_init_attr.cap.max_send_wr = 1024;
        qp_init_attr.cap.max_recv_wr = 1024;
        qp_init_attr.cap.max_send_sge = MAX_SEG_COUNT; /* scatter/gather entries */
        qp_init_attr.cap.max_recv_sge = MAX_SEG_COUNT;
        qp_init_attr.qp_type = IBV_QPT_RC;
        /* only generate completion queue entries if requested */
        qp_init_attr.sq_sig_all = 0;

        ret = rdma_create_qp(cm_id, cm_id->pd, &qp_init_attr);
        if (ret) {
                ret = errno;
                DERROR("rdma_create_qp fail, cm_id:%p, errno:%d\n", cm_id, ret);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __corenet_rdma_post_mem_handler(rdma_conn_t *handler, core_t *core)
{
        int ret, i;
        hyw_iovec_t *hyw_iovs;
        struct ibv_recv_wr *bad_wr;
        void *ptr, *tmp;

        (void)core;

        ret = ymalloc(&tmp, 512 * DEFAULT_MH_NUM);
        if (ret) {
                YASSERT(0);
        }

        handler->iov_addr = tmp;

        handler->iov_mr = (struct ibv_mr *)rdma_register_mgr(handler->pd, tmp, 512 * DEFAULT_MH_NUM);
        if (handler->iov_mr == NULL)
                YASSERT(0);

        for (i = 0; i < DEFAULT_MH_NUM; i++) {

                static_assert(sizeof(hyw_iovec_t) <= 256, "hyw_iovec_t");

                ptr = tmp + i * 512;

                hyw_iovs = (hyw_iovec_t *)(ptr + 256);
                hyw_iovs->rdma_handler = handler;

                __iovs_post_recv_init(hyw_iovs, ptr);

                ret = ibv_post_recv(handler->qp, &hyw_iovs->rr, &bad_wr);
                if (ret)
                        YASSERT(0);
        }

        handler->is_connected = 1;
        corenet_rdma_get(handler, DEFAULT_MH_NUM);// multiply 2, means both mem_handler and post recv ref

        return 0;
}

static int __corenet_rdma_create_qp_real(core_t *core, struct rdma_cm_id *cm_id, rdma_conn_t *rdma_handler)
{
        int ret;

        ret = __corenet_rdma_create_qp__(cm_id, core, rdma_handler);
        if (ret)
                GOTO(err_ret, ret);

        rdma_handler->qp = cm_id->qp;
        rdma_handler->pd = cm_id->pd;
        rdma_handler->cm_id = cm_id;
        ret = __corenet_rdma_post_mem_handler(rdma_handler, core);
        if (ret)
                GOTO(err_free, ret);

        return 0;
err_free:
        rdma_destroy_qp(cm_id);
err_ret:
        return ret;
}

static int __corenet_rdma_create_qp_coroutine(va_list ap)
{
        rdma_conn_t *rdma_handler = va_arg(ap, rdma_conn_t *);
        struct rdma_cm_id *cm_id = va_arg(ap, struct rdma_cm_id *);
        core_t *core = va_arg(ap, core_t *);

        va_end(ap);

        YASSERT(rdma_handler->ref <= 1);
        if (rdma_handler->ref > 1) {
                DWARN("the rdma_handler ref %d\n", rdma_handler->ref);
        }

        return __corenet_rdma_create_qp_real(core, cm_id, rdma_handler);
}

static int __corenet_rdma_create_qp(core_t *core, struct rdma_cm_id *cm_id, rdma_conn_t *rdma_handler)
{
        int ret;

        if (core_self()) {
                return __corenet_rdma_create_qp_real(core, cm_id, rdma_handler);
        } else {

        retry:
                ret = core_request(core->hash, -1, "rdma_create_qp", __corenet_rdma_create_qp_coroutine, rdma_handler, cm_id, core);
                if (ret) {
                        if (ret == ENOSPC) {
                                DWARN("core request queue is full, sleep 5ms will retry\n");
                                usleep(5000);
                                goto retry;
                        } else {
                                UNIMPLEMENTED(__DUMP__);
                        }
                }
        }
        return 0;
}

static int __corenet_rdma_add_coroutine(va_list ap)
{
        core_t *core = va_arg(ap, core_t *);
        sockid_t *sockid = va_arg(ap, sockid_t *);
        void *ctx = va_arg(ap, void *);
        core_exec exec = va_arg(ap, core_exec);
        core_exec1 exec1 = va_arg(ap, core_exec1);
        func_t reset = va_arg(ap, func_t);
        func_t check = va_arg(ap, func_t);
        func_t recv = va_arg(ap, func_t);
        rdma_conn_t **handler = va_arg(ap, rdma_conn_t **);

        va_end(ap);

        return corenet_rdma_add(core, sockid, ctx, exec, exec1, reset, check, recv, handler);
}

static int __corenet_rdma_add(core_t *core, sockid_t *sockid, void *ctx, core_exec exec, core_exec1 exec1, func_t reset, func_t check, func_t recv,
                     rdma_conn_t **handler)
{
        int ret;

        if (core_self()) {
                return corenet_rdma_add(core, sockid, ctx, exec, exec1, reset, check, recv, handler);
        } else {
                ret = core_request(core->hash, -1, "rdma_add", __corenet_rdma_add_coroutine,
                                core, sockid, ctx, exec, exec1, reset, check, recv, handler);
                if (unlikely(ret)) {
                        GOTO(err_ret, ret);
                }
        }

        return 0;
err_ret:
        return ret;
}

#define RESOLVE_TIMEOUT 500

static int __corenet_rdma_resolve_addr(struct rdma_cm_id *cm_id, const char *host, const char *port, sockid_t *sockid)
{
        struct addrinfo *addr;
        int ret;

        ret = getaddrinfo(host, port, NULL, &addr);
        if (ret) {
                ret = errno;
                DERROR("get host %s:%s  addr info error, errno:%d\n", host, port, ret);
                GOTO(err_ret, ret);
        }

        sockid->addr = ((struct sockaddr_in *)addr->ai_addr)->sin_addr.s_addr;

        DINFO("connect to server:%s\n", inet_ntoa(((struct sockaddr_in *)(addr->ai_addr))->sin_addr));
        ret = rdma_resolve_addr(cm_id, NULL, addr->ai_addr, RESOLVE_TIMEOUT);
        if (ret) {
                ret = errno;
                DERROR("rdma_resolve_addr host %s:%s error, errno:%d\n", host, port, ret);
                GOTO(err_free, ret);
        }

        freeaddrinfo(addr);

        return 0;
err_free:
        freeaddrinfo(addr);
err_ret:
        return ret;
}

static int __corenet_rdma_disconn(va_list ap)
{
        rdma_conn_t *rdma_handler = va_arg(ap, rdma_conn_t *);
        va_end(ap);

        if (rdma_handler == NULL || rdma_handler->is_closing == 1)
                return 0;

        corenet_rdma_close(rdma_handler);

        return 0;
}

void corenet_rdma_disconnected(struct rdma_cm_event *ev, void *_core)
{
        int ret;
        struct rdma_cm_id *cm_id = ev->id;
        rdma_conn_t *rdma_handler = cm_id->context;
        core_t *core = _core;

        if (rdma_handler == NULL)
                return;

retry:
        ret = core_request(core->hash, -1, "rdma_disconnected", __corenet_rdma_disconn, rdma_handler);
        if (ret) {
                if (ret == ENOSPC) {
                        DWARN("core request queue is full, sleep 5ms will retry\n");
                        usleep(5000);
                        goto retry;
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }
}

static int __corenet_rdma_tw_exit(va_list ap)
{
        rdma_conn_t *rdma_handler = va_arg(ap, rdma_conn_t *);

        YASSERT(rdma_handler);
        DWARN("corenet rdma  closed\n");
        corenet_rdma_put(rdma_handler);

        return 0;
}

void corenet_rdma_timewait_exit(struct rdma_cm_event *ev, void *_core)
{
        int ret;
        struct rdma_cm_id *cm_id = ev->id;
        rdma_conn_t *rdma_handler = cm_id->context;
        core_t *core = _core;

retry:
        ret = core_request(core->hash, -1, "rdma_timewait", __corenet_rdma_tw_exit, rdma_handler);
        if (ret) {
                if (ret == ENOSPC) {
                        DWARN("core request queue is full, sleep 5ms will retry\n");
                        usleep(5000);
                        goto retry;
                } else {
                        UNIMPLEMENTED(__DUMP__);
                }
        }
}

static int __corenet_rdma_resolve_route(struct rdma_cm_id *cm_id, core_t *core, sockid_t *sockid)
{
        int ret;
        rdma_conn_t *rdma_handler;
        corerpc_ctx_t *ctx;
        DINFO("cm_id:%p addr resolved.\n", cm_id);

        // create rdma_handler, qp, post recv

        sockid->outst_req = 0;
        sockid->seq = _random();
        sockid->type = SOCKID_CORENET;

        static_assert(sizeof(corerpc_ctx_t) < sizeof(mem_cache64_t), "corerpc_ctx_t");
        ret = ymalloc((void **)&ctx, sizeof(corerpc_ctx_t));
        ctx->running = 0;
        ret = __corenet_rdma_add(core, sockid, ctx, corerpc_rdma_recv_msg, corerpc_rdma_recv_data, corerpc_close, NULL, NULL, &rdma_handler);
        if (ret)
                UNIMPLEMENTED(__DUMP__);

        ctx->sockid = *sockid;
        cm_id->context = rdma_handler;
        rdma_handler->private_mem = core->tls[VARIABLE_CORE];

        ret = __corenet_rdma_create_qp(core, cm_id, rdma_handler);
        if (ret)
                GOTO(err_free, ret);

        ret = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT);
        if (ret) {
                ret = errno;
                DERROR("rdma_resolve_route cm_id:%p error, errno:%d\n", cm_id, ret);
                GOTO(err_free_qp, ret);
        }

        return 0;

err_free_qp:
        rdma_destroy_qp(cm_id);
err_free:
        yfree((void **)&ctx);
        __corenet_rdma_free_node1(core, sockid);
        return ret;
}

static int __corenet_rdma_connect(struct rdma_cm_id *cm_id)
{
        int ret;
        struct rdma_conn_param cm_params;

        memset(&cm_params, 0, sizeof(cm_params));

        cm_params.responder_resources = 16;
        cm_params.initiator_depth = 16;
        cm_params.retry_count = 5;

        DINFO("cm_id:%p route resolved.\n", cm_id);

        ret = rdma_connect(cm_id, &cm_params);
        if (ret) {
                ret = errno;
                DERROR("rdma_connect cm_id:%p error, errno:%d\n", cm_id, ret);
                GOTO(err_ret, ret);
        }

        DINFO("cm_id:%p connect successful.\n", cm_id);

        return 0;
err_ret:
        return ret;
}

static int __corenet_rdma_on_active_event(struct rdma_event_channel *evt_channel, core_t *core, sockid_t *sockid)
{
        struct rdma_cm_event *ev = NULL;
        enum rdma_cm_event_type ev_type;
        int ret;

        DINFO("rdma_get_cm_event\r\n");
        /**
         * rdma_get_cm_event will blocked, so cannot exec in core/task.
         */
        while (1) {
                struct pollfd ev_pollfd;
                int ms_timeout = 20 * 1000;
 
                /*
                * poll the channel until it has an event and sleep ms_timeout
                * milliseconds between any iteration
                */
                ev_pollfd.fd      = evt_channel->fd;
                ev_pollfd.events  = POLLIN;
                ev_pollfd.revents = 0;

                ret = poll(&ev_pollfd, 1, ms_timeout);
                if (ret < 0) {
                        DERROR("rdma_get_cm_event poll failed, err:%d\r\n", ret);
                        return -errno;
                }
                else if(ret == 0) {
                        DERROR("rdma_get_cm_event timeout, err:%d\r\n", ret);
                        return errno?-errno:-1;
                }

                ret = rdma_get_cm_event(evt_channel, &ev);
                if(ret) {
                        DERROR("rdma_get_cm_event failed, err:%d\r\n", -errno);
                        return -errno;
                }

                ev_type = ev->event;

                switch (ev_type) {
                case RDMA_CM_EVENT_ADDR_RESOLVED:
                        ret = __corenet_rdma_resolve_route(ev->id, core, sockid);
                        if (ret)
                                GOTO(err_ret, ret);
                        break;
                case RDMA_CM_EVENT_ROUTE_RESOLVED:
                        ret = __corenet_rdma_connect(ev->id);
                        if (ret)
                                GOTO(err_ret, ret);
                        break;
                case RDMA_CM_EVENT_ESTABLISHED:
                        DINFO("connection established on active side. channel:%p\n", evt_channel);
                        goto out;
                case RDMA_CM_EVENT_ADDR_ERROR:
                case RDMA_CM_EVENT_ROUTE_ERROR:
                case RDMA_CM_EVENT_UNREACHABLE:
                case RDMA_CM_EVENT_CONNECT_ERROR:
                        ret = ECONNREFUSED;
                        GOTO(err_ret, ret);
                default:
                        DERROR("Illegal event:%d - ignored\n", ev_type);
                        ret = ECONNREFUSED;
                        GOTO(err_ret, ret);
                }

                rdma_ack_cm_event(ev);
        }
out:
        DINFO("rdma_get_cm_event finish\r\n");
        return 0;
err_ret:
        DERROR("rdma_get_cm_event failed, err: %d\r\n", ret);
        return ret;
}

#if CORENET_RDMA_ON_ACTIVE_WAIT
static void *__corenet_rdma_on_active_wait__(void *arg)
{
        struct rdma_cm_event *ev = NULL;
        enum rdma_cm_event_type ev_type;
        struct rdma_event_channel *evt_channel = arg;
        rdma_conn_t *rdma_handler = NULL;

        while (rdma_get_cm_event(evt_channel, &ev) == 0) {
                rdma_handler = ev->id->context;
                ev_type = ev->event;

                switch (ev_type) {
                case RDMA_CM_EVENT_CONNECT_ERROR:
                case RDMA_CM_EVENT_ADDR_CHANGE:
                case RDMA_CM_EVENT_DISCONNECTED:
                        DWARN("disconnect on active side. channel:%p, event:%s\n", evt_channel, rdma_event_str(ev_type));
                        corenet_rdma_disconnected(ev, rdma_handler->core);
                        break;
                case RDMA_CM_EVENT_TIMEWAIT_EXIT:
                        DWARN("disconnect on active side. channel:%p, event:%s\n", evt_channel, rdma_event_str(ev_type));
                        corenet_rdma_timewait_exit(ev, rdma_handler->core);
                        rdma_ack_cm_event(ev);
                        goto out;
                default:
                        DERROR("Illegal event:%d - ignored\n", ev_type);
                        break;
                }

                rdma_ack_cm_event(ev);
        }
out:
        return NULL;
}

static int __corenet_rdma_on_active_wait(struct rdma_event_channel *evt_channel)
{
        int ret;

        pthread_t th;
        pthread_attr_t ta;

        (void)pthread_attr_init(&ta);
        (void)pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        ret = pthread_create(&th, &ta, __corenet_rdma_on_active_wait__, evt_channel);
        if (ret)
                GOTO(err_ret, ret);

        return 0;
err_ret:
        return ret;
}
#endif /*CORENET_RDMA_ON_ACTIVE_WAIT*/

int corenet_rdma_connect_by_channel(const char *host, const char *port, core_t *core, sockid_t *sockid)
{
        int ret = 0;
        int flags;
        struct rdma_cm_id *cma_conn_id;
        struct rdma_event_channel *evt_channel;
        
        ANALYSIS_BEGIN(0);
        evt_channel = rdma_create_event_channel();
        if (!evt_channel) {
                DERROR("rdma create channel fail, errno:%d\n", errno);
                GOTO(err_ret, ret);
        }

        ret = rdma_create_id(evt_channel, &cma_conn_id, NULL, RDMA_PS_TCP);
        if (ret) {
                DERROR("rdma_create_id failed, %m\n");
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = __corenet_rdma_resolve_addr(cma_conn_id, host, port, sockid);
        if (ret)
                GOTO(err_ret, ret);

        /* change the blocking mode of the completion channel */
        flags = fcntl(evt_channel->fd, F_GETFL);
        ret = fcntl(evt_channel->fd, F_SETFL, flags | O_NONBLOCK);
        if (ret < 0) {
                DERROR("failed to change file descriptor of Completion Event Channel\n");
                GOTO(err_ret, ret);
        }

        ret = __corenet_rdma_on_active_event(evt_channel, core, sockid);
        if (ret)
                GOTO(err_ret, ret);

#if CORENET_RDMA_ON_ACTIVE_WAIT
        // create a new thread to wait the channel disconnect
        ret = __corenet_rdma_on_active_wait(evt_channel);
        if (ret)
                GOTO(err_ret, ret);
#else
        ret = rdma_event_add(evt_channel->fd, RDMA_CLIENT_EV_FD, EPOLLIN, rdma_handle_event, NULL, core);
        if (ret)
                GOTO(err_ret, ret);
#endif /*CORENET_RDMA_ON_ACTIVE_WAIT*/

        ANALYSIS_END(0, 1000 * 1000 * 5, NULL);
        return 0;

/*err_id:
        rdma_destroy_id(cma_conn_id); */
err_ret:
        ANALYSIS_END(0, 1000 * 1000 * 5, NULL);
        return ret;
}

static int __corenet_rdma_bind_addr(struct rdma_cm_id *cm_id, int cpu_idx)
{
        int listen_port = 0, ret = 0;
        struct sockaddr_in sock_addr;

        listen_port = gloconf.rdma_base_port + cpu_idx;
        DINFO("listen port:%d, cpu_idx:%d\n", listen_port, cpu_idx);

        memset(&sock_addr, 0, sizeof(sock_addr));
        sock_addr.sin_family = AF_INET;
        sock_addr.sin_port = htons(listen_port);
        sock_addr.sin_addr.s_addr = INADDR_ANY;

        ret = rdma_bind_addr(cm_id, (struct sockaddr *)&sock_addr);
        if (ret) {
                ret = errno;
                DERROR("rdma_bind_addr: %s fail. errno:%d\n", strerror(ret), ret);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

int corenet_rdma_listen_by_channel(int cpu_idx)
{
        int afonly = 1, ret;
        struct rdma_cm_id *cma_listen_id;
        core_t *core = core_get(cpu_idx);
        struct rdma_event_channel *evt_channel = NULL;

        ret = corenet_rdma_create_channel(cpu_idx);
        if (ret) {
                DERROR("rdma create channel fail, errno:%d\n", ret);
                GOTO(err_ret, ret);
        }

        evt_channel = corenet_rdma_get_evt_channel(cpu_idx);
        ret = rdma_create_id(evt_channel, &cma_listen_id, NULL, RDMA_PS_TCP);
        if (ret) {
                DERROR("rdma_create_id failed, %m\n");
                GOTO(err_ret, ret);
        }

        rdma_set_option(cma_listen_id, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY, &afonly, sizeof(afonly));

        ret = __corenet_rdma_bind_addr(cma_listen_id, cpu_idx);
        if (ret) {
                DERROR("rdma bind addr fail : %s\n", strerror(ret));
                GOTO(err_ret, ret);
        }

        /* 0 == maximum backlog */
        ret = rdma_listen(cma_listen_id, 0);
        if (ret) {
                DERROR("rdma_listen fail : %s\n", strerror(ret));
                GOTO(err_ret, ret);
        }

#if CORENET_RDMA_ON_ACTIVE_WAIT
        (void)core;
#else
        ret = rdma_event_add(evt_channel->fd, RDMA_SERVER_EV_FD, EPOLLIN, rdma_handle_event, NULL, core);
        if (unlikely(ret))
                GOTO(err_ret, ret);
#endif /*CORENET_RDMA_ON_ACTIVE_WAIT*/

        return 0;
/*err_id:
        rdma_destroy_id(cma_listen_id); */
err_ret:
        return ret;
}

void corenet_rdma_established(struct rdma_cm_event *ev, void *_core)
{
        int ret;
        nid_t from;
        core_t *core = _core;
        sockid_t *sockid;
        corenet_node_t *node;
        corenet_rdma_t *corenet;
        struct rdma_cm_id *cm_id = ev->id;
        char peer_addr[MAX_NAME_LEN] = "";
        struct sockaddr *addr;
        rdma_conn_t *handler = ev->id->context;

        addr = rdma_get_peer_addr(cm_id);
        if (addr == NULL) {
                DERROR("get peer addr fail, maybe disconnect\n");
                return;
        }

        strcpy(peer_addr, inet_ntoa(((struct sockaddr_in *)addr)->sin_addr));
        ret = maping_addr2nid(peer_addr, &from);
        if (unlikely(ret)) {
                DERROR("hostname %s trans to nid failret (%u) %s\n", peer_addr, ret, strerror(ret));
                YASSERT(0);
        }

        corenet = core->rdma_net ? core->rdma_net : __corenet_get();
        node = &corenet->array[handler->node_loc];
        sockid = &node->sockid;
        sockid->addr = ((struct sockaddr_in *)addr)->sin_addr.s_addr;

        ret = corenet_maping_accept(core, &from, sockid);
        if (ret) {
                UNIMPLEMENTED(__DUMP__);
        }

        return;
}

void corenet_rdma_connect_request(struct rdma_cm_event *ev, void *_core)
{
        int ret;
        struct rdma_cm_id *cm_id = ev->id;
        core_t *core = _core;
        rdma_conn_t *rdma_handler;
        sockid_t *sockid;
        corerpc_ctx_t *ctx;

        struct rdma_conn_param conn_param = {
                .responder_resources = 16,
                .initiator_depth = 16,
                .retry_count = 5,
        };

        static_assert(sizeof(corerpc_ctx_t) < 64, "corerpc_ctx_t");

        ret = ymalloc((void **)&ctx, sizeof(corerpc_ctx_t));
        if (unlikely(ret))
                YASSERT(0);

        ctx->running = 0;
        ctx->sockid.addr = ((struct sockaddr_in *)(&ev->id->route.addr.dst_addr))->sin_addr.s_addr;
        ctx->sockid.outst_req = 0;
        ctx->sockid.seq = _random();
        ctx->sockid.type = SOCKID_CORENET;
        sockid = &ctx->sockid;

        ret = __corenet_rdma_add(core, sockid, ctx, corerpc_rdma_recv_msg, corerpc_rdma_recv_data, corerpc_close, NULL, NULL, &rdma_handler);
        if (unlikely(ret))
                UNIMPLEMENTED(__DUMP__);

        cm_id->context = (void *)rdma_handler;
        ctx->sockid.rdma_handler = (uintptr_t)rdma_handler;
        rdma_handler->private_mem = core->tls[VARIABLE_CORE];

        ret = __corenet_rdma_create_qp(core, cm_id, rdma_handler);
        if (unlikely(ret))
                GOTO(err_ret, ret);

        /* now we can actually accept the connection */
        DINFO("corenet rdma accept begin, cm_id:%p\n", cm_id);

        ret = rdma_accept(cm_id, &conn_param);
        if (unlikely(ret)) {
                DERROR("rdma_accept failed, cm_id:%p\n", cm_id);
                GOTO(err_ret, ret);
        }

        DINFO("corenet rdma accept end, cm_id:%p\n", cm_id);

        return;
err_ret:
        yfree((void **)&ctx);
        ret = rdma_reject(cm_id, NULL, 0);
        if (unlikely(ret))
                DERROR("cm_id:%p rdma_reject failed, %m\n", cm_id);
}

#if CORENET_RDMA_ON_ACTIVE_WAIT
int corenet_rdma_on_passive_event(int cpu_idx)
{
        struct rdma_cm_event *ev = NULL;
        enum rdma_cm_event_type ev_type;
        core_t *core = core_get(cpu_idx);
        ;

        while (rdma_get_cm_event(corenet_rdma_get_evt_channel(cpu_idx), &ev) == 0) {
                ev_type = ev->event;
                switch (ev_type) {
                case RDMA_CM_EVENT_CONNECT_REQUEST:
                        corenet_rdma_connect_request(ev, core);
                        break;

                case RDMA_CM_EVENT_ESTABLISHED:
                        DINFO("corenet rdma  connection established on passive side. channel:%p\n", corenet_rdma_get_evt_channel(cpu_idx));
                        corenet_rdma_established(ev, core);
                        break;

                case RDMA_CM_EVENT_REJECTED:
                case RDMA_CM_EVENT_ADDR_CHANGE:
                case RDMA_CM_EVENT_DISCONNECTED:
                        DWARN("disconnect on passive side. channel:%p, event:%s\n", corenet_rdma_get_evt_channel(cpu_idx), rdma_event_str(ev_type));
                        corenet_rdma_disconnected(ev, core);
                        break;

                case RDMA_CM_EVENT_TIMEWAIT_EXIT:
                        corenet_rdma_timewait_exit(ev, core);
                        break;
                default:
                        DERROR("Illegal event:%d - ignored\n", ev_type);
                        break;
                }

                rdma_ack_cm_event(ev);
        }

        return 0;
}
#endif
