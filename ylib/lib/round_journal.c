

#include <sys/mman.h>
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#define DBG_SUBSYS S_YFSCDS

#include "job_tracker.h"
#include "configure.h"
#include "job_dock.h"
#include "ylib.h"
#include "round_journal.h"
#include "dbg.h"

#define RJNL_MAX_REQ (1024 * 4)
#define RJNL_MAX_QUEUE (RJNL_MAX_REQ * 10)
#define RJNL_COMMIT (64 * 4)
#define RJNL_TMO (60 * 2)

#define RJNL_ALIGN 4096

typedef rjnl_head_t head_t;
typedef rjnl_iocb_t iocb_t;

#define BIG_LEN (1024 * 1024 * 4)

#if 0
static int __submit_mini(rjnl_t *jnl, iocb_t *queue, int commit, char *buf)
{
        int ret, i, offset, fd, done = 0;
        struct iocb *iocb;
        job_t *job;

        (void) jnl;

        fd = queue[0].iocb.aio_fildes;

        offset = 0;
        for (i = 0; i < commit; i++) {
                iocb = &queue[i].iocb;

                if (fd != iocb->aio_fildes)
                        break;

                if (offset + iocb->u.c.nbytes > BIG_LEN)
                        break;

                if (i > RJNL_COMMIT)
                        break;

                memcpy(buf + offset, iocb->u.c.buf, iocb->u.c.nbytes);
                offset += iocb->u.c.nbytes;
        }

        done = i;

        DBUG("write size %llu offset %llu count %u, total %u\n", (LLU)offset,
              (LLU)queue[0].iocb.u.c.offset, done, commit);

        YASSERT(done);

        YASSERT(queue[0].iocb.u.c.offset + offset <= jnl->file.max_size);
        ret = _pwrite(fd, buf, offset, queue[0].iocb.u.c.offset);
        if (ret < 0) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        for (i = 0; i < done; i++) {
                job = queue[i].iocb.data;

                if (job) {
                        job_timermark(job, "rjnl_done");
                        //YASSERT(job->request.len);
                        job_resume(job, 0);
                }
        }

        return done;
err_ret:
        return -ret;
}
#endif

static int __submit_big(iocb_t *queue, int count, struct iocb **ioarray,
                        struct io_event *events, rjnl_t *jnl)
{
        int ret, left, offset, commit;
        long r, j, i;
        struct timespec tmo;
        struct io_event *ev;
        time_t t;
        job_t *job;
        head_t *head;
        
        for (i = 0; i < count; i++) {
                ioarray[i] = &queue[i].iocb;

                head = queue[i].iocb.u.c.buf;

                DBUG("magic  %x offset %llu size %llu\n", head->magic, (LLU)queue[i].iocb.u.c.offset,
                      (LLU)queue[i].iocb.u.c.nbytes);
        }

        left = count;
        offset = 0;
        while (left) {
                commit = left < RJNL_MAX_REQ ? left : RJNL_MAX_REQ;
                ret = io_submit(jnl->queue.ctx, commit, &ioarray[offset]);
                if (ret != commit) {
                        ret = -ret;
                        DERROR("ret %u\n", ret);
                        EXIT(ret);
                }

                left -= commit;
                offset += commit;

                t = time(NULL);
                if (t == -1) {
                        ret = errno;
                        YASSERT(0);
                }

                tmo.tv_sec = t + 60;
                tmo.tv_nsec = 0;

                while (commit > 0) {
                        r = io_getevents(jnl->queue.ctx, 1, commit,
                                         events, &tmo);
                        if (r > 0) {
                                for (j = 0; j < r; j++) {
                                        ev = &events[j];
                                        job = ev->data;

                                        DBUG("res %llu %llu\n", (LLU)ev->res,
                                             (LLU)ev->res2);

                                        if (job) {
                                                YASSERT(job);
                                                job_timermark(job, "rjnl_done");
                                                //YASSERT(job->request.len);
                                                job_resume(job, 0);
                                        }
                                }

                                i += r;
                                commit -= r;
                        } else {
                                if (srv_running)
                                        YASSERT(0);
                                else
                                        goto out;
                        }
                }
        }

out:
        return 0;
}

static void *__worker(void *_args)
{
        int ret, count, i;
        struct iocb **ioarray;
        iocb_t *queue, *queue1, *iocb;
        rjnl_t *jnl;
        struct io_event *events;

        jnl =  _args;

        ret = ymalloc((void **)&events, sizeof(struct io_event) * (RJNL_MAX_QUEUE));
        if (ret)
                YASSERT(0);

        ret = ymalloc((void **)&ioarray, sizeof(struct iocb *) * (RJNL_MAX_QUEUE));
        if (ret)
                YASSERT(0);

        ret = ymalloc((void **)&jnl->queue.queue, sizeof(iocb_t) * (RJNL_MAX_QUEUE + 1));
        if (ret)
                YASSERT(0);

        ret = ymalloc((void **)&queue, sizeof(iocb_t) * (RJNL_MAX_QUEUE + 1));
        if (ret)
                YASSERT(0);

        while (1) {
                ret = sy_spin_lock(&jnl->lock);
                if (ret)
                        UNIMPLEMENTED(__DUMP__);

                if (jnl->queue.count == 0) {
                retry:
                        sy_spin_unlock(&jnl->lock);

                        ret = _sem_wait(&jnl->queue.sem);
                        if (ret) {
                                YASSERT(0);
                        }

                        ret = sy_spin_lock(&jnl->lock);
                        if (ret)
                                UNIMPLEMENTED(__DUMP__);

                        if (jnl->queue.count == 0) {
                                goto retry;
                        }
                }

                queue1 = jnl->queue.queue;
                jnl->queue.queue = queue;
                queue = queue1;
                count = jnl->queue.count;
                jnl->queue.count = 0;

                sy_spin_unlock(&jnl->lock);

                DBUG("got count %u\n", count);

                ret = __submit_big(queue, count, ioarray, events, jnl);
                if (ret) {
                        if (srv_running == 0)
                                return NULL;
                        else
                                UNIMPLEMENTED(__DUMP__);
                }

                for (i = 0; i < count; i++) {
                        iocb = &queue[i];
                        if (iocb->head)
                                yfree(&iocb->head);
                }
        }

        return NULL;
}

int __create_swap(const char *path, int size)
{
        int ret, fd1, fd2, left, cp;
        char buf[MAX_BUF_LEN];
        //sem_t *sem;

        DINFO("create journal file at %s, please waiting...\n", path);

#if 0
        sem = sem_open("/create_sem", O_CREAT, 0644, 1);
        if (sem == SEM_FAILED) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        ret = sem_wait(sem);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }
#endif

        fd1 = creat(path, 0644);
        if (fd1 < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        fd2 = open("/dev/zero", O_RDONLY);
        if (fd2 < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        left = size;

        while (left) {
                cp = left < MAX_BUF_LEN ? left : MAX_BUF_LEN;

                ret = _read(fd2, buf, cp);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                ret = _write(fd1, buf, cp);
                if (ret < 0) {
                        ret = -ret;
                        GOTO(err_ret, ret);
                }

                left -= cp;
        }

        close(fd1);
        close(fd2);

#if 0
        sem_post(sem);
        sem_close(sem);
#endif

        return 0;
err_ret:
        return ret;
}

int rjnl_init(const char *_path, int count, int size, rjnl_t *jnl,
              int *dirty, const char *lock)
{
        int ret, i, fd;
        char path[MAX_PATH_LEN];
        struct stat stbuf;
        pthread_t th;
        pthread_attr_t ta;

        (void) lock;

        ret = path_validate(_path, 1, 1);
        if (ret)
                GOTO(err_ret, ret);

        (void) pthread_attr_init(&ta);
        (void) pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);

        YASSERT(count < RJNL_MAX_COUNT);

        jnl->queue.ctx = 0;

        ret = io_setup(RJNL_MAX_REQ, &jnl->queue.ctx);
        if (ret) {
                ret = -ret;
                GOTO(err_ret, ret);
        }

        ret = sy_spin_init(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = sem_init(&jnl->queue.sem, 0, 0);
        if (ret)
                GOTO(err_ret, ret);

        jnl->seq = 1;

        for (i = 0; i < count; i++) {
                snprintf(path, MAX_PATH_LEN, "%s/%u", _path, i);

        retry:
                fd = open(path, O_RDWR | O_DIRECT);
                if (fd < 0) {
                        ret = errno;
                        if (ret == ENOENT) {
                                ret = __create_swap(path, size);
                                if (ret)
                                        GOTO(err_ret, ret);

                                goto retry;
                        }

                        GOTO(err_ret, ret);
                }

                ret = fstat(fd, &stbuf);
                if (ret < 0) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                if (stbuf.st_size != size) {
                        ret = EIO;
                        GOTO(err_ret, ret);
                }

                jnl->file.array[i].fd = fd;
                jnl->file.array[i].ref = 0;
                jnl->file.array[i].last_use = 0;
                jnl->file.array[i].seq = jnl->seq;
                jnl->file.array[i].ref_total = 0;
                jnl->file.array[i].unref_total = 0;
        }

        snprintf(path, MAX_PATH_LEN, "%s/dirty", _path);

        ret = stat(path, &stbuf);
        if (ret < 0) {
                ret = errno;

                if (ret == ENOENT) {
                        *dirty = 0;

                        fd = creat(path, 0644);
                        if (fd < 0) {
                                ret = errno;
                                GOTO(err_ret, ret);
                        }

                        close(fd);
                }
        } else {
                *dirty = 1;
        }

        jnl->file.count = count;
        jnl->file.idx = 0;
        jnl->file.max_size = size;
        jnl->queue.count = 0;
        jnl->inited = 1;
        strcpy(jnl->path, _path);

        ret = pthread_create(&th, &ta, __worker, jnl);
        if (ret < 0) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}

static int __getloc(rjnl_t *jnl, int *fd, rjnl_handle_t *idx, int size, int *off)
{
        int ret, newidx;
        rjnl_seg_t *seg;

        YASSERT(size < jnl->file.max_size);

        seg = &jnl->file.array[jnl->file.idx];

        if (seg->off + size > jnl->file.max_size) {
                newidx = jnl->file.idx + 1;
                if (newidx == jnl->file.count)
                        newidx = 0;

                seg = &jnl->file.array[newidx];

                if (seg->ref) { /*reference mode*/
                        ret = EBUSY;
                        DWARN("idx %u ref %u, busy\n", newidx, seg->ref);
                        goto err_ret;
                }

                jnl->file.idx = newidx;
                seg->off = 0;
                seg->seq = jnl->seq++;
        }

        idx->idx = jnl->file.idx;
        idx->seq = seg->seq;
        *fd = seg->fd;
        *off = seg->off;

        DBUG("size %u off %u idx %u/%u ref %u\n", size, *off, idx->idx, idx->seq, seg->ref);

        return 0;
err_ret:
        return ret;
}

static void __setloc(rjnl_t *jnl, rjnl_handle_t *idx, int size)
{
        rjnl_seg_t *seg;

        YASSERT(idx->idx == jnl->file.idx);
        YASSERT(size < jnl->file.max_size);

        seg = &jnl->file.array[idx->idx];
        seg->off += size;
        seg->ref++;
        seg->ref_total++;
        YASSERT(idx->seq == seg->seq);

        DBUG("seg %u off %u\n", idx->idx, seg->off);
}

static int __get_location(rjnl_t *jnl, int size, int *fd, int *offset, rjnl_handle_t *idx)
{
        int ret;

        ret = __getloc(jnl, fd, idx, size, offset);
        if (ret) {
                if (ret == EBUSY) {
                        goto err_ret;
                } else
                        GOTO(err_ret, ret);
        }

        __setloc(jnl, idx, size);

        return 0;
err_ret:
        return ret;
}

int rjnl_append(rjnl_t *jnl, const void *op, int oplen, const buffer_t *buf,
                rjnl_handle_t *idx, job_t *job)
{
        int ret, fd, off, count, size, i, off1, post = 0;
        iocb_t *iocb;
        head_t *head;
        struct iovec *io, iov[Y_BLOCK_MAX / PAGE_SIZE + 1];
        int iovcnt = Y_BLOCK_MAX / PAGE_SIZE + 1;

        YASSERT(idx);
        YASSERT(jnl->inited);
        YASSERT(buf->len % SDFS_BLOCK_SIZE == 0);
        YASSERT(oplen + sizeof(head_t) < SDFS_BLOCK_SIZE);

        ret = mbuffer_trans(iov, &iovcnt, buf);
        YASSERT(ret == (int)buf->len);

        YASSERT(iovcnt < Y_BLOCK_MAX / PAGE_SIZE + 1);

        size = SDFS_BLOCK_SIZE + buf->len;

retry:
        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        ret = __get_location(jnl, size, &fd, &off, idx);
        if (ret)
                GOTO(err_lock, ret);

        DBUG("get idx %u/%u size %u offset %u\n", idx->idx, idx->seq, size, off);

        if (jnl->queue.count + iovcnt >= RJNL_MAX_QUEUE) {
                sy_spin_unlock(&jnl->lock);
                rjnl_release(jnl, idx);
                DWARN("sleep\n");
                sleep(1);
                goto retry;
        }

        iocb = &jnl->queue.queue[jnl->queue.count];

        ret = ymalign(&iocb->head, SDFS_BLOCK_SIZE, SDFS_BLOCK_SIZE);
        if (ret ) {
                GOTO(err_lock, ret);
        }

        head = iocb->head;
        head->magic = YFS_MAGIC;
        head->headlen = oplen + sizeof(head_t);
        head->datalen = buf->len;
        memcpy(head->buf, op, oplen);

        off1 = 0;
        count = 0;
        YASSERT(off % SDFS_BLOCK_SIZE == 0);
        io_prep_pwrite(&iocb->iocb, fd, head, SDFS_BLOCK_SIZE, off);
        iocb->iocb.data = NULL;
        count++;
        off1 += SDFS_BLOCK_SIZE;

        for (i = 0; i < iovcnt; i++) {
                io = (struct iovec *)&iov[i];
                DBUG("newseg, len %llu count %u iovcnt %u\n", (LLU)io->iov_len, count, iovcnt);

                iocb = &jnl->queue.queue[jnl->queue.count + count];

                YASSERT(off1 + off + io->iov_len <= (unsigned)jnl->file.max_size);
                YASSERT((off + off1) % SDFS_BLOCK_SIZE == 0);
                YASSERT(io->iov_len % SDFS_BLOCK_SIZE == 0);
                io_prep_pwrite(&iocb->iocb, fd, io->iov_base, io->iov_len, off + off1);
                iocb->iocb.data = NULL;
                iocb->head = NULL;
                off1 += io->iov_len;

                count++;
        }

        iocb = &jnl->queue.queue[jnl->queue.count + count - 1];
        iocb->iocb.data = job;
        //job_set_child(job, iovcnt + 1);

        if (jnl->queue.count == 0)
                post = 1;

        jnl->queue.count += count;

        DBUG("jnl count %u %u headlen %u datalen %u\n", jnl->queue.count, count,
             head->headlen, head->datalen);

        sy_spin_unlock(&jnl->lock);

        if (post)
                sem_post(&jnl->queue.sem);

        return 0;
err_lock:
        sy_spin_unlock(&jnl->lock);
err_ret:
        return ret;
}

int rjnl_release(rjnl_t *jnl, rjnl_handle_t *idx)
{
        int ret;
        rjnl_seg_t *seg;

        YASSERT(jnl->inited);

        YASSERT(idx->idx < jnl->file.count);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        seg = &jnl->file.array[idx->idx];
        YASSERT(idx->seq == seg->seq);
        YASSERT(seg->ref > 0);
        seg->ref--;
        seg->unref_total++;

        //YASSERT(seg->ref == seg->ref_total - seg->unref_total);

        DBUG("seg %u/%u ref %d\n", idx->idx, idx->seq, seg->ref);

        sy_spin_unlock(&jnl->lock);

        idx->idx = -1;
        idx->seq = -1;

        return 0;
err_ret:
        return ret;
}

int rjnl_release1(rjnl_t *jnl, int idx)
{
        int ret;
        rjnl_seg_t *seg;

        YASSERT(jnl->inited);

        YASSERT(idx < jnl->file.count);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        seg = &jnl->file.array[idx];
        YASSERT(seg->ref > 0);
        seg->ref--;
        seg->unref_total++;

        //YASSERT(seg->ref == seg->ref_total - seg->unref_total);

        sy_spin_unlock(&jnl->lock);

        return 0;
err_ret:
        return ret;
}

int rjnl_newref(rjnl_t *jnl, int idx)
{
        int ret;
        rjnl_seg_t *seg;

        YASSERT(jnl->inited);

        YASSERT(idx < jnl->file.count);

        ret = sy_spin_lock(&jnl->lock);
        if (ret)
                GOTO(err_ret, ret);

        seg = &jnl->file.array[idx];
        YASSERT(seg->ref > 0);
        seg->ref++;

        DBUG("seg %u ref %d\n", idx, seg->ref);

        sy_spin_unlock(&jnl->lock);

        return 0;
err_ret:
        return ret;
}

static void *__next(head_t *head, const void *end)
{
        int seek = 0;
        if (head->magic != YFS_MAGIC)
                return NULL;

        head = (void *)head + head->datalen + SDFS_BLOCK_SIZE;

        if ((void *)head + sizeof(head_t) > end)
                return NULL;

        while (head->magic != YFS_MAGIC) {
                seek++;
                if ((void *)head + sizeof(head_t) + 1 > end)
                        return NULL;

                head = (void *)head + 1;
        }

        if ((void *)head + head->datalen + SDFS_BLOCK_SIZE > end)
                return NULL;

        if (seek && seek > 4096) {
                DWARN("try to seek %u, do we need this? left %llu\n", seek, (LLU)(end - (void *)head));
        }

        return head;
}

int rjnl_load(rjnl_t *jnl, int (*callback)(void *, void *, int, void *, int), void *arg)
{
        int ret, i;
        rjnl_seg_t *seg;
        void *addr;
        head_t *head;

        for (i = 0; i < jnl->file.count; i++) {
                seg = &jnl->file.array[i];

                addr = mmap(0, jnl->file.max_size, PROT_READ, MAP_PRIVATE, seg->fd, 0);
                if (addr == MAP_FAILED) {
                        ret = errno;
                        GOTO(err_ret, ret);
                }

                head = addr;
                if (head->magic != YFS_MAGIC) {
                        DINFO("no data %u, magic %x\n", i, head->magic);
                        munmap(addr, jnl->file.max_size);
                        continue;
                }

                while (head != NULL) {
                        YASSERT(head->headlen > sizeof(head_t));

                        ret = callback(arg, head->buf, head->headlen - sizeof(head_t),
                                       (void *)head + SDFS_BLOCK_SIZE, head->datalen);
                        if (ret)
                                GOTO(err_ret, ret);

                        head = __next(head, addr + jnl->file.max_size);
                }

                munmap(addr, jnl->file.max_size);
        }

        return 0;
err_ret:
        return ret;
}

void rjnl_destroy(rjnl_t *jnl)
{
        char path[MAX_PATH_LEN];

        snprintf(path, MAX_PATH_LEN, "%s/dirty", jnl->path);

        unlink(path);

        io_destroy(jnl->queue.ctx);
}

void rjnl_dump(rjnl_t *jnl)
{
        int i;
        rjnl_seg_t *seg;

        for (i = 0; i < jnl->file.count; i++) {
                seg = &jnl->file.array[i];
                DINFO("seg %u ref %u\n", i, seg->ref);
        }
}

