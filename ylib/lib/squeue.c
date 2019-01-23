

#include <stdint.h>
#include <errno.h>


#define DBG_SUBSYS S_LIBYLIB

#include "squeue.h"
#include "ylib.h"
#include "dbg.h"

typedef squeue_entry_t entry_t;

int squeue_init(squeue_t *queue, int group, int (*cmp_func)(const void *,const void *),
                uint32_t (*hash_func)(const void *))
{
        int ret;

        ret = htable_create(&queue->table, group, cmp_func, hash_func);
        if (ret) {
                DERROR("ret (%d) %s\n", ret, strerror(ret));
                GOTO(err_ret, ret);
        }

        INIT_LIST_HEAD(&queue->list);

        return 0;
err_ret:
        return ret;
}

int squeue_insert(squeue_t *queue, const void *id, void *_ent)
{
        int ret;
        entry_t *ent;

        ret = ymalloc((void *)&ent, sizeof(entry_t));
        if (ret)
                GOTO(err_ret, ret);

        ret = htable_insert(queue->table, id, (void *)ent);
        if (ret) {
                if (ret == EEXIST)
                        goto err_free;
                else
                        GOTO(err_free, ret);
        }

        ent->ent = _ent;

        list_add_tail(&ent->hook, &queue->list);

        return 0;
err_free:
        yfree((void **)&ent);;
err_ret:
        return ret;
}

int squeue_get(squeue_t *queue, const void *id, void **_ent)
{
        int ret;
        entry_t *ent;

        ret = htable_get(queue->table, id, (void **)&ent);
        if (ret)
                goto err_ret;

        *_ent = ent->ent;

        return 0;
err_ret:
        return ret;
}

int squeue_getfirst(squeue_t *queue, void **_ent)
{
        int ret;
        entry_t *ent;

        if (list_empty(&queue->list)) {
                ret = ENOENT;
                goto err_ret;
        }

        ent = (void *)queue->list.next;

        *_ent = ent->ent;

        return 0;
err_ret:
        return ret;
}

int squeue_remove(squeue_t *queue, const void *id, void **_ent)
{
        int ret;
        entry_t *ent;

        ret = htable_drop(queue->table, id, (void **)&ent);
        if (ret) {
                YASSERT(0);
                GOTO(err_ret, ret);
        }

        list_del(&ent->hook);

        if (_ent)
                *_ent = ent->ent;

        yfree((void **)&ent);

        return 0;
err_ret:
        return ret;
}

int squeue_move_tail(squeue_t *queue, const void *id)
{
        int ret;
        entry_t *ent;

        ret = htable_get(queue->table, id, (void **)&ent);
        if (ret)
                GOTO(err_ret, ret);

        list_del(&ent->hook);
        list_add_tail(&ent->hook, &queue->list);

        return 0;
err_ret:
        return ret;
}

int squeue_pop(squeue_t *queue, void **_ent)
{
        int ret;
        entry_t *ent;

        if (list_empty(&queue->list)) {
                ret = ENOENT;
                goto err_ret;
        }

        ent = (void *)queue->list.next;

        ret = htable_drop(queue->table, ent->ent, (void **)&ent);
        if (ret) {
                YASSERT(0);
                GOTO(err_ret, ret);
        }

        list_del(&ent->hook);

        *_ent = ent->ent;
        yfree((void **)&ent);

        return 0;
err_ret:
        return ret;
}
