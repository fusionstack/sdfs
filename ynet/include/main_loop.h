#ifndef __MAIN_LOOP_H__
#define __MAIN_LOOP_H__

#include <sys/epoll.h>
#include <semaphore.h>
#include <pthread.h>

#include "net_proto.h"
#include "../sock/ynet_sock.h"
#include "ylock.h"

extern int __main_loop_hold__;

void main_loop_start();
int main_loop_create(int threads);
void main_loop_hold();
int main_loop_check();

int main_loop_request(void (*exec)(void *buf), void *buf, const char *name);
int main_loop_event(int sd, int event, int op);

#if 1
#define main_loop_hold()                            \
        do {                                        \
            int __i__ = 0;                          \
            while (__main_loop_hold__) {                        \
                    sleep(1);                                   \
                    DINFO("main_loop_hold %u\n", __i__++);      \
            }                                                   \
        } while (0);
#else
#define main_loop_hold()
#endif


#endif
