#ifndef __AIOCB_H__
#define __AIOCB_H__

#include "job.h"

static inline int __aio_return (struct aiocb *iocb)
{
        return iocb->__return_value; 
}

static inline int __aio_error (struct aiocb *iocb)
{
        return iocb->__error_code; 
}

#endif
