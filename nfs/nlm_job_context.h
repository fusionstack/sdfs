/*
 * =====================================================================================
 *
 *       Filename:  nlm_job_context.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/07/2011 10:39:10 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#ifndef __NLM_JOB_CONTEXT_H__
#define __NLM_JOB_CONTEXT_H__


#include "xdr_nlm.h"

#include "sdfs_buffer.h"
#include "file_proto.h"

typedef union {
        nlm_lockargs   lockargs;
        nlm_cancargs   cancargs;
        nlm_testargs   testargs;
        nlm_unlockargs unlockargs;
        nlm_notifyargs notifyargs;
} arg_t;

typedef union {
        nlm_res     res;
        nlm_testres testres;
} ret_t;

typedef struct {
        arg_t arg;
        int eof; /*read eof*/
        fileid_t fileid;
        buffer_t buf;
} nlm_job_context_t;

/*************new struct************/
#endif
