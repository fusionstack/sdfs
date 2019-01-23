#ifndef __XDR_H__
#define __XDR_H__

#include "sdfs_buffer.h"

typedef enum {
        __XDR_ENCODE,
        __XDR_DECODE,
        __XDR_FREE
} xdr_op_t;

typedef struct {
        xdr_op_t op;
        buffer_t *buf;
} xdr_t;

typedef int enum_t;
typedef int (*xdr_ret_t)(xdr_t *, void *);
typedef int (*__xdrproc_t) (xdr_t *, void *,...);

extern int __xdr_void(void);
extern int __xdr_bytes(xdr_t *, char **__cpp, uint32_t *__sizep,
                       uint32_t  __maxsize);
extern int __xdr_string(xdr_t *, char **__cpp, u_int __maxsize);
extern int __xdr_enum(xdr_t *, enum_t *__ep);
extern int __xdr_uint32(xdr_t *, uint32_t *__up);
extern int __xdr_uint64(xdr_t *, uint64_t *__up);
extern int __xdr_int(xdr_t *xdr, int *__ip);
extern int __xdr_bool(xdr_t *, int *__bp);
extern int __xdr_opaque(xdr_t *, char * __cp, u_int __cnt);
extern int __xdr_pointer(xdr_t *, char **__objpp,
			   u_int __obj_size, __xdrproc_t);
extern int __xdr_long(xdr_t *, long *val);
extern int __xdr_array(xdr_t *, void *__addrp, uint32_t *__sizep,
                        uint32_t __maxsize, uint32_t __size, __xdrproc_t __proc);
extern int __xdr_reference (xdr_t *, char **__xpp, uint32_t __size,
			     __xdrproc_t __proc);
extern int __xdr_buffer(xdr_t *xdr, char **_buf, uint32_t *size,
			uint32_t max);

#if 0
extern int __xdr_short (xdr_t *, short *__sp);
extern int __xdr_u_short (xdr_t *, u_short *__usp);
extern int __xdr_int (xdr_t *, int *__ip);
extern int __xdr_u_int (xdr_t *, u_int *__up);
extern int __xdr_long (xdr_t *, long *__lp);
extern int __xdr_hyper (xdr_t *, quad_t *__llp);
extern int __xdr_u_hyper (xdr_t *, u_quad_t *__ullp);
extern int __xdr_longlong (xdr_t *, quad_t *__llp);
extern int __xdr_u_longlong (xdr_t *, u_quad_t *__ullp);
extern int __xdr_int8 (xdr_t *, int8_t *__ip);
extern int __xdr_uint8 (xdr_t *, uint8_t *__up);
extern int __xdr_int16 (xdr_t *, int16_t *__ip);
extern int __xdr_uint16 (xdr_t *, uint16_t *__up);
extern int __xdr_int32 (xdr_t *, int32_t *__ip);
extern int __xdr_int64 (xdr_t *, int64_t *__ip);
extern int __xdr_quad (xdr_t *, quad_t *__ip);
extern int __xdr_u_quad (xdr_t *, u_quad_t *__up);
extern int __xdr_union (xdr_t *, enum_t *__dscmp, char *__unp,
			 __const struct __xdr_discrim *__choices,
			 xdrproc_t dfault);
extern int __xdr_char (xdr_t *, char *__cp);
extern int __xdr_u_char (xdr_t *, u_char *__cp);
extern int __xdr_vector (xdr_t *, char *__basep, u_int __nelem,
			  u_int __elemsize, xdrproc_t __xdr_elem);
extern int __xdr_float (xdr_t *, float *__fp);
extern int __xdr_double (xdr_t *, double *__dp);
extern int __xdr_wrapstring (xdr_t *, char **__cpp);
extern u_long __xdr_sizeof (xdrproc_t, void *);
#endif

#endif
