#ifndef __NEWDEF_H__
#define __NEWDEF_H__



#define IO_WARN ((gloconf.rpc_timeout / 2) * 1000 * 1000)
#define IO_INFO ((1) * 1000 * 1000)

#if 0
#define IO_FUNC  __attribute__((section(".xiop")))
#else
#define IO_FUNC
#endif

#endif
