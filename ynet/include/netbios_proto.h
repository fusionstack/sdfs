#ifndef __NETBIOS_PROTO_H__
#define __NETBIOS_PROTO_H__

#include "job.h"
#include "sdevent.h"



/*
typedef struct {
    uint32_t lowpart;
    int32_t highpart;
} large_integer;                      // 64 bits of data
*/



#pragma pack(1)
typedef struct {
	uint8_t		class;	// error class
	char		reserved;	// reserved for future use
	uint16_t	error;	// error code
} doserror_t;

#define SIZE_DOSERR (sizeof(doserror_t))
typedef struct  {
        char				protocol[4];	// contains 0xff,'smb'
        uint8_t				command;	// command code
        union {
		doserror_t              doserror;
                uint32_t		status;	// 32-bit error code
        } status;
        uint8_t				flags;	// flags
        uint16_t			flags2;	// more flags
        union {
                uint16_t		pad[6];	// ensure section is 12 bytes int32_t
                struct {
                        uint16_t	pidhigh;	// high part of pid
                        uint32_t	unused;	// not used
                        uint32_t	unused2;
                } extra;
        };
        uint16_t			tid;	// tree identifier
        uint16_t			pid;	// caller's process id
        uint16_t			uid;	// unauthenticated user id
        uint16_t			mid;	// multiplex id
} smb_head_t;

#pragma pack()



#define SMBERR_DOS 0x1
#define DOSERR_BADFILE 2
//-----------------------------------------------------------------------------
#define SMBERR_SRV 0x2
#define SRVERR_ERROR 1
#define SRVERR_ACCESS 4
//-----------------------------------------------------------------------------
#define SMBERR_HRD 0x3

//-----------------------------------------------------------------------------
#define SMBERR_CMD
//-----------------------------------------------------------------------------


extern int netbios_accept_handler(int fd, void *);
//extern int netbios_request_handler(void *self, void *);
extern int netbios_pack_handler(void *self, void *);
extern int netbios_pack_len(void *, uint32_t);

#endif
