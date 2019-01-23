#ifndef __YLIB_DBG_PROTO_H__
#define __YLIB_DBG_PROTO_H__

/* debug subsystems (32 bits, non-overlapping) */
#define S_UNDEF                 0x00000000
#define S_LIBYLIB                  0x00000001
#define S_LIBYLIBLOCK              0x00000002
#define S_LIBYLIBMEM               0x00000004
#define S_LIBYLIBSKIPLIST          0x00000008
#define S_YSOCK                 0x00000010
#define S_LIBYNET                  0x00000020
#define S_YRPC                  0x00000040
#define S_YFSCDC                0x00000080
#define S_YFSCDS                0x00000100
#define S_YFSLIB                0x00000200
#define S_YFSMDC                0x00000400
#define S_YFSMDS                0x00000800
#define S_YFSFUSE               0x00001000
#define S_YWEB                  0x00002000
#define S_YFTP                  0x00004000
#define S_LIBYLIBNLS               0x00008000
#define S_YNFS                  0x00010000
#define S_YP2P                  0x00020000
#define S_FSMACHINE             0x00040000
#define S_YOSS                  0x00080000
#define S_CDSMACHINE            0x00100000
#define S_YTABLE_MS             0x00200000
#define S_YTABLE_TS             0x00400000
#define S_YTABLE_CLI            0x00800000
#define S_YFSCDS_ROBOT          0x01000000
#define S_YTABLE                0x02000000
#define S_C60                   0x04000000
#define S_PROXY                 0x08000000
#define S_YISCSI                0x10000000
#define S_LIBSCHEDULE           S_LIBYLIB
#define S_YFUSE                 0x20000000

#define YFS_DEBUG
#define D_MSG_ON

/* debug masks (32 bits, non-overlapping) */
#define Y_BUG           0x00000001
#define Y_INFO          0x00000002
#define Y_WARNING       0x00000004
#define Y_ERROR         0x00000008
#define Y_FATAL         0x00000010

#define __D_BUG           0x00000001
#define __D_INFO          0x00000002
#define __D_WARNING       0x00000004
#define __D_ERROR         0x00000008
#define __D_FATAL         0x00000010

#ifndef DBG_SUBSYS
#define DBG_SUBSYS S_UNDEF
#endif
#endif
