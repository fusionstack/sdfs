#ifndef _WORM_H_
#define _WORM_H_

#define WORM_ARG_MIN_PROTECT        0x00000001
#define WORM_ARG_MAX_PROTECT        0x00000002
#define WORM_ARG_DEFAULT_PROTECT    0x00000004
#define WORM_ARG_AUTO_LOCK          0x00000008
#define WORM_ARG_PATH               0x0000000A

#define WORM_CLOCKDIR "/WORM_CLOCK"
#define WORM_FID_NULL 0

#define WORM_ATTR_KEY "uss_system_worm_attr"
#define WORM_FILE_KEY "uss_system_worm_file"
#define WORM_CLOCK_KEY "uss_system_worm_clock"

#define WORM_ADMIN_USER "wormadmin"
#define WORM_ADMIN_PASS "wormadmin"
#define WORM_ADMIN_LEN 9

#define ONE_HOUR (3600)

typedef enum {
        WORM_BEFORE_PROTECT = 1,
        WORM_IN_PROTECT,
        WORM_AFTER_PROTECT,
        WORM_NOT_SET
}worm_status_t;

typedef enum {
        WORM_ROOT = 1,
        WORM_SUBDIR,
        WORM_FILE
} worm_type_t;

typedef struct {
        worm_type_t worm_type;
        fileid_t fileid;
        uint32_t min_protect_period;
        uint32_t max_protect_period;
        uint32_t default_protect_period;
        uint32_t auto_lock_period;
        char path[MAX_NAME_LEN];
}worm_t;

typedef struct {
        char status[MAX_NAME_LEN];
        uint32_t set_atime;
}worm_file_t;

#endif
