#ifndef __WORM_CLI_LIB_H__
#define __WORM_CLI_LIB_H__

#include "sdfs_worm.h"

#if ENABLE_WORM
extern int worm_init_wormclock_dir(fileid_t *_fileid);
extern int worm_set_clock_time(const fileid_t *fileid, const time_t wormclock_timestamp);
extern int worm_get_clock_time(const fileid_t *fileid, time_t *wormclock_timestamp);
extern int worm_update_clock_time(void);
extern int worm_set_file_attr(const fileid_t *fileid, worm_file_t *_worm_file);
extern int worm_get_file_attr(const fileid_t *fileid, worm_file_t *_worm_file);
extern int worm_remove_file_attr(const fileid_t *fileid);
extern int worm_get_attr(const fileid_t *fileid, worm_t *_worm);
extern int worm_set_attr(const fileid_t *fileid, worm_t *_worm);
extern int worm_remove_attr(const fileid_t *fileid);
extern worm_status_t worm_get_status(const fileid_t *fileid);
extern worm_status_t convert_string_to_worm_status(const char *string);
extern int worm_list_root_directory(void (*print_worm_root)(const worm_t *));
extern int get_wormfileid_by_fid_cli(const uint64_t fid, const fileid_t *subfileid, fileid_t *fileid);
extern int worm_auth_valid(const char *username, const char *password);
#endif

#endif

