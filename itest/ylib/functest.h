#ifndef __YNFS_IOTEST_H__
#define __YNFS_IOTEST_H__

#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define NORMAL  "\033[0m"

#define CREATE_DIR_TRUE         1
#define CREATE_DIR_FALSE        0

void make_test_file(unsigned, const char *);

void prepare_env(const char *, int);
void write_log(const char *);

void test_dir(const char *);
void test_mv(const char *);
void test_cp(const char *);
void test_split(const char *);
void test_longname(const char *);
void test_file(const char *);

#endif

