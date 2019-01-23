

#define __USE_FILE_OFFSET64

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>

#include "configure.h"
#include "sdfs_lib.h"
#include "chk_meta.h"

#define CDS_CHK_DIR "/sysy/yfs/cds/%d/ychunk"
#define INVALID_CHK_LOG "invalid_chunk-%d.log"

static int check_cds_chunk(char *chk_dir);
static int process_chk(char *chk_path);

int chk_log_fd;

int main(int argc, char **argv)
{
        int ret, cds_no, args, verbose = 0;
        char c_opt, *prog, cds_chk_dir[MAX_PATH_LEN], invalid_chk[MAX_BUF_LEN];

        prog = strchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        args = 1;

        while((c_opt = getopt(argc, argv, "vn:")) > 0)
                switch (c_opt) {
                case 'v':
                        verbose = 1;
                        args++;
                        break;
                case 'n':
                        cds_no = atoi(argv[++args]);
                        snprintf(cds_chk_dir, MAX_PATH_LEN, 
                                        CDS_CHK_DIR, cds_no);
                        printf("%s\n", cds_chk_dir);
                        args++;
                        break;
                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }

        snprintf(invalid_chk, MAX_BUF_LEN, INVALID_CHK_LOG, cds_no);
        chk_log_fd = open(invalid_chk, O_CREAT | O_WRONLY, 
                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        if (chk_log_fd < 0) {
                DERROR("Open %s error:%s\n", INVALID_CHK_LOG, strerror(errno));
                exit(1);
        }

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("ychk_check");
        if (ret) {
                fprintf(stderr, "ly_init() %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("ly_init()'ed\n");

        ret = check_cds_chunk(cds_chk_dir);
        if (ret) {
                fprintf(stderr, "check_cds_chunk %s\n", strerror(ret));
                exit(1);
        } else if (verbose)
                printf("check_cds_chunk()'ed\n");

        close(chk_log_fd);

        return 0;
}

int check_cds_chunk(char *chk_path)
{
        struct dirent *dirp;
        struct stat statbuf;
        DIR *dp;
        char tmp[MAX_PATH_LEN];
        int ret;

        if (lstat(chk_path, &statbuf) < 0) {
                ret = errno;
                DERROR("stat %s error:%s\n", chk_path, strerror(errno));
                GOTO(err_ret, ret);
        }

        if (S_ISDIR(statbuf.st_mode)  == 0) { //notdir, porcess 
                ret = process_chk(chk_path);
                if (ret)
                        GOTO(err_ret, ret);

                return 0;
        }

        if ((dp = opendir(chk_path)) == NULL) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        while ((dirp = readdir(dp)) != NULL) {
                if (strcmp(dirp->d_name, ".") == 0 ||
                    strcmp(dirp->d_name, "..") ==  0)
                        continue;

                snprintf(tmp, MAX_PATH_LEN, "%s/%s", chk_path, dirp->d_name);

                ret = check_cds_chunk(tmp);
                if (ret) 
                        GOTO(err_dp, ret);
        }

        closedir(dp);
        return 0;
err_dp:
        closedir(dp);
err_ret:
        return ret;
}

int process_chk(char *chk_path)
{
        int fd, ret;
        uint32_t buflen;
        //struct stat stbuf;
        chkmeta2_t md;
        //char tmp[1024];

        fd = open(chk_path, O_RDONLY);
        if (fd == -1) {
                ret = errno;
                DERROR("Open %s error:%s\n", chk_path, strerror(ret));
                GOTO(err_ret, ret);
        }

        buflen = sizeof(chkmeta2_t);
        ret = sy_pread(fd, (void*)&md, &buflen, 0);
        if (ret) {
                DERROR("open %s error:%s\n", chk_path, strerror(errno));
                GOTO(err_fd, ret);
        }

        close(fd);

        UNIMPLEMENTED(__DUMP__);

#if 0
        ret = ly_fidopen(&fileid);
        if (ret < 0) {
                ret = -ret;
                if (ret != ENOENT)
                        fprintf(stderr, "%s errno:%d\n", chk_path, ret);

                snprintf(tmp, MAX_BUF_LEN, "%s %d %llu_v%u\n",
                                chk_path, ret, (LLU)md.proto.chkid.id,
                                md.proto.chkid.version);
                ret = sy_write(chk_log_fd, tmp, strlen(tmp));
                if (ret)
                        GOTO(err_fd, ret);
        } else
                ly_release(ret);
#endif

        return 0;
err_fd:
        close(fd);
err_ret:
        return ret;
}
