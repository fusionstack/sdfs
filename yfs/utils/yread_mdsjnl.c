#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "md_proto.h"
#include "dbg.h"

#define DEBUG 1

#ifdef DEBUG
#include <unistd.h>
#define DBG(arg...) fprintf(stderr, ## arg)

#define IN_FUN fprintf(stderr,"In function %d: %s\n", __LINE__, __FUNCTION__)
#define OUT_FUN fprintf(stderr,"Out function %d: %s\n", __LINE__, __FUNCTION__)
#else
#define DBG(arg...) {}
#endif

#define ERROR_QUIT(arg...)                                 \
        do{                                                \
                fprintf(stderr, "function[%s] line(%d)  ", \
                                __FUNCTION__, __LINE__);   \
                fprintf(stderr, ## arg);                   \
                exit(1);                                   \
        } while (0)




/* Default mds journal directry */
#define DEFAULT_JNL_DIR "/sysy/yfs/mds/1/jnl/"

/* functions */

int analyze_mds_jnl(char *pathname);

uint32_t get_mdp(uint32_t op, void *buf);

/* The operation list, debug only */
char *operation[41] = {
        "UNKNOWN",    /* 0 */

        "GETATTR",
        "fid_getattr",
        "mkdir",
        "CHMOD",
        "CHOWN",
        "unlink",
        "rename",
        "rm",
        "opendir",
        "LINK2UNIQUE",    /* 10 */
        "SETXATTR",
        "GETXATTR",
        "create",
        "fsync",
        "open",
        "fidopen",
        "truncate",
        "chkload",
        "chkget",
        "readdir",       /* 20 */
        "statvfs",
        "useradd",
        "login",
        "passwd",
        "ping",
        "diskjoin",
        "diskrept",
        "diskhb",
        "shadowjoin",
        "fid_chmod",       /* 30 */
        "fid_chown",
        "fid_truncate",
        "setopt",
        "readlink",
        "symlink",
        "link",
        "readdirplus",
        "lvset",
        "lvlist",
        "lvcreate"
};

int read_mds_jnlfile(char *jnlname)
{
        int            ret;
        FILE          *fd;
        size_t         size;
        struct stat    statbuf;
        char          *file_buf;
        char          *tmp_buf;
        uint32_t       mdp_op;
        jnl_head_t     *head_data;

        if ((fd = fopen(jnlname, "rb")) == NULL)
                ERROR_QUIT("Open %s error: %s\n",
                                jnlname, strerror(errno));

        if (lstat(jnlname, &statbuf) < 0){
                fclose(fd);
                ERROR_QUIT("lstat %s error: %s\n",
                                jnlname, strerror(errno));
        }

        size = statbuf.st_size;
        if ((file_buf = (char*) malloc((size_t) size)) == NULL) {
                fclose(fd);
                ERROR_QUIT("Malloc error: %s\n", strerror(errno));
        }

        tmp_buf = file_buf;

        DBG("file size is: %lu\n", (unsigned long)size);
        if (fread(file_buf, size, 1, fd) != 1) {
                free(file_buf);
                fclose(fd);
                ERROR_QUIT("Read from %s Error\n", jnlname);
        }

        /* start annlyze journal */
        DBG("Start analyze %s\n", jnlname);

        while(tmp_buf < file_buf + size) {
                tmp_buf = (void *)tmp_buf;
                head_data = (jnl_head_t *)tmp_buf;
                tmp_buf += sizeof(jnl_head_t);
                _memset((void *) &mdp_op, 0x0, 0x4);
                _memcpy((void *) &mdp_op, (void *) tmp_buf, 0x4);

                ret = get_mdp(mdp_op, (void *)tmp_buf);
                if (ret)
                        ERROR_QUIT("Get_mdp() read message from %s Error\n", jnlname);
                tmp_buf += head_data->len;
          //      sleep(1);
        }

        free(file_buf);
        fclose(fd);

        return 0;
}

int analyze_mds_jnl(char *pathname)
{
        int            ret;
        DIR           *dp;
        char           jnlname[MAX_BUF_LEN];
        struct dirent *dirp;

        if ((dp = opendir(pathname)) == NULL)
                ERROR_QUIT("Open dir %s Error: %s\n", pathname,
                                strerror(errno));

        while ((dirp = readdir(dp)) != NULL) {
                if (_strcmp(dirp->d_name, ".") == 0 ||
                    _strcmp(dirp->d_name, "..") == 0)
                        continue;

                _strcpy(jnlname, pathname);
                strcat(jnlname, dirp->d_name);

                DBG("Open and read %s\n", jnlname);

                ret = read_mds_jnlfile(jnlname);
                if (ret)
                        ERROR_QUIT("read_mds_njlfile() read from %s Error\n", jnlname);
        }

        return 0;
}

/*
 * Rebuild the directory tree
 */
uint32_t get_mdp(uint32_t op, void *buf)
{
        DBG("%s ", operation[op]);

        switch (op) {
                case MDP_GETATTR:
                case MDP_UNLINK:
                case MDP_RMDIR:
                case MDP_OPENDIR:
                        {
                                mdp_getattr_req_t *mdp;
                                mdp = (mdp_getattr_req_t *)buf;
                                printf("op:%d,path_len:%llu,path:%s.",    \
                                       mdp->op, (LLU)mdp->path.len, mdp->path.buf);
                        }
                        break;
                case MDP_MKDIR:
                        {
                                mdp_mkdir_req_t *mdp;
                                mdp = (mdp_mkdir_req_t *)buf;
                                printf("op:%d,mode:%d,path_len:%llu,path:%s.\n", \
                                       mdp->op, mdp->mode, (LLU)mdp->path.len, mdp->path.buf);
                        }
                        break;
                case MDP_CHMOD:
                case MDP_CREATE:
                        {
                                mdp_create_req_t *mdp;
                                mdp = (mdp_create_req_t *)buf;
                                printf("op:%d,mode:%d,fileid:%llu,path_len:%llu,path:%s.\n", \
                                       mdp->op, mdp->mode, (LLU)mdp->fileid.id,
                                       (LLU)mdp->path.len, mdp->path.buf);
                        }
                        break;
                case MDP_RENAME:
                        {
                                mdp_rename_req_t *mdp;
                                mdp = (mdp_rename_req_t *)buf;
                                printf("op:%d,from_len:%d,to_len:%d,from:%s to %s.\n", \
                                       mdp->op, mdp->from_len, mdp->to_len, mdp->buf.buf,
                                       mdp->buf.buf + mdp->from_len);
                        }
                        break;
                case MDP_FSYNC:
                        {
                                mdp_fsync_req_t *mdp;
                                mdp = (mdp_fsync_req_t *)buf;
                                printf("op:%d, fileid:%llu,chkno:%d,chkid:%llu,chkoff:%d,chklen:%d. \n", \
                                                mdp->op,(LLU)mdp->fileid.id, mdp->chkno, (LLU)mdp->chkid.id, \
                                                mdp->chkoff, mdp->chklen);
                        }
                        break;
                case MDP_OPEN:
                        {
                                mdp_open_rep_t *mdp;
                                mdp = (mdp_open_rep_t *)buf;
                                printf("fileid:%llu,version:%d,chkid:%llu.\n", \
                                                (LLU)mdp->fileid.id, mdp->fileid.version, (LLU)mdp->md[0].chks[0].chkid.id);
                        }
                        break;
                case MDP_CHOWN:
                        {
                                mdp_chown_req_t *mdp;
                                mdp = (mdp_chown_req_t *)buf;
                                printf("op:%d,owner:%d,group:%d,path_len:%llu,path:%s.\n", \
                                               mdp->op, mdp->owner, mdp->group, (LLU)mdp->path.len, mdp->path.buf);
                        }
                        break;
                case MDP_SETXATTR:
                        {
                                mdp_xattr_req_t *mdp;
                                mdp = (mdp_xattr_req_t *)buf;
                                printf("op:%d,flags:%d,pathlen:%d,namelen:%d,valuelen:%d,buf:%s.\n", \
                                                mdp->op, mdp->flags, mdp->pathlen, mdp->namelen, mdp->valuelen, mdp->buf);
                        }
                        break;
                case MDP_GETXATTR:
                        {
                                mdp_getxattr_req_t *mdp;
                                mdp = (mdp_getxattr_req_t *)buf;
                                printf("op:%d,pathlen:%d,namelen:%d,buf:%s.\n", \
                                                mdp->op, mdp->pathlen, mdp->namelen, mdp->buf);
                        }
                        break;
                case MDP_TRUNCATE:
                        {
                                mdp_truncate_req_t *mdp;
                                mdp = (mdp_truncate_req_t *)buf;
                                printf("op:%d, path_len:%llu, file_len:%llu, path:%s.\n", \
                                       mdp->op, (LLU)mdp->path.len, (LLU)mdp->file_len, mdp->path.buf);
                        }
                        break;
                case MDP_CHKLOAD:
                        {
                                mdp_chkload_req_t *mdp;
                                mdp = (mdp_chkload_req_t *)buf;
                                printf("chkload");
                                printf("op:%d,chkid:%llu,version:%d.\n", \
                                       mdp->op, (LLU)mdp->chkid.id, mdp->chkid.version);
                        }
                        break;
                case MDP_CHKGET:
                        {
                                mdp_chkget_req_t *mdp;
                                mdp = (mdp_chkget_req_t *)buf;
                                printf("op:%d,chkid:%llu,version:%d,chk_rep:%d,chk_len:%d,path_len:%llu,path:%s.\n", \
                                       mdp->op, (LLU)mdp->chkid.id, mdp->chkid.version,
                                       mdp->chk_rep,
                                       mdp->chk_len, (LLU)mdp->path.len,mdp->path.buf);
                        }
                        break;
                case MDP_READDIR:
                        {
                                mdp_readdir_req_t *mdp;
                                mdp = (mdp_readdir_req_t *)buf;
                                printf("op:%d,offset:%llu,path_len:%llu,path:%s.\n",
                                       mdp->op, (LLU)mdp->offset, (LLU)mdp->path.len, mdp->path.buf);
                        }
                        break;
                case MDP_STATVFS:
                        {
                                mdp_getxattr_req_t *mdp;
                                mdp = (mdp_getxattr_req_t *)buf;
                                printf("op:%d,pathlen:%d,namelen:%d,buf:%s.\n",
                                       mdp->op, mdp->pathlen, mdp->namelen, mdp->buf);
                        }
                        break;
                case MDP_USERADD:
                case MDP_LOGIN:
                        {
                                mdp_useradd_req_t *mdp;
                                mdp = (mdp_useradd_req_t *)buf;
                                printf("op:%d,usernamelen:%d,passwdlen:%d,buf:%s.\n", \
                                       mdp->op, mdp->usernamelen, mdp->passwdlen, mdp->buf);
                        }
                        break;
                case MDP_PASSWD:
                        {
                                mdp_passwd_req_t *mdp;
                                mdp = (mdp_passwd_req_t *)buf;
                                printf("op:%d,usernamelen:%d,oldpasswdlen:%d,newpasswdlen:%d,buf:%s.\n",
                                       mdp->op, mdp->usernamelen, mdp->oldpasswdlen, mdp->newpasswdlen, mdp->buf);
                        }
                        break;
                case MDP_PING:
                        {
                                mdp_ping_req_t *mdp;
                                mdp = (mdp_ping_req_t *)buf;
                                printf("op:%d.\n",mdp->op);
                        }
                        break;
                case MDP_DISKJOIN:
                        {
                                mdp_diskjoin_req_t *mdp;
                                mdp = (mdp_diskjoin_req_t *)buf;
                                printf("op:%d,img diskid:%llu,version:%d,stat ds_fsid:%d,info:%s.\n", \
                                                mdp->op, (LLU)mdp->img.diskid.id, mdp->img.diskid.version, \
                                                mdp->stat.ds_fsid, mdp->info);
                        }
                        break;
                case MDP_DISKREPT:
                        {
                                uint32_t i;
                                mdp_diskrept_req_t *mdp;
                                mdp = (mdp_diskrept_req_t *)buf;
                                printf("op:%d,diskid:%llu,version:%d,chkerptnum:%d.\n", \
                                                        mdp->op, (LLU)mdp->id.id, mdp->id.version, mdp->chkreptnum);
                                for(i=0; i<mdp->chkreptnum; i++)
                                        printf("chkid_%d:%llu.\n", i+1, (LLU)mdp->chkrept[i].proto.chkid.id);
                        }
                        break;
                case MDP_SHADOWJOIN:
                        {
                                mdp_shadowjoin_req_t *mdp;
                                mdp = (mdp_shadowjoin_req_t *)buf;
                                printf("op:%d.\n", mdp->op);
                        }
                        break;
                case MDP_FID_CHMOD:
                        {
                                mdp_fid_chmod_req_t *mdp;
                                mdp = (mdp_fid_chmod_req_t *)buf;
                                printf("op:%d,mode:%d,fileid:%llu,version:%d.\n", \
                                                mdp->op, mdp->mode, (LLU)mdp->fileid.id, mdp->fileid.version);
                        }
                        break;
                case MDP_FID_CHOWN:
                        {
                                mdp_fid_chown_req_t *mdp;
                                mdp = (mdp_fid_chown_req_t *)buf;
                                printf("op:%d,owner:%d,group:%d,fileid:%llu,version:%d.\n", \
                                                mdp->op, mdp->owner, mdp->group, (LLU)mdp->fileid.id, mdp->fileid.version);
                        }
                        break;
                case MDP_FID_TRUNCATE:
                        {
                                mdp_fid_truncate_req_t *mdp;
                                mdp = (mdp_fid_truncate_req_t *)buf;
                                printf("op:%d,file_len:%llu,fileid:%llu,version:%d.\n", \
                                                mdp->op, (LLU)mdp->file_len, (LLU)mdp->fileid.id, mdp->fileid.version);
                        }
                        break;
                case MDP_SETOPT:
                        {
                                mdp_setopt_req_t *mdp;
                                mdp = (mdp_setopt_req_t *)buf;
                                printf("op:%d,key:%d,value:%d.\n", \
                                                mdp->op, mdp->key, mdp->value);
                        }
                        break;
                case MDP_READLINK:
                        {
                                mdp_readlink_req_t *mdp;
                                mdp = (mdp_readlink_req_t *)buf;
                                printf("op:%d,buflen:%llu,buf:%s.\n",     \
                                       mdp->op, (LLU)mdp->buf.len, mdp->buf.buf);
                        }
                        break;
                case MDP_SYMLINK:
                        {
                                mdp_symlink_req_t *mdp;
                                mdp = (mdp_symlink_req_t *)buf;
                                printf("op:%d, from:%s to %s.\n", \
                                       mdp->op, mdp->buf.buf, mdp->buf.buf + mdp->from_len);
                        }
                        break;
                case MDP_LVSET:
                        {
                                mdp_lvset_req_t *mdp;
                                mdp = (mdp_lvset_req_t *)buf;
                                printf("op:%d,buflen:%llu,size:%llu,buf:%s.\n", \
                                                mdp->op, (LLU)mdp->name.len, (LLU)mdp->size, mdp->name.buf);
                        }
                        break;
                case MDP_LVLIST:
                        {
                                mdp_lvlist_req_t *mdp;
                                mdp = (mdp_lvlist_req_t *)buf;
                                printf("op:%d.\n", mdp->op);
                        }
                        break;
                case MDP_LVCREATE:
                        {
                                mdp_lvcreate_req_t *mdp;
                                mdp = (mdp_lvcreate_req_t *)buf;
                                printf("op:%d,buflen:%llu,size:%llu,id:%d,buf:%s.\n", \
                                       mdp->op, (LLU)mdp->name.len, (LLU)mdp->size, mdp->volid, mdp->name.buf);
                        }
                        break;
                default:
                        ERROR_QUIT("Warning: Unhandled op %d: %s\n",
                                        op, operation[op]);
                        break;
        }

        return 0;
}

int main(int argc, char **argv)
{
        int ret;
        char c_opt;
        char *prog;

        prog = strrchr(argv[0], '/');
        prog?++prog:argv[0];

        if (argc != 3) {
                fprintf(stderr, "%s [-v] <dirpath>.\neg: %s -v %s\n",  \
                                prog, prog, DEFAULT_JNL_DIR);
                fprintf(stderr, "%s [-f] <filepath>.\neg: %s -f %sfilename\n",  \
                                prog, prog, DEFAULT_JNL_DIR);
                exit(1);
        }

        while ((c_opt = getopt(argc, argv, "vf")) > 0) {
                switch (c_opt) {
                case 'v':
                        ret = analyze_mds_jnl(argv[2]);
                        if (ret) {
                                printf("exist error!\n");
                                exit(1);
                        }
                        break;
                case 'f':
                        ret = read_mds_jnlfile(argv[2]);
                        if (ret)
                                ERROR_QUIT("read_mds_njlfile() read from %s Error\n", argv[2]);
                        break;

                default:
                        fprintf(stderr, "Hoops, wrong op got!\n");
                        exit(1);
                }
        }


        return 0;
}
