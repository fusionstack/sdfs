

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define DBG_SUBSYS S_YFSLIB

#include "configure.h"
#include "../cdc/replica.h"
#include "../mdc/md_lib.h"
#include "../cdc/cdc_lib.h"
#include "md_array.h"
#include "sdfs_lib.h"
#include "ylib.h"
#include "yfs_file.h"
#include "file_table.h"
#include "ynet_rpc.h"
#include "md_proto.h"
#include "net_global.h"
#include "yfs_node.h"
#include "sdfs_list.h"
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

/* The directory we rebuild the tree */
#define TREE_DIR "/dev/shm/mds"

/** 
 * This dir_t struct is used in 
 * walk through the tree in "TREE_DIR".
 */
typedef struct {
        struct list_head list;
        off_t            d_off;
        char             d_name[0];
} dir_t;

/**
 * The bad_chk_t struct record the chunk 
 * which missing some buckup.
 */
typedef struct {
        struct list_head  list;
        struct yfs_chunk *chk;
        char              path[MAX_PATH_LEN];
        int               def_rep;       /* The number of chunk should be */
        int               cur_rep;       /* The current number of chunk   */
} bad_chk_t;

typedef struct {
        struct list_head list;
        ynet_net_info_t *info;
} chk_info_t;

/////////////////////////////////////////////////

/* Global Value */
char             *prog         = NULL;
int               verbose      = 0;
int               rebalance    = 0;
dir_t             g_dir;
struct list_head  bad_chk_list;

/////////////////////////////////////////////////////

extern struct yfs_sb *yfs_sb ;


/* functions */

char *read_dir(char *d_name, off_t d_off);

int analyze_mds_jnl(char *pathname);

uint32_t get_mdp(uint32_t op, void *buf);

void __check_tree(char *dir_name);

int __check_file(const char *path);

int __check_chk(
                uint32_t chkno, chkid_t *chkid,
                uint32_t def_rep, const char *path);

void usage(void)
{
        printf("Usage:\n");
        printf("  %s [-hprv] [-j external_journal]\n", prog);
        printf("Options:\n");
        printf("  -h       show this message.\n");
        printf("  -r       relabance.\n");
}

/* The operation list, debug only */
char *operation[25] = {
        "UNKNOWN",    /* 0 */

        "GETATTR",
        "mkdir",
        "CHMOD",
        "rm",
        "mv",
        "rmdir",
        "OPENDIR",
        "LINK2UNIQUE",
        "SETXATTR",
        "GETXATTR",    /* 10 */

        "OPEN",
        "FSYNC",
        "touch",

        "CHKLOAD",
        "CHKGET",

        "READDIR",    /* 16 */

        "STATVFS",   /* 17 */
        "USERADD",
        "LOGIN",
        "PASSWD",
        "PING",

        "DISKJOIN",
        "DISKREPT",
        "DISKHB"
};

int analyze_mds_jnl(char *pathname)
{
        DIR           *dp;
        FILE          *fd;
        size_t         size;
        char           jnlname[MAX_BUF_LEN];
        char          *file_buf;
        char          *tmp_buf;
        uint32_t       mdp_op;
        uint32_t       mdp_len;
        struct dirent *dirp;
        struct stat    statbuf;

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

                if ((fd = fopen(jnlname, "rb")) == NULL)
                        ERROR_QUIT("Open %s error: %s\n", 
                                        jnlname, strerror(errno));

                if (lstat(jnlname, &statbuf) < 0) 
                        ERROR_QUIT("lstat %s error: %s\n",
                                        jnlname, strerror(errno));

                size = statbuf.st_size;
                if ((file_buf = (char*) malloc((size_t) size)) == NULL) 
                        ERROR_QUIT("Malloc error: %s\n", strerror(errno));

                tmp_buf = file_buf;

                DBG("file size is: %lu\n", (unsigned long)size);
                if (fread(file_buf, size, 1, fd) != 1) 
                        ERROR_QUIT("Read from %s Error\n", jnlname);

                /* start annlyze journal */
                DBG("Start analyze %s\n", jnlname);

                while(tmp_buf < file_buf + size) {
                        _memset((void *) &mdp_op, 0x0, 0x4);
                        _memcpy((void *) &mdp_op, (void *) tmp_buf, 0x4);

                        mdp_len = get_mdp(mdp_op, (void *)tmp_buf);       
                        tmp_buf += mdp_len;
                        //sleep(1);
                }

                free(file_buf);
        }

        return 0;
}

/*
 * Rebuild the directory tree
 */
uint32_t get_mdp(uint32_t op, void *buf)
{
        int  len;
        char command[MAX_BUF_LEN];

        DBG("%s ", operation[op]);

        switch (op) {
                case MDP_GETATTR:
                case MDP_UNLINK:
                case MDP_RMDIR:
                case MDP_OPENDIR: 
                        {
                                len = MDP_GETATTR_REQ_LEN((mdp_getattr_req_t *)buf);
                                DBG("%s\n", (char *)buf +
                                                sizeof(mdp_getattr_req_t));
                                sprintf(command, "%s %s%s > /dev/null", 
                                                operation[op],
                                                TREE_DIR,
                                                (char *)buf + 
                                                sizeof(mdp_getattr_req_t));
                                system(command);
                        }
                        break;
                case MDP_MKDIR:
                        {
                                len = MDP_MKDIR_REQ_LEN((mdp_mkdir_req_t *)buf);
                                DBG("%s\n", (char *)buf +
                                                sizeof(mdp_mkdir_req_t));
                                sprintf(command, 
                                                "%s -p %s%s > /dev/null", 
                                                operation[op],
                                                TREE_DIR,
                                                (char *)buf +
                                                sizeof(mdp_mkdir_req_t));
                                system(command);
                        }
                        break;
                case MDP_CHMOD:
                case MDP_CREATE:
                        {
                                len = MDP_MKDIR_REQ_LEN((mdp_mkdir_req_t *)buf);
                                DBG("%s\n", (char *)buf +
                                                sizeof(mdp_mkdir_req_t));
                                sprintf(command, "%s %s%s > /dev/null", 
                                                operation[op],
                                                TREE_DIR,
                                                (char *)buf + 
                                                sizeof(mdp_mkdir_req_t));
                                system(command);
                        }
                        break;
                case MDP_RENAME:
                        {
                                len = MDP_RENAME_REQ_LEN((mdp_rename_req_t *)buf);
                                DBG("%s ", (char *)buf +
                                                sizeof(mdp_rename_req_t));
                                DBG("%s \n", (char *)buf +
                                                ((mdp_rename_req_t *)buf)->from_len + 
                                                sizeof(mdp_rename_req_t));

                                sprintf(command, 
                                                "%s %s%s /dev/shm/mds%s > /dev/null", 
                                                operation[op],
                                                TREE_DIR,
                                                (char *)buf + 
                                                sizeof(mdp_rename_req_t),
                                                (char *)buf + 
                                                ((mdp_rename_req_t *)buf)->from_len +
                                                sizeof(mdp_rename_req_t));
                                system(command);
                        }
                        break;
                case MDP_FSYNC:
                        {
                                len = sizeof(mdp_fsync_req_t);
                                DBG("%s\n", (char *)buf + 
                                                sizeof(mdp_fsync_req_t));
                        }
                        break;
                default:
                        ERROR_QUIT("Warning: Unhandled op %d: %s\n",
                                        op, operation[op]);
                        break;
        }

        return len;
}

void __check_tree(char *dir_name)
{
        char             *tmp;
        dir_t            *tmp_del;
        char              d_name[MAX_BUF_LEN];
        off_t             d_offset;
        struct list_head *pos;
        struct list_head *q;

        _strcpy(d_name, dir_name);
        d_offset = 0;

        INIT_LIST_HEAD(&g_dir.list);

        while (srv_running) {
                tmp = read_dir(d_name, d_offset);
                if ((tmp == NULL) && list_empty(&g_dir.list)) 
                        break;
                else if (tmp == NULL) {
                        list_for_each_safe(pos, q, &g_dir.list) {
                                tmp_del = list_entry(pos, dir_t, list);
                                list_del(pos);

                                _strcpy(d_name, tmp_del->d_name);
                                d_offset = tmp_del->d_off;

                                yfree((void **)&tmp_del);
                                break;
                        }
                } else {
                        _strcpy(d_name, tmp);
                        d_offset = 0;

                        yfree((void **)&tmp);
                }
        }
}

/**
 * @retval NULL if file
 * @retval d_name if dir
 */
char *read_dir(char *d_name, off_t d_off)
{
        DIR           *dp;
        char          *ret;
        char           file[MAX_BUF_LEN];
        struct dirent *entry;
        struct stat    statbuf;
        dir_t         *tmp;
        int            len;

        len = _strlen(TREE_DIR);
        
        if ((dp = opendir(d_name)) == NULL) {
                fprintf(stderr, "open %s error: %s\n", 
                                d_name, strerror(errno));
                exit(1);
        }
        
        seekdir(dp, d_off);

        chdir(d_name);

        while ((entry = readdir(dp)) != NULL) {
                if (_strcmp(".", entry->d_name) == 0 || 
                    _strcmp("..", entry->d_name) == 0)
                        continue;

                lstat(entry->d_name, &statbuf);

                if (S_ISDIR(statbuf.st_mode)) {
                        tmp = malloc(sizeof(dir_t) + 
                                        _strlen(d_name) + 1);
                        if (tmp == NULL)
                                ERROR_QUIT("Malloc error\n");

                        _strcpy(tmp->d_name, d_name);
                        tmp->d_off = entry->d_off;
                        list_add(&tmp->list, &g_dir.list);

                        ret = malloc(strlen(d_name) + 1 + 
                                        _strlen(entry->d_name) + 1);
                        if (ret == NULL)
                                ERROR_QUIT("Malloc error\n");

                        _strcpy(ret, d_name);
                        strcat(ret, "/");
                        strcat(ret, entry->d_name);

                        goto label;
                }

                sprintf(file, "%s/%s", d_name, entry->d_name);

                __check_file(file + len);
        }

        chdir("..");
        closedir(dp);
         
        return NULL;

label:
        chdir("..");
        closedir(dp);

        return ret;
}

int __check_file(const char *path)
{
        int              ret;
        int              yfd;
        struct yfs_file *yf;
        uint32_t         chkno;
        chkid_t          chkid;
        int              def_rep;
        md_chk_t *mdchk;

        /* @res yfd */
        yfd = ly_open(path);
        if (yfd < 0) {
                ret = -yfd;
                GOTO(err_ret, ret);
        }

        yf = get_file(yfd);
        if (yf == NULL) {
                ret = ENOENT;
                GOTO(err_yfd, ret);
        }

        def_rep = yf->node->md->chkrep;

        DBG("  [%s]: mdsize %u filelen %llu chklen %u chkrep %u chknum %u\n",
            yf->node->path, yf->node->md->md_size,
            (unsigned long long)yf->node->md->file_len, yf->node->md->chk_len,
            yf->node->md->chkrep, yf->node->md->chknum);

        for (chkno = 0; chkno < yf->node->md->chknum; chkno++) {
                mdchk = &yf->node->md->chks[chkno];
                chkid.id = mdchk->chkid.id;
                chkid.version = mdchk->chkid.version;

                __check_chk(chkno, &chkid, def_rep, path);
        }

        ly_release(yfd);

        return 0;
err_yfd:
        ly_release(yfd);
err_ret:
        return ret;
}

/**
 * @param chkno
 */
int __check_chk(
                uint32_t chkno, 
                chkid_t *chkid,
                uint32_t def_rep, 
                const char *path)
{
        int               ret;
        struct yfs_chunk *chk;
        bad_chk_t        *bad_chk;

        /* @res chk */
        ret = ymalloc((void **)&chk, sizeof(struct yfs_chunk));
        if (ret)
                GOTO(err_ret, ret);

        ret = md_chkload(yfs_sbi, chk, chkid);
        if (ret)
                GOTO(err_chk, ret);

        if (chk->rep < def_rep) {
                ret = ymalloc((void **)&bad_chk, sizeof(bad_chk_t));
                if (ret)
                        GOTO(err_chk, ret);

                INIT_LIST_HEAD(&bad_chk->list);

                bad_chk->chk = chk;
                bad_chk->def_rep = def_rep;
                bad_chk->cur_rep = chk->rep;
                _strcpy(bad_chk->path, path);

                list_add_tail(&bad_chk->list, &bad_chk_list);

                return 0;
        } else if (chk->rep > def_rep) {
                /* XXX */
        } 

        yfree((void **)&chk);

        return 0;
err_chk:
        yfree((void **)&chk);
err_ret:
        return ret; 
}

/*
 * rebalance chunk
 */
int rebalance_chk(bad_chk_t *bad_chk)
{
        int ret, repno;
        uint32_t nr_clone, new_rep, i, j, buflen;
        struct yfs_chunk  chk;
        ynet_net_info_t  *info;
        char buf[MAX_BUF_LEN];
        info = (void *)buf;
        buflen = MAX_BUF_LEN;

        /* @pre */
        if (bad_chk->cur_rep >= bad_chk->def_rep)
                return 0;

        _memset(&chk, 0x0, sizeof(struct yfs_chunk));

        chk.chkid.id      = bad_chk->chk->chkid.id;
        chk.chkid.version = bad_chk->chk->chkid.version;
        chk.no            = bad_chk->chk->no;
        chk.chklen        = bad_chk->chk->chklen;

        new_rep = bad_chk->def_rep;

        DBUG("=== request new CDS...%s %d\n", bad_chk->path, bad_chk->def_rep);

        /* @res chk */
        ret = md_chkget(NULL, bad_chk->path, &chk); 
        if (ret || chk.rep < new_rep) {
                DERROR("***There are less than %d CDSs in CLUSTER***\n", new_rep);
                GOTO(err_ret, ret);
        }

        nr_clone = 0;

        for (i = 0; i < new_rep; ++i) { 
                for (j = 0; j < bad_chk->chk->rep; ++j) {
                        if (net_handle_cmp(&bad_chk->chk->nid[j], &chk.nid[i]) == 0) {
                                break;
                        }
                }
                
                /* got it */
                if (j == bad_chk->chk->rep) {
                        /* @res info */
                        ret = net_nid2info(info, &buflen, &chk.nid[i]);
                        if (ret)
                                GOTO(err_chk, ret);

                        DBUG("=== Clone to new CDS...\n");
                        repno = random() % bad_chk->chk->rep;

#if 0
                        ret = cdc_repclone(bad_chk->chk, repno, info);
                        if (ret) {

                        } else 
                                nr_clone++;
#endif
                        yfree((void **)&info);
                }
        }

        YASSERT(nr_clone == new_rep - bad_chk->chk->rep);

        yfree((void **)&chk.nid);

        return 0;
err_chk:
        yfree((void **)&chk.nid);
err_ret:
        return ret;
}

/////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
        int               ret;
        char              c_opt;
        char             *journal              = NULL;
        bad_chk_t        *bad_chk;
        char              command[MAX_BUF_LEN];
        struct list_head *pos;
        struct list_head *q;

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

        while ((c_opt = getopt(argc, argv, "hj:pvr")) > 0) {
                switch (c_opt) {
                        case 'h':
                                usage();
                                exit(1);
                                break;
                        case 'j':
                                journal = optarg;
                                break;
                        case 'r':
                                rebalance = 1;
                                break;
                        case 'p':
                                break;
                        case 'v':
                                verbose = 1;
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                usage();
                                exit(1);
                }
        }

        /* 
         * Remove all directories in /dev/shm/mds,
         * and rebuild them in the next step.
         */
        sprintf(command, "rm -fr %s/*", TREE_DIR);
        system(command);

        fprintf(stderr, "Start analyze journal ...\n");

        /* analyze mds journal and rebuild tree in "TREE_DIR" */
        if (journal != NULL)
                analyze_mds_jnl(journal);           
        else
                analyze_mds_jnl(DEFAULT_JNL_DIR);

        ret = conf_init(YFS_CONFIGURE_FILE);
        if (ret)
                exit(1);

        ret = ly_init_simple("yfsck");
        if (ret) 
                ERROR_QUIT("ly_init error: %s\n", strerror(ret));

        fprintf(stderr, "Start verify chunk ...\n");

        INIT_LIST_HEAD(&bad_chk_list);

        /* Checking chunk, record it's id & version if is incorrect */
        __check_tree(TREE_DIR);

        fprintf(stderr, "Check yfs complete ...\n");
        if (list_empty(&bad_chk_list))
                printf("All chunks are correct. ^_^\n");
        else {
                printf("Some chunks are incorrect. ~_~\n");
                printf("%-40s %-20s   def rep  cur rep\n", "path", "chunk");
        }

        /* print incorrect chunk */
        list_for_each_safe(pos, q, &bad_chk_list) {
                bad_chk = list_entry(pos, bad_chk_t, list);

                if (rebalance) {
                        rebalance_chk(bad_chk);
                } else {
                        printf("%-40s (%llu, %u) %8d %8d\n", 
                               bad_chk->path,
                               (unsigned long long)bad_chk->chk->chkid.id,
                               bad_chk->chk->chkid.version, bad_chk->def_rep,
                               bad_chk->cur_rep);
                }

                list_del(pos);
                yfree((void **)&bad_chk->chk);
                yfree((void **)&bad_chk);
        }

        (void) ly_destroy();

        return 0;
}
