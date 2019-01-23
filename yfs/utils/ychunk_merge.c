#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "yfscds_conf.h"
#include "chk_meta.h"

#define LINE_MAX_LEN    2048 
#define CHK_HEAD_OFF    (sizeof(chkmeta2_t) + YFS_CDS_CRC_LEN)        
#define BUFSIZE         2048

int merge_file(const char *, FILE *);

int main(int argc, char *argv[])
{
        int ret;
        FILE *fp_list, *fp_target;
        char line[LINE_MAX_LEN];

        if (argc != 3) {
                fprintf(stderr, "Usage: %s list target.\n", argv[0]);
                goto __func__err_exit;
        }

        fp_list = fopen(argv[1], "r");
        if (fp_list == NULL) {
                perror("");
                goto __func__err_exit;
        }

        fp_target = fopen(argv[2], "w");
        if (fp_target == NULL) {
                perror("");
                goto __func__err_exit;
        }

        while (fgets(line, LINE_MAX_LEN, fp_list) != NULL) {
                printf("%s", line);
                ret = merge_file(line, fp_target);
                if (ret == -1) 
                        goto __func__err_exit;
        }
        fclose(fp_list);
        fclose(fp_target);
        
        return 0;

__func__err_exit:
        return -1;
}


int merge_file(const char *line, FILE *fp_target)
{
        FILE *fp_chunk;
        char buf[BUFSIZE], file[LINE_MAX_LEN];
        unsigned int i, j;
        int ret, len;
        
        for (i = 0, j = 0; i < _strlen(line); ++i) {
                if (line[i] == '\n') 
                        continue;
                file[j++] = line[i];
        }
        file[j] = '\0';

        fp_chunk = fopen(file, "r");
        if (fp_chunk == NULL) {
                perror("");
                goto __func__err_exit;
        }

        ret = fseek(fp_chunk, CHK_HEAD_OFF, SEEK_SET);
        if (ret == -1) {
                perror("");
                goto __func__err_exit;
        }
        
        while (srv_running) { 
                len = fread(buf, sizeof(char), sizeof(buf), fp_chunk);
                if (len == 0)
                        break;

                ret = fwrite(buf, sizeof(char), len, fp_target);
		
		if (ret != len) {
                        perror("");
                        goto __func__err_exit;
                }
        }
        fclose(fp_chunk);

        return 0;

__func__err_exit:
        return -1;
}



        
