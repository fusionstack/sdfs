#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>

#include "functest.h"
#include "iotest.h"

#define ARR_SIZE 5

typedef enum _bool {
        FALSE,
        TRUE
} bool;

// show help message and exit
void usage(const char *);

typedef void (*test_func)(const char *);


int main(int argc, char *argv[])
{
        int     c_opt;
        int     concurrent = 1, size = 1024;
        char    *sz_str, unit;
        char    test_path[PATH_MAX];
        bool    bFunc = FALSE, bRead = FALSE, bWrite = FALSE;

        // make sure have root rights to write log
        if (getuid() != 0) {
                fprintf(stderr, "Please run this program with root.\n");
                EXIT(1);
        }

        // test function array which run randomly
        test_func test_func_arr[ARR_SIZE] = {
                test_dir,
		test_mv,
                test_cp,
                test_split,
                // test_longname,
		test_file
        };

        memset(test_path, 0, PATH_MAX);
        srand((unsigned)time(0));

        // read command line arguments
        while ((c_opt = getopt(argc, argv, "c:d:hs:t:")) != -1)
                switch (c_opt) {
                case 'c':
                        if ((concurrent = atoi(optarg)) < 1)
                                usage(argv[0]);
                        break;

                case 'd':
                        strncpy(test_path, optarg, sizeof(test_path) - 1);
                        test_path[sizeof(test_path) - 1] = '\0';
                        break;

                case 'h':
                        usage(argv[0]);
                        break;

                case 's':
                        unit = optarg[strlen(optarg) - 1];
                        sz_str = (char *)malloc(sizeof(char) * strlen(optarg));
                        strncpy(sz_str, optarg, strlen(optarg) - 1);
                        sz_str[strlen(optarg) - 1] = '\0';

                        switch (unit) {
                        case 'b':
                        case 'B':
                                // make the value of size in range 1024 to INI_MAX
                                if ((size = atoi(sz_str)) < 1)
                                        usage(argv[0]);
                                else if (size < 1024)
                                        usage(argv[0]);
                                break;
                        case 'k':
                        case 'K':
                                // make the value of size in range 1024 to INI_MAX
                                if ((size = atoi(sz_str)) < 1)
                                        usage(argv[0]);
                                else if (size > INT_MAX / 1024)
                                        usage(argv[0]);
                                else
                                        size *= 1024;
                                break;
                        case 'm':
                        case 'M':
                                // make the value of size in range 1024 to INI_MAX
                                if ((size = atoi(sz_str)) < 1)
                                        usage(argv[0]);
                                else if (size > INT_MAX / (1024 * 1024))
                                        usage(argv[0]);
                                else
                                        size *= 1024 * 1024;
                                break;
                        default:
                                usage(argv[0]);
                        }
                        break;

                case 't':
                        if (strcmp("func", optarg) == 0)
                                bFunc = TRUE;
                        else if (strcmp("read", optarg) == 0)
                                bRead = TRUE;
                        else if (strcmp("write", optarg) == 0)
                                bWrite = TRUE;
                        else
                                usage(argv[0]);
                        break;

                default:
                        usage(argv[0]);
                }

        // value of -d and -t must be set
        if (!((test_path && *test_path) && (bFunc || bRead || bWrite)))
                usage(argv[0]);

        printf("[ CONCURRENT: %d    DIR: %s    SIZE: %d    MODE: ", concurrent, test_path, size);
        if (bFunc) {
                printf("func ]\n");
                while (1) {
                       test_func_arr[rand() % (ARR_SIZE)](test_path);
                }
        } else if (bRead) {
                printf("read ]\n");
                read_test(test_path);
        } else if (bWrite) {
                printf("write ]\n");
                write_test(test_path, concurrent, size);
        }

        return 0;
}


void usage(const char *soft)
{
        printf("Usage: %s [options] --args\n"
                        "Options:\n "
                        "\t-c CONCURRENT \tSet concurrent of write, valid only when -t is set to \"write\".\n"
                        "\t\t\tDefault value: 1\tMinimum: 1\tMaximun: 2147483647.\n"
                        "\t-d DIR \t\tSet the directory to test.\n"
                        "\t-h \t\tShow this help message and exit.\n"
                        "\t-s SIZE \tSet maximal size of write, valid only when -t is set to \"write\".\n"
                        "\t\t\tExamples: 512b 512B 1k 1K 1m 1M.\n"
                        "\t\t\tDefault value: 1k\tMinimum: 1k\tMaximum: 2147483647b\n"
                        "\t-t TEST \tSet test mode:\"func\", \"read\", \"write\".\n"
                        , soft);

        printf("Describe:\n"
                        "Program will check the md5 value of test files, if"
                        " an error occurred, a warning\nwill be written into"
                        " the file \"/var/log/ynfs_functest.log\".\n");
        EXIT(1);
}
