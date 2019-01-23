#include <iostream>
#include <string>
#include <vector>

// #include <ncurses.h>

#include "yutils.h"
#include "ycommands.h"

#if defined(HAVE_READLINE) && HAVE_READLINE==1
# include <readline/readline.h>
# include <readline/history.h>
#else
# define readline(p) local_getline(p,stdin)
# define add_history(X)
# define read_history(X)
# define write_history(X)
# define stifle_history(X)
#endif

//

#define GROUP "group"

//
/* A structure which contains information on the g_commands this program
   can understand. */
typedef struct {
        const char *name;               /* User printable name of the function. */
        const char *doc;                /* Documentation for this function.  */
        //rl_icpfunc_t *func;           /* Function to call to do the job. */
} COMMAND;

COMMAND g_commands[] = {
        {GROUP,      "Global"},
        {"?",        "help [command]"},
        {"help",     "help [command]"},
        {"version",  "version"},
        {"exit",     "exit"},
        {"quit",     "quit"},
        {"ping",     "ping"},
        {"recycle",  "recycle"},
        {"rebuild_cdsjnl", "rebuild_cdsjnl"},
        {"setopt",   "setopt"},

        {GROUP,      "Stat"},
        {"stat",     "stat fs\n"
                     "                    stat log\n"
                     "                    stat file <file>\n"
                     "                    stat chunk [--clear] cn chkid/index"},

        {GROUP,      "Logical Volume Management"},
        {"lvm",      "lvm create <volname> [size:0]\n"
                     "                    lvm list"},

        {GROUP,      "Directory"},
        {"ls",       "ls <path>" },
        {"mkdir",    "mkdir <path>"},
        {"rmdir",    "rmdir <path>"},

        {GROUP,      "File"},
        {"cp",       "cp <src> <dest>" },
        {"mv",       "mv <src> <dest>" },
        {"rm",       "rm <file>" },
        //{"chmod",    "help" },
        //{"truncate", "help" },
        //
        {GROUP,      "Chunk"},
        {"rep",      "rep"},
        {"repcheck", "repcheck"},
        {"chkmerge", "chunk merge"},

        {GROUP,      "Misc"},
        {"login",    "login" },
        {"user",     "user" },
        {"password", "password" },

        { (const char *)NULL, (const char *)NULL},
        //{ "stat", com_stat, "Stat"},
};

static uint64_t str2volsize(const char *arg)
{
        char strSize[MAX_NAME_LEN], unit;
        uint64_t size;

        strcpy(strSize, arg);
        unit = strSize[strlen(strSize) - 1];
        strSize[strlen(strSize) - 1] = '\0';
        size = atol(strSize);

        if (size < 10) {
                fprintf(stderr, "error:min size is 10G\n");
                return -1;
        }

        if (unit == 'G') {
                size *= (1024 * 1024 * 1024);
        } else {
                fprintf(stderr, "error:unit is G\n");
                return -1;
        }

        return size;
}

/* -------------------------------------------------------- */
const COMMAND *find_command(const char *cmd, int list_index = 0) {
        int len;
        const COMMAND *thecmd;

        len = strlen(cmd);
        while (1) {
                thecmd = &g_commands[list_index];
                if (!thecmd->name)
                        break;

                list_index++;
                if (strcmp(thecmd->name, GROUP) == 0)
                        continue;

                if (strncmp (thecmd->name, cmd, len) == 0) {
                        return thecmd;
                }
        }

        return NULL;
}

void usage() {
        const COMMAND *thecmd;
        int idx = 0;

        while (1) {
                thecmd = &g_commands[idx];
                if (!thecmd->name)
                        break;

                idx++;
                if (strcmp(thecmd->name, GROUP) == 0) {
                        printf("=== %s ===\n", thecmd->doc);
                } else {
                        printf("%16s -- %s\n", thecmd->name, thecmd->doc);
                }
        }
}

void help(const char *cmd) {
        const COMMAND *thecmd;

        thecmd = find_command(cmd);
        if (thecmd) {
                printf("%16s -- %s\n", thecmd->name, thecmd->doc);
        } else {
                fprintf(stderr, "Not implemented: %s\n", cmd);
        }
}

int check_args(StringVector & args, unsigned int len, bool showHelp=true) {
        assert(!args.empty());
        if (args.size() < len) {
                if (showHelp) help(args[0].c_str());
                return 1;
        }
        return 0;
}

#define CHECK_ARGS(args, len)                   \
        do {                                    \
                if (check_args(args, len) != 0) \
                return 0;                       \
        } while(0)

#define CHECK_ARGS_NOHELP(args, len)            \
        do {                                    \
                if (check_args(args, len, false) != 0) \
                return 0;                       \
        } while(0)

/* -------------------------------------------------------- */
int execute_line(char *line) {
        StringVector args;
        std::string cmd;

        split(line, args);
        if (args.empty())
                return 0;

        cmd = args[0];
        if (cmd == "help" || cmd == "?") {
                if (args.size() == 1)
                        usage();
                CHECK_ARGS_NOHELP(args, 2);
                help(args[1].c_str());
        } else if (cmd == "quit" || cmd == "exit") {
                return 1;
        } else if (cmd == "ping") {
                cmd_ping();
        } else if (cmd == "lvm") {
                CHECK_ARGS(args, 2);
                if (args[1] == "create") {
                        CHECK_ARGS(args, 3);
                        uint64_t size = 0;
                        if (args.size() > 3)
                                size = str2volsize(args[3].c_str());
                        cmd_lvm_create(args[2].c_str(), size);
                } else if (args[1] == "list") {
                        cmd_lvm_list();
                } else {
                        help(line);
                }
        } else if (cmd == "mkdir") {
                CHECK_ARGS(args, 2);
                cmd_mkdir(args[1].c_str());
        } else if (cmd == "rmdir") {
                CHECK_ARGS(args, 2);
                cmd_rmdir(args[1].c_str());
        } else if (cmd == "ls") {
                CHECK_ARGS(args, 2);
                cmd_ls(args[1].c_str());
        } else if (cmd == "cp") {
                CHECK_ARGS(args, 3);
                cmd_cp(args[1].c_str(), args[2].c_str());
        } else if (cmd == "stat") {
                CHECK_ARGS(args, 2);
                std::string subcmd = args[1];
                if (subcmd == "fs") {
                } else if (subcmd == "file") {
                        CHECK_ARGS(args, 3);
                        cmd_stat_file(args[2].c_str());
                } else if (subcmd == "chunk") {
                        CHECK_ARGS(args, 3);
                        if (args[2] == "--clear") {
                                CHECK_ARGS(args, 5);
                                int cn = atoi(args[3].c_str());
                                cmd_stat_chunk(cn, args[4].c_str(), 1);
                        } else {
                                CHECK_ARGS(args, 4);
                                int cn = atoi(args[2].c_str());
                                cmd_stat_chunk(cn, args[3].c_str(), 0);
                        }
                } else if (subcmd == "log") {
                        help(line);

                } else {
                        help(cmd.c_str());
                }
        } else {
                help(line);
        }

        return 0;
}

static void interrupt_handler(int NotUsed) {
        int seenInterrupt;

        seenInterrupt = 1;
        std::cout << "interrupt_handler" << std::endl;
}

static char *line_read = (char *)NULL;

char *command_generator PARAMS((const char *, int));

/* Generator function for command completion.  STATE lets us know whether
   to start from scratch; without any state (i.e. STATE == 0), then we
   start at the top of the list. */
char *
command_generator (const char *text, int state) {
        static int list_index, len;
        const char *name;

        /* If this is a new word to complete, initialize now.  This includes
           saving the length of TEXT for efficiency, and initializing the index
           variable to 0. */
        if (!state) {
                list_index = 0;
                len = strlen (text);
        }

        /* Return the next name which partially matches from the command list. */
        while ((name = g_commands[list_index].name)) {
                list_index++;
                if (strcmp(name, GROUP) == 0)
                        continue;

                if (strncmp (name, text, len) == 0)
                        return (strdup(name));
        }

        /* If no names matched, then return NULL. */
        return ((char *)NULL);
}

/* Attempt to complete on the contents of TEXT.  START and END bound the
   region of rl_line_buffer that contains the word to complete.  TEXT is
   the word to complete.  We can use the entire contents of rl_line_buffer
   in case we want to do some simple parsing.  Return the array of matches,
   or NULL if there aren't any. */
char **
yshell_completion (const char *text, int start, int end) {
        char **matches;

        matches = (char **)NULL;

        /* If this word is at the start of the line, then it is a command
           to complete.  Otherwise it is the name of a file in the current
           directory. */
        if (start == 0)
                matches = rl_completion_matches (text, command_generator);

        return (matches);
}

/* Tell the GNU Readline library how to complete.  We want to try to complete
   on command names if this is the first word in the line, or on filenames
   if not. */
void rl_init () {
        /* Allow conditional parsing of the ~/.inputrc file. */
        rl_readline_name = "yshell";

        /* Tell the completer that we want a crack first. */
        rl_attempted_completion_function = yshell_completion;
}

char * rl_gets() {
        /* If the buffer has already been allocated,
           return the memory to the free pool. */
        if (line_read) {
                free (line_read);
                line_read = (char *)NULL;
        }

        /* Get a line from the user. */
        line_read = readline ("yshell> ");

        /* If the line has any text in it,
           save it on the history. */
        if (line_read && *line_read)
                add_history (line_read);

        return (line_read);
}

static int yfs_inited = 0;
void yfs_init() {
        int ret;

        if (!yfs_inited) {
                ret = conf_init(YFS_CONFIGURE_FILE);
                if (ret) {
                        fprintf(stderr, "conf_init() %s\n", strerror(ret));
                        exit(1);
                }

                ret = ly_init_simple("yshell");
                if (ret) {
                        fprintf(stderr, "ly_init() %s\n", strerror(ret));
                        exit(1);
                }
                yfs_inited = 1;
        }
}

void yfs_destroy() {
        if (yfs_inited) {
                ly_destroy();
        }
}

int main(int argc, char *argv[]) {
        int ret;
        char c_opt;

        //signal(SIGINT, interrupt_handler);

        /*
           initscr();

           printw("Hello, world!");
           refresh();
           getch();
           endwin();

           return 0;
           */

        dinfo_off();

        while ((c_opt = getopt(argc, argv, "v")) > 0) {
                switch (c_opt) {
                        case 'v':
                                break;
                        default:
                                fprintf(stderr, "Hoops, wrong op got!\n");
                                exit(1);
                }
        }

        yfs_init();

        int interactive_mode = 1;
        if (interactive_mode) {
                usage();

                rl_init (); /* Bind our completer. */

                char *line, *s;
                while (1) {
                        //std::cout << "yshell> ";
                        //std::cin.getline(line, 256);
                        line = rl_gets();
                        if (!line)
                                continue;

                        s = stripwhite (line);
                        ret = execute_line(s);
                        if (ret) {
                                printf("Bye\n");
                                break;
                        }
                }
        } else {
                // argv
        }

        yfs_destroy();

        return 0;
}
