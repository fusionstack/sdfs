

#include <unistd.h>
#include <termios.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h> 

#include "configure.h"
#include "sdfs_lib.h"
#include "chk_proto.h"


#define ECHOFLAGS (ECHO | ECHOE | ECHOK | ECHONL)

int set_disp_mode(int fd, int option)
{
        int ret;
        struct termios term;

        if(tcgetattr(fd, &term) == -1){
                fprintf(stderr, "Cannot get the attribution of the terminal\n");
                return 1;
        }

        if(option)
                term.c_lflag |= ECHOFLAGS;
        else
                term.c_lflag &= ~ECHOFLAGS;

        ret = tcsetattr(fd, TCSAFLUSH, &term);
        if(ret == -1 && ret == EINTR){
                fprintf(stderr, "Cannot set the attribution of the terminal\n");
                return 1;
        }

        return 0;
}

int getpasswd(char *passwd, int size)
{
        int c;
        int n = 0;

        do {
                c = getchar();
                if (c != '\n')
                        passwd[n++] = c;
        } while (c != '\n' && n < (size - 1));

        passwd[n] = '\0';

        return n;
}

int main(int argc, char *argv[])
{
        int ret, i;
        char *prog;
        char cur_passwd[MAX_PASSWD_LEN], user[MAX_PATH_LEN];
        char new_passwd1[MAX_PASSWD_LEN], new_passwd2[MAX_PASSWD_LEN];

        prog = strrchr(argv[0], '/');
        if (prog)
                prog++;
        else
                prog = argv[0];

	if (argc == 2) {
		strncpy(user, (char *)argv[1], _strlen(argv[1]));
		user[strlen(argv[1])] = '\0';
		printf("Changing password for %s\n", user);

		set_disp_mode(STDIN_FILENO, 0);

		printf("(current) password:");
		getpasswd(cur_passwd, MAX_PASSWD_LEN);

		for (i = 0; i < 3; i++) {
			printf("\nNew password:");
			getpasswd(new_passwd1, MAX_PASSWD_LEN);

			if (strlen(new_passwd1) < MIN_PASSWD_LEN) {
				fprintf(stderr,
						"\nBAD PASSWORD: it is WAY too short\n");
				continue;
			} else
				break;
		}

		set_disp_mode(STDIN_FILENO, 1);

		if (i == 3) {
			fprintf(stderr, "\nAuthentication token manipulation error\n");
			exit(1);
		}

		set_disp_mode(STDIN_FILENO, 0);

		for (i = 0; i < 2; i++) {
			printf("\nRetype new password:");
			getpasswd(new_passwd2, MAX_PASSWD_LEN);

			if (strlen(new_passwd2) < MIN_PASSWD_LEN) {
				fprintf(stderr,
						"\nBAD PASSWORD: it is WAY too short\n");
				continue;
			} else
				break;
		}

		set_disp_mode(STDIN_FILENO, 1);

		if (i == 2) {
			fprintf(stderr, "\nAuthentication token manipulation error\n");
			exit(1);
		}

		if (strncmp(new_passwd1, new_passwd2, sizeof(new_passwd1))){
			fprintf(stderr, "\nSorry, passwords do not match.\n");
			exit(1);
		}

                ret = conf_init(YFS_CONFIGURE_FILE);
                if (ret)
                        exit(1);

		ret = ly_init_simple("ypasswd");
		if (ret) {
			fprintf(stderr, "ly_init() %s\n", strerror(ret));
			exit(1);
		}

		ret = ly_passwd(user, cur_passwd, new_passwd1);
		if(ret) {
			fprintf(stderr, " change password failed ly_passwd() %s\n",
					strerror(ret));
			exit (1);
		}

	} else if(argc == 4) {
		ret = ly_init_simple("ypasswd");
		if (ret) {
			fprintf(stderr, "ly_init() %s\n", strerror(ret));
			exit(1);
		}

		strncpy(user, (char *)argv[1], _strlen(argv[1]));
		user[strlen(argv[1])] = '\0';

		strncpy(cur_passwd, (char *)argv[2], _strlen(argv[2]));
		cur_passwd[strlen(argv[2])] = '\0';

		strncpy(new_passwd1, (char *)argv[3], _strlen(argv[3]));
		new_passwd1[strlen(argv[3])] = '\0';

		if (strlen(new_passwd1) < MIN_PASSWD_LEN) {
			fprintf(stderr,
					"\nBAD PASSWORD: it is WAY too short\n");
			exit(1);
		}

		ret = ly_passwd(user, cur_passwd, new_passwd1);
		if(ret) {
			fprintf(stderr, " change password failed ly_passwd() %s\n",
					strerror(ret));
			exit (1);
		}

	} else {
                fprintf(stderr, 
		"Usage:\n%s <username>  or \n%s <username> <old_password> <new_password>\n"
		,prog, prog);
                exit(1);
        }
	
        ret = ly_destroy();
        if (ret) {
                fprintf(stderr, "ly_destroy() %s\n", strerror(ret));
                exit(1);
        }

        printf("%s: all authentication tokens updated successfully.\n", prog);

        return 0;
}
