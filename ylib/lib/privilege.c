#include <stdlib.h>
#include <pwd.h>
#include <grp.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "privilege.h"
#include "dbg.h"

int grant(const char *username, const char* group)
{
	struct passwd *pwd;
	struct group *grp;
	uid_t user_id;
	gid_t group_id;
	int ret;
	
	if(_strcmp(username, "") == 0 || _strcmp(group, "") == 0) {
		ret = EINVAL;
		GOTO(err_ret, ret);
	}

	if(((pwd = getpwnam(username)) == NULL) || ((grp = getgrnam(group)) == NULL)) {
		ret = ENOENT;
		GOTO(err_ret, ret);
	}

	user_id = pwd->pw_uid;
	group_id = grp->gr_gid;

	if(setgid(group_id) || setegid(group_id) || setuid(user_id) || seteuid(user_id)) {
		ret = errno;
		GOTO(err_ret, ret);
	}

	return 0;	
err_ret:
	return ret;
}
