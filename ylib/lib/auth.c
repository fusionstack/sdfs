

#include <sys/time.h>
#include <unistd.h>
#include <shadow.h>
#include <pwd.h>
#include <errno.h>

#define DBG_SUBSYS S_LIBYLIB

#include "sysutil.h"
#include "dbg.h"

#if 0
int chech_auth_shadow(const char *user, const char *passwd)
{
        int ret, days;
        struct passwd *pwd;
        struct spwd *spwd;
        char *crypted;
        uint32_t plen, clen;
        struct timeval tv;

        pwd = getpwnam(user);
        if (pwd == NULL) {
                ret = ENOENT;
                GOTO(err_ret, ret);
        }

        DBUG("user %s(%s)\n", pwd->pw_name, user);
        DBUG("passwd(%s) uid(%llu) gid(%llu) realname(%s) dir(%s) shell(%s)\n",
             pwd->pw_passwd, (unsigned long long)pwd->pw_uid,
             (unsigned long long)pwd->pw_gid, pwd->pw_gecos, pwd->pw_dir,
             pwd->pw_shell);

        spwd = getspnam(user);
        if (spwd == NULL) {
                crypted = crypt(passwd, pwd->pw_passwd);

                clen = _strlen(crypted);
                plen = _strlen(pwd->pw_passwd);

                if ((clen != plen)
                    || (_strcmp(crypted, pwd->pw_passwd) != 0)) {
                        ret = EAGAIN;
                        DERROR("crypted %u (%s) pwd %u (%s)\n", clen, crypted,
                               plen, pwd->pw_passwd);
                        GOTO(err_ret, ret);
                }
        }

        DBUG("user %s(%s)\n", spwd->sp_namp, user);
        DBUG("passwd(%s) 1st(%ld) min(%ld) max(%ld) wn(%ld) in(%ld) exp(%ld)\n",
             spwd->sp_pwdp, spwd->sp_lstchg, spwd->sp_min, spwd->sp_max,
             spwd->sp_warn, spwd->sp_inact, spwd->sp_expire);

        ret = _gettimeofday(&tv, NULL);
        if (ret == -1) {
                ret = errno;
                GOTO(err_ret, ret);
        }

        days = tv.tv_sec / (60 * 60 * 24);

        if (spwd->sp_expire > 0 && spwd->sp_expire < days) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        if (spwd->sp_lstchg > 0 && spwd->sp_max > 0
            && spwd->sp_lstchg + spwd->sp_max < days) {
                ret = EPERM;
                GOTO(err_ret, ret);
        }

        crypted = crypt(passwd, spwd->sp_pwdp);

        clen = _strlen(crypted);
        plen = _strlen(spwd->sp_pwdp);

        if ((clen != plen)
            || (_strcmp(crypted, spwd->sp_pwdp) != 0)) {
                ret = EAGAIN;
                DERROR("crypted %u (%s) pwd %u (%s)\n", clen, crypted,
                       plen, spwd->sp_pwdp);
                GOTO(err_ret, ret);
        }

        return 0;
err_ret:
        return ret;
}
#endif
