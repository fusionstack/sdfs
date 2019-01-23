#!/usr/bin/env python
# -*- coding:utf-8 -*-
default_config = """#!/bin/sh
# ctdb event script for Samba

[ -n "$CTDB_BASE" ] || CTDB_BASE=$(d=$(dirname "$0") ; cd -P "$d" ; dirname "$PWD")

. "${CTDB_BASE}/functions"

detect_init_style

case $CTDB_INIT_STYLE in
        suse)
                CTDB_SERVICE_SMB=${CTDB_SERVICE_SMB:-smb}
                CTDB_SERVICE_NMB=${CTDB_SERVICE_NMB:-nmb}
                ;;
        debian)
                CTDB_SERVICE_SMB=${CTDB_SERVICE_SMB:-smbd}
                CTDB_SERVICE_NMB=${CTDB_SERVICE_NMB:-nmbd}
                ;;
        *)
                # Use redhat style as default:
                CTDB_SERVICE_SMB=${CTDB_SERVICE_SMB:-smb}
                CTDB_SERVICE_NMB=${CTDB_SERVICE_NMB:-""}
                ;;
esac

# service_name is used by various functions
# shellcheck disable=SC2034
service_name="samba"

loadconfig

service_state_dir=$(ctdb_setup_service_state_dir) || exit $?

service_start ()
{
    # make sure samba is not already started
    service "$CTDB_SERVICE_SMB" stop > /dev/null 2>&1
    if [ -n "$CTDB_SERVICE_NMB" ] ; then
        service "$CTDB_SERVICE_NMB" stop > /dev/null 2>&1
    fi
    killall -0 -q smbd && {
        sleep 1
        # make absolutely sure samba is dead
        killall -q -9 smbd
    }
    killall -0 -q nmbd && {
        sleep 1
        # make absolutely sure samba is dead
        killall -q -9 nmbd
    }

    # start Samba service. Start it reniced, as under very heavy load
    # the number of smbd processes will mean that it leaves few cycles
    # for anything else
    net serverid wipe

    if [ -n "$CTDB_SERVICE_NMB" ] ; then
        nice_service "$CTDB_SERVICE_NMB" start || die "Failed to start nmbd"
    fi

    nice_service "$CTDB_SERVICE_SMB" start || die "Failed to start samba"
}

service_stop ()
{
    service "$CTDB_SERVICE_SMB" stop
    program_stack_traces "smbd" 5
    if [ -n "$CTDB_SERVICE_NMB" ] ; then
        service "$CTDB_SERVICE_NMB" stop
    fi
}

######################################################################
# Show the testparm output using a cached smb.conf to avoid delays due
# to registry access.

smbconf_cache="$service_state_dir/smb.conf.cache"

testparm_foreground_update ()
{
    _timeout="$1"

    # No need to remove these temporary files, since there are only 2
    # of them.
    _out="${smbconf_cache}.out"
    _err="${smbconf_cache}.err"

    timeout "$_timeout" testparm -v -s >"$_out" 2>"$_err"
    case $? in
        0) : ;;
        124)
            if [ -f "$smbconf_cache" ] ; then
                echo "WARNING: smb.conf cache update timed out - using old cache file"
                return 1
            else
                echo "ERROR: smb.conf cache create failed - testparm command timed out"
                exit 1
            fi
            ;;
        *)
            if [ -f "$smbconf_cache" ] ; then
                echo "WARNING: smb.conf cache update failed - using old cache file"
                cat "$_err"
                return 1
            else
                echo "ERROR: smb.conf cache create failed - testparm failed with:"
                cat "$_err"
                exit 1
            fi
    esac

    # Only using $$ here to avoid a collision.  This is written into
    # CTDB's own state directory so there is no real need for a secure
    # temporary file.
    _tmpfile="${smbconf_cache}.$$"
    # Patterns to exclude...
    _pat='^[[:space:]]+(registry[[:space:]]+shares|include|copy|winbind[[:space:]]+separator)[[:space:]]+='
    grep -Ev "$_pat" <"$_out" >"$_tmpfile"
    mv "$_tmpfile" "$smbconf_cache" # atomic

    return 0
}

testparm_background_update ()
{
    _timeout="$1"

    testparm_foreground_update "$_timeout" >/dev/null 2>&1 </dev/null &
}

testparm_cat ()
{
    testparm -s "$smbconf_cache" "$@" 2>/dev/null
}

list_samba_shares ()
{
    testparm_cat |
    sed -n -e 's@^[[:space:]]*path[[:space:]]*=[[:space:]]@@p' |
    sed -e 's/"//g'
}

list_samba_ports ()
{
    testparm_cat --parameter-name="smb ports" |
    sed -e 's@,@ @g'
}

###########################

is_ctdb_managed_service || exit 0

###########################

case "$1" in
startup)
        service_start
        ;;

shutdown)
        service_stop
        ;;

monitor)
        testparm_foreground_update 10
        ret=$?

        smb_ports="$CTDB_SAMBA_CHECK_PORTS"
        if [ -z "$smb_ports" ] ; then
            smb_ports=$(list_samba_ports)
            [ -n "$smb_ports" ] || die "Failed to set smb ports"
        fi
        # Intentionally unquoted multi-word value here
        # shellcheck disable=SC2086
        ctdb_check_tcp_ports $smb_ports || exit $?

        if [ "$CTDB_SAMBA_SKIP_SHARE_CHECK" != "yes" ] ; then
            list_samba_shares | ctdb_check_directories || exit $?
        fi

        if [ $ret -ne 0 ] ; then
            testparm_background_update 10
        fi
        ;;
esac

exit 0
"""
ctdb_base_path = "/usr/local/samba/etc/ctdb/"

def update_samba_config():
    with open(ctdb_base_path+"events.d/50.samba",mode="r") as fr:
        keywords = "startup)"
        smbd_cmd = "/usr/local/samba/sbin/smbd -D"
        nmbd_cmd = "/usr/local/samba/sbin/nmbd -D"
        content = fr.read()
        post = content.find(keywords)
        if post != -1:
            content = content[:post+len(keywords)]+"\n\t" +smbd_cmd+"\n\t"+nmbd_cmd + content[post + len(keywords) + 22:]
    with open(ctdb_base_path+"events.d/50.samba",mode="w") as fw:
        fw.write(content)

if __name__ == "__main__":
    with open(ctdb_base_path+"events.d/50.samba",mode="w") as fw:
        fw.write(default_config)
    update_samba_config()