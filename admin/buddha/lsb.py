import subprocess
import platform

def check_lsb_release():
    """
    Verify if lsb_release command is available
    """
    args = [ 'which', 'lsb_release', ]
    process = subprocess.Popen(
        args=args,
        stdout=subprocess.PIPE,
        )
    lsb_release_path, _ = process.communicate()
    ret = process.wait()
    if ret != 0:
        raise RuntimeError('The lsb_release command was not found on remote host.  Please install the lsb-release package.')

def lsb_release():
    """
    Get LSB release information from platform.

    Returns truple with distro, release and codename. 
    """
    distro, release, codename = platform.dist()
    if distro == 'centos':
        distro = 'CentOS'
    return (str(distro).rstrip(), str(release).rstrip(), str(codename).rstrip())

def choose_init(distro, codename):
    """
    Select a init system for a given distribution.

    Returns the name of a init system (upstart, sysvinit ...).
    """
    if distro == 'Ubuntu':
        return 'upstart'
    return 'sysvinit'

if __name__ == '__main__':
    lsb_release = lsb_release()
    print lsb_release
    print choose_init('Ubuntu', 400)
