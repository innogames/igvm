import os
import grp
import pwd

from fabric.api import cd, run
from fabric.contrib.files import append

def find_keys():

    user = pwd.getpwuid(os.getuid())[0]
    home_dirs = [(user, pwd.getpwnam(user).pw_dir)]
    home_dirs.append(('root', '/root'))

    keys = []
    for user, home_dir in home_dirs:
        try:
            with open(os.path.join(home_dir, '.ssh', 'authorized_keys')) as f:
                lines = [line.strip() for line in f.readlines()]
                keys += ['{0} ({1})'.format(key, user) for key in lines if key]
        except IOError:
            pass

    return keys

def create_authorized_keys(target_dir):
    keys = find_keys()
    key_entries = '\n{0}\n'.format('\n'.join(keys))
    with cd(target_dir):
        run('mkdir -p root/.ssh')
        append('root/.ssh/authorized_keys', key_entries)
