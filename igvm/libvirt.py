"""igvm - libvirt

Copyright (c) 2018 InnoGames GmbH
"""

from libvirt import open as libvirt_open, libvirtError
from os import path, environ

from igvm.utils import get_ssh_config

_conns = {}


def get_virtconn(fqdn):
    if 'IGVM_SSH_USER' in environ:
        username = environ.get('IGVM_SSH_USER') + '@'
    else:
        ssh_config = get_ssh_config(fqdn)
        if 'user' in ssh_config:
            username = ssh_config['user'] + '@'
        else:
            username = ''

    scripts_dir = path.join(path.dirname(__file__), 'scripts')

    if fqdn not in _conns:
        url = (
            'qemu+ssh://{}{}/system?'
            'socket=/var/run/libvirt/libvirt-sock&'
            'command={}/ssh_wrapper'
        ).format(
            username, fqdn, scripts_dir
        )
        _conns[fqdn] = libvirt_open(url)
    return _conns[fqdn]


def close_virtconn(fqdn):
    if fqdn not in _conns:
        return

    conn = _conns[fqdn]
    try:
        conn.close()
    except libvirtError:
        pass

    del _conns[fqdn]


def close_virtconns():
    for fqdn in list(_conns.keys()):
        close_virtconn(fqdn)
