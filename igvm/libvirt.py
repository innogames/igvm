"""igvm - libvirt

Copyright (c) 2018 InnoGames GmbH
"""

from fabric.api import env
from libvirt import open as libvirt_open, libvirtError
from os import path


_conns = {}


def get_virtconn(fqdn):
    # Unfortunately required for igvm intergration testing
    if 'user' in env:
        username = env['user'] + '@'
    else:
        username = ''

    scripts_dir = path.join(path.dirname(__file__), 'scripts')

    if fqdn not in _conns:
        url = 'qemu+ssh://{}{}/system?command={}/ssh_wrapper'.format(
            username, fqdn, scripts_dir
        )
        _conns[fqdn] = libvirt_open(url)
    return _conns[fqdn]


def close_virtconns():
    for fqdn in list(_conns.keys()):
        conn = _conns[fqdn]
        try:
            conn.close()
        except libvirtError:
            pass
        del _conns[fqdn]
