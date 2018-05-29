"""igvm - libvirt

Copyright (c) 2018 InnoGames GmbH
"""

from libvirt import open as libvirt_open, libvirtError

_conns = {}


def get_virtconn(fqdn):
    if fqdn not in _conns:
        url = 'qemu+ssh://{}{}/system'.format(username, fqdn)
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
