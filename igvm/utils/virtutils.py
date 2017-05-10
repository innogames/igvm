from libvirt import open as libvirt_open, libvirtError


_conns = {}


def get_virtconn(host, hypervisor):
    if hypervisor != 'kvm':
        raise ValueError('Only kvm is supported for now')

    index = (hypervisor, host)
    if index not in _conns:
        url = 'qemu+ssh://{0}/system'.format(host)
        _conns[index] = libvirt_open(url)

    return _conns[index]


def close_virtconns():
    for conn_key in _conns.keys():
        conn = _conns[conn_key]
        try:
            conn.close()
        except libvirtError:
            pass
        del _conns[conn_key]
