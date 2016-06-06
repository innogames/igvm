import libvirt

from fabric.api import puts

_conns = {}

def get_virtconn(host, hypervisor):
    if hypervisor != 'kvm':
        raise ValueError('Only kvm is supported for now')

    index = (hypervisor, host)
    if index not in _conns:
        url = 'qemu+ssh://{0}/system'.format(host)
        puts('Connecting to libvirt at ' + url)
        _conns[index] = libvirt.open(url)

    return _conns[index]

def close_virtconns():
    for conn_key in _conns.keys():
        conn = _conns[conn_key]
        try:
            conn.close()
        except libvirt.libvirtError:
            pass
        del _conns[conn_key]
