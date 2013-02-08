import libvirt

from fabric.api import puts

_conns = {}

def get_virtconn(host, hypervisor):
    if hypervisor != 'libvirt-xen':
        raise ValueError('Only libvirt-xen is supported for now')

    index = (hypervisor, host)
    if index not in _conns:
        url = 'xen+ssh://{0}/'.format(host)
        puts('Connecting to libvirt at ' + url)
        _conns[index] = libvirt.open(url)
    
    return _conns[index]

def close_virtconns():
    for conn in _conns.values():
        conn.close()
