from fabric.api import run, hide

def get_meminfo():
    """Return a dictionary with the values of /proc/meminfo.

    All values are in bytes, if they are not an amount.

    Most important keys: MemTotal, MemFree, Buffers
    """
    with hide('everything'):
        meminfo = run('cat /proc/meminfo')
    info = {}
    for line in meminfo.splitlines():
        parts = line.strip().split()
        if len(parts) == 3:
            key = parts[0][:-1]
            value = int(parts[1])
            if parts[2] == 'kB':
                value *= 1024
            else:
                raise ValueError('Unknown unit ' + parts[2])
            info[key] = value
        elif len(parts) == 2:
            key = parts[0][:-1]
            value = int(parts[1])
            info[key] = value
    return info

def get_cpuinfo():
    """Return a list of dictionaries with info about the CPUs."""
    with hide('everything'):
        cpuinfo = run('cat /proc/cpuinfo')
    info = []
    cpu = {}
    for line in cpuinfo.splitlines():
        line = line.strip()
        if not line:
            info.append(cpu)
            cpu = {}
            continue

        parts = [part.strip() for part in line.split(':')]
        if len(parts) != 2:
            raise ValueError('Invalid line on /proc/cpuinfo')

        key = parts[0]
        value = parts[1]
        
        if key == 'flags':
            value = set(value.split())

        cpu[key] = value

    return info
