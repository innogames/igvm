"""igvm - Settings

Copyright (c) 2018 InnoGames GmbH
"""

from os import environ
from sys import stdout

from libvirt import (
    VIR_MIGRATE_PEER2PEER,
    VIR_MIGRATE_TUNNELLED,
)

from igvm.hypervisor_preferences import (
    HashDifference,
    HypervisorAttributeValue,
    HypervisorAttributeValueLimit,
    InsufficientResource,
    OtherVMs,
    OverAllocation,
)

COMMON_FABRIC_SETTINGS = dict(
    disable_known_hosts=True,
    use_ssh_config=True,
    always_use_pty=stdout.isatty(),
    forward_agent=True,
    shell='/bin/sh -c',
    timeout=5,
    connection_attempts=1,
    remote_interrupt=True,
)

VG_NAME = 'xen-data'
# Reserved pool space on Hypervisor
# TODO: this could be a percent value, at least for ZFS.
RESERVED_DISK = {
    'logical': 5.0,
    'zfs': 2 * 1024,
 }

# Reserved memory for host OS in MiB
HOST_RESERVED_MEMORY = {
    'logical': 2 * 1024,
    'zfs': 8 * 1024,
}

VM_OVERHEAD_MEMORY = 50


# Default max number of CPUs, unless the hypervisor has fewer cores or num_cpu
# is larger than this value.
KVM_DEFAULT_MAX_CPUS = 24


# Mapping to determine the libvirt CPU model based on serveradmin hw_model
KVM_HWMODEL_TO_CPUMODEL = {
    'Nehalem': ['Dell_R510', 'Dell_M610', 'Dell_M710'],
    'SandyBridge': [
        'Dell_R320',
        'Dell_M620', 'Dell_M630', 'Dell_M640',
        'Dell_R620', 'Dell_R640',
    ],
}


# There are various combinations of source and target HVs which come
# with their own bugs and must be addressed separately.
MIGRATE_CONFIG = {
    ('jessie', 'jessie'): {
        # Using p2p migrations on Jessie causes qemu process to allocate
        # as much memory as disk size on source HV.
        'uri': 'qemu+ssh://{destination}/system',
        'flags': 0,
    },
    ('stretch', 'stretch'): {
        # Live migration works only via p2p on Stretch. See Debian bug #796122.
        'uri': 'qemu+tls://{destination}/system',
        'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TUNNELLED,
    },
    ('stretch', 'jessie'): {
        # Jessie can still correctly *receive* p2p migration.
        'uri': 'qemu+tls://{destination}/system',
        'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TUNNELLED,
    },
    # Jessie to Stretch is unsupported because VM after migration looses
    # access to disk. After kernel reboots (not by panic, maybe by watchdog?)
    # it works fine again.
}

# Arbitrarily chosen MAC address prefix with U/L bit set
# It will be padded with the last three octets of the internal IP address.
MAC_ADDRESS_PREFIX = (0xCA, 0xFE, 0x01)

try:
    IGVM_IMAGE_URL = environ['IGVM_IMAGE_URL']
    IGVM_IMAGE_MD5_URL = IGVM_IMAGE_URL + '.md5'
except KeyError:
    print('Please set the IGVM_IMAGE_URL environment variable')
    raise

IMAGE_PATH = '/tmp'

HYPERVISOR_ATTRIBUTES = [
    'cpu_util_pct',
    'cpu_util_vm_pct',
    'hardware_model',
    'hostname',
    'igvm_locked',
    'intern_ip',
    'iops_avg',
    'libvirt_memory_total_gib',
    'libvirt_memory_used_gib',
    'libvirt_pool_total_gib',
    'libvirt_pool_used_gib',
    'num_cpu',
    'os',
    'state',
    {
        'vlan_networks': [
            'hostname',
            'intern_ip',
            'vlan_tag',
        ],
    },
    {
        'vms': [
            'disk_size_gib',
            'environment',
            'function',
            'game_market',
            'game_type',
            'game_world',
            'hostname',
            'memory',
            'num_cpu',
            'project',
        ],
    },
]

VM_ATTRIBUTES = [
    'disk_size_gib',
    'environment',
    'function',
    'game_market',
    'game_type',
    'game_world',
    'hostname',
    'igvm_locked',
    'intern_ip',
    'io_weight',
    'mac',
    'memory',
    'num_cpu',
    'os',
    'project',
    'puppet_ca',
    'puppet_disabled',
    'puppet_master',
    'route_network',
    'sshfp',
    'state',
    {'hypervisor': HYPERVISOR_ATTRIBUTES},
]

# The list is ordered from more important to less important.  The next
# preference is only going to be checked when the previous ones return all
# the same values.
HYPERVISOR_PREFERENCES = [
    InsufficientResource('libvirt_pool_total_gib', 'disk_size_gib'),
    InsufficientResource('libvirt_memory_total_gib', 'memory', multiplier=1024),
    # Checks the maximum vCPU usage (95 percentile) of the given hypervisor
    # for the given time_range and dismisses it as target when it is over
    # the value of threshold.
    HypervisorAttributeValueLimit('cpu_util_vm_pct', 45),
    # Don't migrate two redundant VMs together
    OtherVMs([
        'project',
        'function',
        'environment',
        'game_market',
        'game_world',
        'game_type',
    ]),
    # Don't migrate two masters database servers together
    OtherVMs(['game_world', 'function'], [0, 'db']),
    OtherVMs(['function'], ['master_db']),
    # Don't migrate two monitoring worker to the same hypervisor
    OtherVMs(['function'], ['monitoring-worker']),
    # Less over-allocated (CPU) hypervisors first
    OverAllocation('num_cpu'),
    # Find less loaded Hypervisor
    HypervisorAttributeValue('cpu_util_pct'),
    # Find Hypervisor with less I/O utilization
    HypervisorAttributeValue('iops_avg'),
    # Prefer the hypervisor with less VMs from the same cluster
    OtherVMs(['project', 'environment', 'game_market']),
    # As the last resort, choose the hypervisor with less VMs
    OtherVMs(),
    # Use hash differences to have a stable ordering
    HashDifference(),
]
