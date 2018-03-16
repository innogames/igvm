"""igvm - Settings

Copyright (c) 2018, InnoGames GmbH
"""

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
    always_use_pty=False,
    forward_agent=True,
    shell='/bin/sh -c',
    timeout=5,
    connection_attempts=1,
)

# Swap size in MiB
DEFAULT_SWAP_SIZE = 1024


VG_NAME = 'xen-data'
RESERVED_DISK = 5.0


# Reserved memory for host OS in MiB
HOST_RESERVED_MEMORY = 2 * 1024


# Default max number of CPUs, unless the hypervisor has fewer cores or num_cpu
# is larger than this value.
KVM_DEFAULT_MAX_CPUS = 24


# Mapping to determine the libvirt CPU model based on serveradmin hw_model
KVM_HWMODEL_TO_CPUMODEL = {
    'Nehalem': ['Dell_M610', 'Dell_M710'],
    'SandyBridge': ['Dell_M620', 'Dell_M630', 'Dell_M640', 'Dell_R620'],
}


# There are various combinations of source and target HVs which come
# with their own bugs and must be addressed separately.
MIGRATE_COMMANDS = {
    ('jessie', 'jessie'):
        # Using p2p migrations on Jessie causes qemu process to allocate
        # as much memory as disk size on source HV.
        ' --desturi qemu+ssh://{destination}/system',
    ('stretch', 'stretch'): (
        # Live migration works only via p2p on Stretch. See Debian bug #796122.
        ' --desturi qemu+tls://{destination}/system'
        ' --p2p'
        ' --tunnelled'
    ),
    ('stretch', 'jessie'): (
        # Jessie can still correctly *receive* p2p migration.
        ' --desturi qemu+tls://{destination}/system'
        ' --p2p'
        ' --tunnelled'
    ),
    # Jessie to Stretch is unsupported because VM after migration looses
    # access to disk. After kernel reboots (not by panic, maybe by watchdog?)
    # it works fine again.
}

# Arbitrarily chosen MAC address prefix with U/L bit set
# It will be padded with the last three octets of the internal IP address.
MAC_ADDRESS_PREFIX = (0xCA, 0xFE, 0x01)

FOREMAN_IMAGE_URL = 'http://aw-foreman.ig.local:8080/{image}'
FOREMAN_IMAGE_MD5_URL = 'http://aw-foreman.ig.local:8080/{image}.md5'

IMAGE_PATH = '/tmp'

HYPERVISOR_ATTRIBUTES = [
    'cpu_util_pct',
    'cpu_util_vm_pct',
    'disk_size_gib',
    'hardware_model',
    'hostname',
    'intern_ip',
    'memory',
    'num_cpu',
    'os',
    'state',
    'vlan_networks',
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
    'intern_ip',
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
    'xen_host',
]

# The list is ordered from more important to less important.  The next
# preference is only going to be checked when the previous ones return all
# the same values.
HYPERVISOR_PREFERENCES = [
    # We assume 10 GiB for root partition, 16 for swap, and 6 reserved.
    InsufficientResource('disk_size_gib', reserved=32),
    InsufficientResource('memory'),
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
    # Less over-allocated (CPU) hypervisors first
    OverAllocation('num_cpu'),
    # Find less loaded Hypervisor
    HypervisorAttributeValue('cpu_util_pct'),
    # Prefer the hypervisor with less VMs from the same cluster
    OtherVMs(['project', 'environment', 'game_market']),
    # As the last resort, choose the hypervisor with less VMs
    OtherVMs(),
    # Use hash differences to have a stable ordering
    HashDifference(),
]
