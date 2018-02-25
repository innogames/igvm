"""igvm - Settings

Copyright (c) 2018, InnoGames GmbH
"""

from igvm.balance.constraints import (
    DiskSpace,
    EnsureFunctionDistribution,
    HypervisorMaxVcpuUsage,
    Memory,
)
from igvm.balance.rules import (
    CpuOverAllocation,
    HypervisorMaxCpuUsage,
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

# The list should be ordered from cheaper to execute to more expensive.
HYPERVISOR_CONSTRAINTS = [
    # Hypervisor has enough disk space
    DiskSpace(reserved=5),
    # Hypervisor has enough memory
    Memory(),
    # Hypervisor Max 95 vCPU usage < than 45%
    HypervisorMaxVcpuUsage(threshold=45),
    # Don't migrate two webservers of the same function on one hypervisor
    EnsureFunctionDistribution(),
]

HYPERVISOR_RULES = [
    # Find less loaded Hypervisor
    HypervisorMaxCpuUsage(),
    # Less over-allocated (CPU) hypervisors first
    CpuOverAllocation(),
]
