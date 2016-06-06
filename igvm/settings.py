COMMON_FABRIC_SETTINGS = dict(
    disable_known_hosts=True,
    use_ssh_config=True,
    always_use_pty=False,
    forward_agent=True,
    user='root',
    shell='/bin/bash -c',
)

DEFAULT_DNS_SERVERS = (
    '10.0.0.102',
    '10.0.0.85',
    '10.0.0.83',
)

# Swap size in MiB
DEFAULT_SWAP_SIZE = 1024


# Reserved memory for host OS in MiB
HOST_RESERVED_MEMORY = 2*1024


# Default max number of CPUs, unless the HV has fewer cores or num_cpu
# is larger than this value.
KVM_DEFAULT_MAX_CPUS = 24


# Mapping to determine the libvirt CPU model based on serveradmin hw_model
KVM_HWMODEL_TO_CPUMODEL = {
    'Nehalem': ['Dell_M610', 'Dell_M710'],
    'SandyBridge': ['Dell_M620', 'Dell_M630', 'Dell_R620'],
}


PUPPET_CA_MASTERS = (
    # Puppet 3
    'master.puppet.ig.local',
    # Puppet 4
    'ca.puppet.ig.local',
)
