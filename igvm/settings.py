COMMON_FABRIC_SETTINGS = dict(
    disable_known_hosts=True,
    use_ssh_config=True,
    always_use_pty=False,
    forward_agent=True,
    user='root',
    shell='/bin/bash -c',
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
    'SandyBridge': ['Dell_M620', 'Dell_M630', 'Dell_R620'],
}


# Arbitrarily chosen MAC address prefix with U/L bit set
# It will be padded with the last three octets of the internal IP address.
MAC_ADDRESS_PREFIX = (0xCA, 0xFE, 0x00,)

FOREMAN_IMAGE_URL = 'http://aw-foreman.ig.local:8080/{image}'
FOREMAN_IMAGE_MD5_URL = 'http://aw-foreman.ig.local:8080/{image}.md5'
