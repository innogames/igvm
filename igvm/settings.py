"""igvm - Settings

Copyright (c) 2018 InnoGames GmbH
"""

from os import environ
from sys import stdout

from libvirt import (
    VIR_MIGRATE_PEER2PEER,
    VIR_MIGRATE_TLS,
    VIR_MIGRATE_TUNNELLED,
)

from igvm.hypervisor_preferences import (
    HypervisorAttributeValueLimit,
    HypervisorCpuUsageLimit,
    HypervisorEnvironmentValue,
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
    connection_attempts=3,
    remote_interrupt=True,
)

# Can't not add a key with dict built above and None value gets interpreted
# as "None" username, thus separate code.
if 'IGVM_SSH_USER' in environ:
    COMMON_FABRIC_SETTINGS['user'] = environ.get('IGVM_SSH_USER')

VG_NAME = 'xen-data'
# Reserved pool space on Hypervisor
# TODO: this could be a percent value, at least for ZFS.
RESERVED_DISK = {
    'logical': 5.0,
    'zfs': 2 * 1024,
}

# Reserved memory for host OS in MiB
HOST_RESERVED_MEMORY_MIB = {
    'logical': 2 * 1024,
    'zfs': 8 * 1024,
}

VM_OVERHEAD_MEMORY_MIB = 50

# Prevent creation of VMs with odd memory block size.
# See vm.py for details.
MEM_BLOCK_BOUNDARY_GiB = 63
MEM_BLOCK_SIZE_GiB = 2

# Default max number of CPUs, unless the hypervisor has fewer cores or num_cpu
# is larger than this value.
KVM_DEFAULT_MAX_CPUS = 24

# Mapping to determine the libvirt CPU model based on serveradmin hw_model
KVM_HWMODEL_TO_CPUMODEL = {
    'Nehalem': ['Dell_R510', 'Dell_M610', 'Dell_M710'],
    'SandyBridge': ['Dell_R320', 'Dell_M620', 'Dell_R620'],
    'Haswell-noTSX': ['Dell_R430', 'Dell_M630', 'Dell_M640', 'Dell_R640'],
    'EPYC': ['Dell_R6515', 'Dell_R7515', 'Supermicro_H13S'],
}

XFS_CONFIG = {
    'stretch': [''],
    'buster': ['-m reflink=1'],
    'bullseye': ['-m reflink=1'],
    'bookworm': ['-m reflink=1'],
    'rolling': ['-m reflink=1'],
}

P2P_MIGRATION = {
    'uri': 'qemu+tls://{destination}/system',
    'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TUNNELLED,
}

P2P_TLS_MIGRATION = {
    'uri': 'qemu+tls://{destination}/system',
    'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TLS,
}

# There are various combinations of source and target HVs which come
# with their own bugs and must be addressed separately.
MIGRATE_CONFIG = {
    # Live migration works only via p2p on Stretch. See Debian bug #796122.
    ('stretch', 'stretch'): P2P_MIGRATION,
    ('stretch', 'buster'): P2P_MIGRATION,
    # ('buster', 'stretch') impossible because of AppArmor on Buster
    # "direct migration is not supported by the source host"
    ('buster', 'buster'): P2P_MIGRATION,
    # Online migrations are only working between Bullseye and Bullseye.
    # The other cases for migrations (Buster to Bullseye and vice-versa) are
    # not working, due to what looks like libvirt incompatibility.
    ('bullseye', 'bullseye'): P2P_TLS_MIGRATION,
    ('bullseye', 'bookworm'): P2P_TLS_MIGRATION,
    ('bookworm', 'bookworm'): P2P_TLS_MIGRATION,
    ('bookworm', 'bullseye'): P2P_TLS_MIGRATION,
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

NETWORK_ATTRIBUTES = [
    'hostname',
    'service_groups',
]

HYPERVISOR_ATTRIBUTES = [
    'cpu_perffactor',
    'cpu_util_pct',
    'environment',
    'hardware_model',
    'hostname',
    'igvm_locked',
    'igvm_migration_log',
    'intern_ip',
    'iops_avg',
    'igvm_migration_log',
    'libvirt_memory_total_gib',
    'libvirt_memory_used_gib',
    'libvirt_pool_total_gib',
    'libvirt_pool_used_gib',
    'num_cpu',
    'os',
    {'route_network': NETWORK_ATTRIBUTES},
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
            'served_game',
            'state',
        ],
    },
]

VM_ATTRIBUTES = [
    'aws_image_id',
    'aws_instance_id',
    'aws_instance_type',
    'aws_key_name',
    'aws_placement',
    'aws_subnet_id',
    'aws_vpc_id',
    'datacenter',
    'datacenter_type',
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
    'load_99',
    'mac',
    'memory',
    'num_cpu',
    'os',
    'primary_ip6',
    'project',
    {'project_network': NETWORK_ATTRIBUTES},
    'puppet_ca',
    'puppet_disabled',
    'puppet_master',
    {'route_network': NETWORK_ATTRIBUTES},
    'served_game',
    'service_groups',
    'sshfp',
    'state',
    {'hypervisor': HYPERVISOR_ATTRIBUTES},
]

AWS_RETURN_CODES = {
    'pending': 0,
    'running': 16,
    'shutting-down': 32,
    'terminated': 48,
    'stopping': 64,
    'stopped': 80,
}

AWS_FALLBACK_INSTANCE_TYPE = 'c5.xlarge'
AWS_INSTANCES_OVERVIEW_URL = 'https://www.ec2instances.info/instances.json'
AWS_INSTANCES_OVERVIEW_FILE = 'AWS_INSTANCES_OVERVIEW_FILE.json'
AWS_INSTANCES_OVERVIEW_FILE_ETAG = 'AWS_INSTANCES_OVERVIEW_FILE.etag'
AWS_ECU_FACTOR = 7
# To prevent conflict with other AWS-using projects
environ['AWS_SHARED_CREDENTIALS_FILE'] = "~/.aws/credentials"

AWS_CONFIG = [{
    'apt': [{
        'name': 'innogames_stable',
        'filename': 'innogames_stable.list',
        'source': 'deb https://aptly.innogames.de/innogames innogames stable',
        'key': [
            '-----BEGIN PGP PUBLIC KEY BLOCK-----',
            '',
            'mQINBF/BMWsBEADaN7jZ05f8nRckQhbECn3MnEosMObAYYc9m3ktGvg5fGPIwRFw',
            'StyT/EjLiTmVjjYdb4ZeMOrIoXNaw3kTv3Q3LM29MkfcNGBOxBO7EjKkNjnp6CQk',
            '3aUG6sh2N1hrvHBQ4ybbHjsiOeuiYD6CYtOk1m9nKHLD41mMjUhiRkD1hw7nvM/I',
            'Lq5K/l45OAAClWotOCd2DA5yg63XYcH8iDYbOtVMiLgTPtUxkfXVdT5QEs/22pP8',
            'gm3RlL/JI+uceuBfDLPNPpALHMRQZnWO+NDmR/o5ilIF5oY2kc6zqYigAO4eapne',
            'Ri6sdqAe5QCxM8vZb5hDBfPxlObaym5erClfpjWqeyWQmmlISEoQTZQwkuOSFp6l',
            'SywinwzLQ9iHPMafhnrkIE6yY+H49u17w5KdHosDbHjEzm5IEWoufrDIaS4hm2Vh',
            'pnA2G12UiprlCGfpLLg5R5gOjmDquJENo1KZK1bGtUFt+ZjoN6Nf1Mp/6vosF/fh',
            'k/Gu4Xoo4YUCNedd5zr4bI81f8FL830uH2bowSYItBQbRhBDaVNNcZd6ZF2lbadk',
            '8fzSNWOZtdCbt+sD9jA7vepOZWq+pwcVVLNi4RWmcZg0dmIzzxcepKxczEMUN8dT',
            'AoOXLvrJuvfwxPLgN5NriU4jqwDVseIQOZP1jgy4s8hv9AbIv4ogdJjkQwARAQAB',
            'tEtJbm5vR2FtZXMgQXB0bHkgKElubm9HYW1lcyBBcHRseSBSZXBvc2l0b3J5IFNp',
            'Z25pbmcgS2V5KSA8aXRAaW5ub2dhbWVzLmNvbT6JAk4EEwEKADgWIQQNhO6rw0VH',
            '0PrfG+qyPKCtrIn4zwUCX8ExawIbAwULCQgHAgYVCgkICwIEFgIDAQIeAQIXgAAK',
            'CRCyPKCtrIn4z+glD/9fXKLWSMLTaGvj2TaKRE6TRzkuAn68JmX8dEt17+LCI3HN',
            'UPtF6LFBG9hQeOV+u3lMPr98MjQrIVxJm54zWlzGsev9VauRhoQcmB6L68K7WK4r',
            'HETQdoLMQl5tpocZKsDER2eiGgaJiUYt17EJeUr2/sqKPnAvHzLqtOpKUS1bdGtw',
            '7o3oVnu7Ufn4aSx00WAjeooAWByXlAafo835+ue/H1QrDUaoc1WEUWZz2iwsVrWG',
            'jmcaXwWmylauxSqO2x4QK7uLZ9jHvj99Zykclq+zF9FRAyvH95Z3zWcQRQoOzcH7',
            'ffiPe9ewx3J9VSO7SrkXz3Hq2ZDvKaJeY26br1a0P0XGh5oQ2eRKOh+gmbfKVqNK',
            'LGT00VlkGCDbFwFCJm4Zsu2qfJdknsfOR4qL/snUc7IRnsu264vBRvH+zlsNP3CL',
            'pcqaKEbJ6coS5Cf/sv+LpEZAtt6P/DIf24x6PmUNFErCbOhW7vEUFFn+0FDGPQ68',
            'k8XxG6hyar5HbBJrQWO6GSJqF/7PqCiP4hdaEGn03bK9t9FuGLagx4qYiBDw64LY',
            'LXnqKmrTts4bCk0KE/qngJR3QrozvdyA0G3BZgMM7Y4h9LiL30qLXvtnv3YARpIl',
            '3CMAd5U773hZbyCb6cIcDG6EVVGVlHDqvY6f+r/eqPxqDzoS3DpY2/5ijikN4w==',
            '=m6al',
            '-----END PGP PUBLIC KEY BLOCK-----',
        ],
    }, {
        'name': 'base#VM_OS#_stable',
        'filename': 'base#VM_OS#_stable.list',
        'source': 'deb https://aptly.innogames.de/base#VM_OS# base#VM_OS# stable',
        'key': [
            '-----BEGIN PGP PUBLIC KEY BLOCK-----',
            '',
            'mQINBF/BMWsBEADaN7jZ05f8nRckQhbECn3MnEosMObAYYc9m3ktGvg5fGPIwRFw',
            'StyT/EjLiTmVjjYdb4ZeMOrIoXNaw3kTv3Q3LM29MkfcNGBOxBO7EjKkNjnp6CQk',
            '3aUG6sh2N1hrvHBQ4ybbHjsiOeuiYD6CYtOk1m9nKHLD41mMjUhiRkD1hw7nvM/I',
            'Lq5K/l45OAAClWotOCd2DA5yg63XYcH8iDYbOtVMiLgTPtUxkfXVdT5QEs/22pP8',
            'gm3RlL/JI+uceuBfDLPNPpALHMRQZnWO+NDmR/o5ilIF5oY2kc6zqYigAO4eapne',
            'Ri6sdqAe5QCxM8vZb5hDBfPxlObaym5erClfpjWqeyWQmmlISEoQTZQwkuOSFp6l',
            'SywinwzLQ9iHPMafhnrkIE6yY+H49u17w5KdHosDbHjEzm5IEWoufrDIaS4hm2Vh',
            'pnA2G12UiprlCGfpLLg5R5gOjmDquJENo1KZK1bGtUFt+ZjoN6Nf1Mp/6vosF/fh',
            'k/Gu4Xoo4YUCNedd5zr4bI81f8FL830uH2bowSYItBQbRhBDaVNNcZd6ZF2lbadk',
            '8fzSNWOZtdCbt+sD9jA7vepOZWq+pwcVVLNi4RWmcZg0dmIzzxcepKxczEMUN8dT',
            'AoOXLvrJuvfwxPLgN5NriU4jqwDVseIQOZP1jgy4s8hv9AbIv4ogdJjkQwARAQAB',
            'tEtJbm5vR2FtZXMgQXB0bHkgKElubm9HYW1lcyBBcHRseSBSZXBvc2l0b3J5IFNp',
            'Z25pbmcgS2V5KSA8aXRAaW5ub2dhbWVzLmNvbT6JAk4EEwEKADgWIQQNhO6rw0VH',
            '0PrfG+qyPKCtrIn4zwUCX8ExawIbAwULCQgHAgYVCgkICwIEFgIDAQIeAQIXgAAK',
            'CRCyPKCtrIn4z+glD/9fXKLWSMLTaGvj2TaKRE6TRzkuAn68JmX8dEt17+LCI3HN',
            'UPtF6LFBG9hQeOV+u3lMPr98MjQrIVxJm54zWlzGsev9VauRhoQcmB6L68K7WK4r',
            'HETQdoLMQl5tpocZKsDER2eiGgaJiUYt17EJeUr2/sqKPnAvHzLqtOpKUS1bdGtw',
            '7o3oVnu7Ufn4aSx00WAjeooAWByXlAafo835+ue/H1QrDUaoc1WEUWZz2iwsVrWG',
            'jmcaXwWmylauxSqO2x4QK7uLZ9jHvj99Zykclq+zF9FRAyvH95Z3zWcQRQoOzcH7',
            'ffiPe9ewx3J9VSO7SrkXz3Hq2ZDvKaJeY26br1a0P0XGh5oQ2eRKOh+gmbfKVqNK',
            'LGT00VlkGCDbFwFCJm4Zsu2qfJdknsfOR4qL/snUc7IRnsu264vBRvH+zlsNP3CL',
            'pcqaKEbJ6coS5Cf/sv+LpEZAtt6P/DIf24x6PmUNFErCbOhW7vEUFFn+0FDGPQ68',
            'k8XxG6hyar5HbBJrQWO6GSJqF/7PqCiP4hdaEGn03bK9t9FuGLagx4qYiBDw64LY',
            'LXnqKmrTts4bCk0KE/qngJR3QrozvdyA0G3BZgMM7Y4h9LiL30qLXvtnv3YARpIl',
            '3CMAd5U773hZbyCb6cIcDG6EVVGVlHDqvY6f+r/eqPxqDzoS3DpY2/5ijikN4w==',
            '=m6al',
            '-----END PGP PUBLIC KEY BLOCK-----',
        ],
    }, {
        'name': '#VM_OS#_puppet7',
        'filename': '#VM_OS#_puppet7.list',
        'source': 'deb https://apt.puppetlabs.com #VM_OS# puppet7',
        'key': [
            '-----BEGIN PGP PUBLIC KEY BLOCK-----',
            'Version: GnuPG v1',
            '',
            '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',
            '=u0aK',
            '-----END PGP PUBLIC KEY BLOCK-----',
        ],
    }],
}]

# The loadtested CPU usage thresholds per hypervisor hardware model,
# before we are experiencing reasonable steal time.
HYPERVISOR_CPU_THRESHOLDS = {
    'Dell_M610': 50,  # untested
    'Dell_M620': 60,
    'Dell_M630': 70,
    'Dell_M640': 75,
    'Dell_M710': 50,  # untested
    'Dell_R320': 50,  # untested
    'Dell_R430': 25,  # only one non relevant host
    'Dell_R510': 50,  # untested
    'Dell_R620': 25,  # only two hosts in retired state
    'Dell_R640': 75,
    'Dell_R6515': 50,
    'Dell_R7515': 50,
    'Supermicro_H13S': 50,
}

# The list is ordered from more important to less important.  The next
# preference is only going to be checked when the previous ones return all
# the same values.
HYPERVISOR_PREFERENCES = [
    InsufficientResource(
        'libvirt_pool_total_gib',
        'disk_size_gib',
        reserved=32,
    ),
    InsufficientResource(
        'libvirt_memory_total_gib',
        'memory',
        multiplier=1024,
        reserved=2048,
    ),
    # Compares the environment of the VM with the environment of the
    # hypervisor. It makes hypervisors of different envs less likely chosen.
    HypervisorEnvironmentValue('environment'),
    # Calculates the performance_value of the given VM, which is comparable
    # across hypervisor hardware models. It uses this value to predict the
    # CPU usage of the VM on the destination hypervisor and dismisses all
    # targets with a value above the threshold.
    HypervisorCpuUsageLimit(
        'hardware_model',
        HYPERVISOR_CPU_THRESHOLDS,
    ),
    # Don't migrate two redundant VMs together
    OtherVMs([
        'project',
        'function',
        'environment',
        'game_market',
        'game_world',
        'game_type',
        'served_game',
    ]),
    # Don't migrate two masters database servers together
    OtherVMs(['game_world', 'function'], [0, 'db']),
    # Don't migrate two jenkins nodes to the same hypervisor
    OtherVMs(['function'], ['jenkins_node']),
    # Don't migrate two monitoring worker to the same hypervisor
    OtherVMs(['function'], ['monitoring-worker']),
    # Less over-allocated (CPU) hypervisors first
    OverAllocation('num_cpu'),
    # Find less loaded Hypervisor
    HypervisorAttributeValueLimit('cpu_util_pct', 100),
    # Find Hypervisor with less I/O utilization
    HypervisorAttributeValueLimit('iops_avg', 100),
    # Prefer the hypervisor with less VMs from the same cluster
    OtherVMs(['project', 'environment', 'game_market']),
]
