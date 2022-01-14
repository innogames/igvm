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
    HypervisorAttributeValue,
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
    connection_attempts=1,
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

# Default max number of CPUs, unless the hypervisor has fewer cores or num_cpu
# is larger than this value.
KVM_DEFAULT_MAX_CPUS = 24

# Mapping to determine the libvirt CPU model based on serveradmin hw_model
KVM_HWMODEL_TO_CPUMODEL = {
    'Nehalem': ['Dell_R510', 'Dell_M610', 'Dell_M710'],
    'SandyBridge': ['Dell_R320', 'Dell_M620', 'Dell_R620'],
    'Haswell-noTSX': ['Dell_R430', 'Dell_M630', 'Dell_M640', 'Dell_R640'],
    'EPYC': ['Dell_R6515'],
}

XFS_CONFIG = {
    'stretch': [''],
    'buster': ['-m reflink=1'],
    'bullseye': ['-m reflink=1'],
}

P2P_MIGRATION = {
    'uri': 'qemu+tls://{destination}/system',
    'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TUNNELLED,
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
    # Bullseye migrations. Only the straightforward ones that are likely
    # to work for now. They are yet to be verified. Bullseye to buster must
    # be figured out later.
    # TODO: rememberme
    ('bullseye', 'bullseye'): P2P_MIGRATION,
    ('buster', 'bullseye'): P2P_MIGRATION,
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

AWS_GRP_NAME = environ.get('AWS_GRP_NAME', default='adm')
AWS_INSTANCES_OVERVIEW_URL = 'https://www.ec2instances.info/instances.json'
AWS_INSTANCES_OVERVIEW_FILE = '/tmp/AWS_INSTANCES_OVERVIEW_FILE.json'
AWS_INSTANCES_OVERVIEW_FILE_ETAG = '/tmp/AWS_INSTANCES_OVERVIEW_FILE.etag'
AWS_ECU_FACTOR = 7
AWS_PROFILE_NAME = environ.get('AWS_PROFILE_NAME', default='default')

AWS_CONFIG = [{
    'apt': [{
        'name': 'innogames_stable',
        'filename': 'innogames_stable.list',
        'source': 'deb http://update-int.ig.local/ innogames stable',
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
        'source': 'deb http://update-int.ig.local/ base#VM_OS# stable',
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
]
