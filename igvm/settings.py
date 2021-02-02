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
    'jessie': [''],
    'stretch': [''],
    'buster': ['-m reflink=1'],
}

P2P_MIGRATION = {
    'uri': 'qemu+tls://{destination}/system',
    'flags': VIR_MIGRATE_PEER2PEER | VIR_MIGRATE_TUNNELLED,
}

# There are various combinations of source and target HVs which come
# with their own bugs and must be addressed separately.
MIGRATE_CONFIG = {
    # Using p2p migrations on Jessie causes qemu process to allocate
    # as much memory as disk size on source HV.
    ('jessie', 'jessie'): {
        'uri': 'qemu+ssh://{destination}/system',
        'flags': 0,
    },
    # ('jessie', 'stretch') is unsupported because VM after migration looses
    # access to disk. After kernel reboots (not by panic, maybe by watchdog?)
    # it works fine again.
    #
    # Jessie can still correctly *receive* p2p migration.
    ('stretch', 'jessie'): P2P_MIGRATION,
    # Live migration works only via p2p on Stretch. See Debian bug #796122.
    ('stretch', 'stretch'): P2P_MIGRATION,
    ('stretch', 'buster'): P2P_MIGRATION,
    # ('buster', 'stretch') impossible because of AppArmor on Buster
    # "direct migration is not supported by the source host"
    ('buster', 'buster'): P2P_MIGRATION,
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
    'cpu_perffactor',
    'cpu_util_pct',
    'cpu_util_vm_pct',
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
    'route_network',
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
    'aws_security_group_ids',
    'aws_subnet_id',
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
    'puppet_ca',
    'puppet_disabled',
    'puppet_master',
    'route_network',
    'served_game',
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

AWS_CONFIG = [
    {
        'apt': [
            {
                'name': 'innogames_stable',
                'filename': 'innogames_stable.list',
                'source': 'deb http://update-int.ig.local/ innogames stable',
                'key': [
                    "-----BEGIN PGP PUBLIC KEY BLOCK-----",
                    "Version: GnuPG v1.4.10 (GNU/Linux)",
                    "",
                    "mQINBE1JNIYBEADWZJRGs4vzkffGhbCQcrBcMnnq/ogY4ebzv2P",
                    "cT1KbyXjmulJG6KqTf6cmecU77UVIJluxfhZEoPBUdIFWRSbB/u",
                    "e2G+pj2hwu4uSy2MOPtQaXnrrHJyA2EP0s//h+jJwh5KJ/ExNEX",
                    "XpQqDKwik7xvn13Bg5mkP9rj3MIdN/muw4YDoyEHkEVy6VWHy8K",
                    "ZOZqNv3ONlx/tv8mp5GCsyz8my+Ly61fauPFESVphQs/PsyXDzc",
                    "c7BdA2Xprs6nc08tr2pm14Zhzuv1aDb3RbmIOC+KfSlBfdhgMNO",
                    "DtnkoHugdXXunwQFOQKhKUoqrD06v3yluhooKVGzlOOB1wu6RPg",
                    "SFrd2qR//c8ksM3kJfVBk+oovOU70/SYbv/JgLlQqdQW1yXUTsZ",
                    "+MCBQvnP/Lbg48FxpuWiv2/XToiamR/DvpYY4kE6GFnBCFigVFN",
                    "tw+2LnK6iwBdb6zEqsRkc91czEtfdho61zbNajfNy293F3uRbmN",
                    "NmClMLZTuxGx3abQiccQxCjFy4MVsrvqUSi3Q+OcGNpZ6Cnd4TS",
                    "ulT2Uj9FpvJ77/eKg0YCGDffqfQjwGaThb8fU5TWSsmRWxDVTOn",
                    "3xvMHOBWvoGR479+KngaohU5t+mBct3NfTYcXYOxA8EHZfXxvjJ",
                    "Vu+4v1gYIyOPdOLNltKFd+yq3Z6UxAuw1mwARAQABtDxNYXR0aG",
                    "lhcyBLbGVpbiAoVXBkYXRlLU1pcnJvcikgPG1hdHRoaWFzLmtsZ",
                    "WluQGlubm9nYW1lcy5kZT6JAjgEEwECACIFAk1JNIYCGwMGCwkI",
                    "BwMCBhUIAgkKCwQWAgMBAh4BAheAAAoJEKoruC7fdZMUGk8P+wX",
                    "0ZzJ4wqOWc/n3n3ZSxy9MbUa/Ry84woNr9dEtdhyEwpnlFxJYmW",
                    "SqXBwHZNWZ3ENwWfZisPawtfNbujwxI83dAziiDtmllQg0kKWOF",
                    "I7Qh7BhJNnq5sWA/CjtOcFsJGCBL5QMKX+u9xSSMvk7nUKaQ6Vz",
                    "EqzlR67DJigZc5LiCqR7LgXzDoACKAD6LZiv7Ft4ApSpnemvYEQ",
                    "tUbuptGqAcBF4TPeZw2DDKdaev6HOmWA0BvxO7/lGwxvsQ/x+c3",
                    "FBBWhz81Vmv8kEpgQxj6pookPcvHX7jOCBWb3MfOkz6+2nw8b6N",
                    "FRNu+/huMiHtFtGz0e7F5tdsMZpfbQVbBYERa8XKABcTLpK0wAT",
                    "0n9RHQOQaeBfZ/OPYrZbKPsDXo/094J6Z9UffZMGPaEgt9+hR5Z",
                    "62//TEyEUHNoFukDBBJcy8c+GnPpmTQ17XzhxKI0pPJ2H5aDNeh",
                    "5b3D6y0WXgpiOKiNxr0wX1t0pyDO1Y7JE/51qq7ETol0ezws7vV",
                    "LrxFi0C7s2mjNWivS5J3PcvF+TK+a1CwYG0XDf3aV2OnxhDWz9Y",
                    "VtyY8NLEmLzsbmCigzXy+1h23TBXaVlKXaw9k8dxnKM9UStc3hM",
                    "IN50aqK0Tucbz2yj9+QQf9YHfzWOfqP9m0nm2CCmx1w+GZE9MLA",
                    "hszNrlTW7UN+hEyA1Z=mJCR",
                    "-----END PGP PUBLIC KEY BLOCK-----"
                ],
            },
            {
                'name': 'base#VM_OS#_stable',
                'filename': 'base#VM_OS#_stable.list',
                'source': 'deb http://update-int.ig.local/ base#VM_OS# stable',
                'key': [
                    "-----BEGIN PGP PUBLIC KEY BLOCK-----",
                    "Version: GnuPG v1",
                    "",
                    "mQINBFjsyv0BEADlyvUwquhi/PzwaYIG2JMNHEMnUc3jKaHd3SA",
                    "W6kRk6uzao9B18fBMRnuGFtfrmRxa7CvqTTcn3hXl8h7kZQICBH",
                    "XJrNXB2KDs4gnNDXbjXBuLJyW5jVe8Ghn7GEJbzUAu+iNPBKi1U",
                    "YkUoMSk+sAbXqOaUJQDJSaOU4A7C9wL1mAxsWNhucpCBtt9HTTn",
                    "VWiZiLAtvop7LJEgXEpQbht/jW7mdMX5yAbWfYUJMdXFMCADz5r",
                    "6mbtuun5BaDae31dgA//IPI+Im8PxJBgdyyETfhoSbThiFDk4K1",
                    "UgXXlAwYxR7TVxPjvWN8HDlbBl4TvM29ppz8bZJARQx3h1SYIAr",
                    "YJ2Xc9lQwsbK0KzL/w1NmZsiPztbpM0Fs0Q7+oQScxuIolpu4c7",
                    "7siEged2R6dnXR/er+zN5/GgTdaIEcQnTRI9F8bZKrhIy/TSLgR",
                    "nxZYFyNE30yQtJD2WR9fuh6zT72LY0rvqQcHw7Ze3qW2ZgORswd",
                    "GWmasHMJ/LkXv75MiV4KMBwweYp6OW9ewisdrZEijQS0NEcZ+YF",
                    "C288dLJeWLpPkzdczTO/v/bLDQ/Uk8aOcldtJfB4BjBW9v7JjvL",
                    "cKtCMBsW5P4o3IjVJt+08FfGs7cyaHJifH6RCM2GBDUaNAoPPZm",
                    "ioiKZcw6twMhdrjzVHiPFmolEXzItLmFHNwARAQABtDphZG1pbi",
                    "AoSW5ub2dhbWVzIGRlYmlhbiByZXBvc2l0b3J5IGtleSkgPGl0Q",
                    "Glubm9nYW1lcy5jb20+iQI3BBMBCgAhBQJY7Mr9AhsDBQsJCAcD",
                    "BRUKCQgLBRYCAwEAAh4BAheAAAoJEKy7fO0+1wCYz5oP/iBSLv3",
                    "h+DRCAMeV68EquhyOvGUIZuZ910wL7wW9lqoUmKCdrSoOvXxpE7",
                    "z1TFegRfUtbnXYJYmELwp0SD5F/60ScCoW7bbTQdK4FHms0CYjU",
                    "cJnRjRrQVyE0fiChql6CYif2tWWOR7L+COBeIKrSaqXDPw2X5A0",
                    "mJ+QFRcVhNbz02FIfr1mIECVshN34x5RMnmJtd6ZNhcbldS26Fh",
                    "xC5SrzFkAPExCFBVLiAsfEK51ED4QOfnRRYOlbBvDwL3XQBZtFV",
                    "tznwI1DLg/JNVVXFM8sDei+eL3O5yoOVEuTBRvNH7ONlFRcBdFC",
                    "dfSi1VvKVY2JEQX4R4BHyZvD/B2pcBeTM5puav5QuwKXppwHg4d",
                    "Bbs1BrRsHWGMdqTJWGkwwVUdt3af6WSWD6BA+rv2I3DM7BMt6t/",
                    "FK1BMapHGY2fj3gK8pWyi13R8tryr4ZdIa/RnwxWt8ySWclgDVS",
                    "PxTcNE1Zn2fYDB/YkvTyF52dn3+6UW6mnjL45Hictqo9A6aOxre",
                    "5dlwEyHXhurs/7UP0Ell20UEN1VsKDPE2V8pq+JFXQ+F2sLMEar",
                    "4L5teopk78L7t0FibqmKcFRVgvseBbjm0xHfdTk4mXCDwbaBtX+",
                    "E9qhd5e4brEYNx9I9I2fvXGw5sX6Qdty9HnEQOKGMshEAfl2iCz",
                    "J56n4CAkPnU50v=KPbA",
                    "-----END PGP PUBLIC KEY BLOCK-----"
                ]
            }
        ]
    }
]

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
    # Checks the maximum vCPU usage (95 percentile) of the given hypervisor
    # for the given time_range and dismisses it as target when it is over
    # the value of threshold.
    HypervisorAttributeValueLimit('cpu_util_vm_pct', 45),
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
