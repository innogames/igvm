"""igvm - Shared functions

Copyright (c) 2020 InnoGames GmbH
"""

from datetime import datetime, timezone
from math import ceil, log
from re import match
from time import sleep
from shlex import quote

import boto3
from adminapi.dataset import Query
from adminapi.exceptions import DatasetError
from adminapi.filters import Any, Not, Regexp
from botocore.exceptions import ClientError
from libvirt import VIR_DOMAIN_RUNNING

from igvm.exceptions import VMError, IGVMTestError
from igvm.hypervisor import Hypervisor
from igvm.settings import HYPERVISOR_ATTRIBUTES, AWS_RETURN_CODES
from tests import (
    IGVM_LOCKED_TIMEOUT,
    JENKINS_EXECUTOR,
    PYTEST_XDIST_WORKER,
    PYTEST_XDIST_WORKER_COUNT,
    VM_HOSTNAME_PATTERN,
    VM_NET,
)


def cmd(cmd, *args, **kwargs):
    escaped_args = [quote(str(arg)) for arg in args]

    escaped_kwargs = {}
    for key, value in kwargs.items():
        escaped_kwargs[key] = quote(str(value))

    return cmd.format(*escaped_args, **escaped_kwargs)


def clean_all(route_network, datacenter_type, vm_hostname=None):
    # Cancelled builds are forcefully killed by Jenkins. They did not have the
    # opportunity to clean up so we forcibly destroy everything found on any HV
    # which would interrupt our work in the current JENKINS_EXECUTOR.
    hvs = [Hypervisor(o) for o in Query({
        'servertype': 'hypervisor',
        'environment': 'testing',
        'vlan_networks': route_network,
        'state': 'online',
    }, HYPERVISOR_ATTRIBUTES)]

    # If a VM hostname is given, only that will be cleaned from HVs.
    if vm_hostname is None:
        pattern = '^([0-9]+_)?(vm-rename-)?{}$'.format(
            VM_HOSTNAME_PATTERN.format(JENKINS_EXECUTOR, '[0-9]+'),
        )
    else:
        pattern = '^([0-9]+_)?(vm-rename-)?{}$'.format(vm_hostname)

    # Clean HVs one by one.
    if datacenter_type == 'kvm.dct':
        for hv in hvs:
            clean_hv(hv, pattern)

    if datacenter_type == 'aws.dct':
        clean_aws(vm_hostname)

    # Remove all connected Serveradmin objects.
    clean_serveradmin({'hostname': Regexp(pattern)})

    # Try to remove VMs with the same IP in any case because we use custom
    # logic to assign them and we want to avoid IP address conflicts.
    # Index 1 is usually used for the test's subject VM,
    # 2 might be used for testing IP change.
    ips = [get_next_address(VM_NET, i) for i in [1, 2]]
    clean_serveradmin({'intern_ip': Any(*ips)})


def clean_hv(hv, pattern):
    # We never know what happened on the HV, so always refresh
    # the storage pool before we do anything.
    st_pool = hv.get_storage_pool()
    st_pool.refresh()

    # Undefine leftover domains
    for domain in hv.conn().listAllDomains():
        if not match(pattern, domain.name()):
            continue

        if domain.state()[0] == VIR_DOMAIN_RUNNING:
            domain.destroy()
        domain.undefine()

    # Delete leftover volumes
    for vol_name in st_pool.listVolumes():
        if not match(pattern, vol_name):
            continue

        hv.run(
            'mount '
            '| awk \'/{}/ {{print $3}}\' '
            '| xargs -r -n1 umount'.format(
                vol_name.replace('-', '--'),
            ),
        )
        st_pool.storageVolLookupByName(vol_name).delete()

    # Cleanup igvm_locked status after a timeout
    if hv.dataset_obj['igvm_locked'] is not None:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        diff = now - hv.dataset_obj['igvm_locked']

        if diff >= IGVM_LOCKED_TIMEOUT:
            try:
                hv.release_lock()
            except DatasetError:
                # In case multiple workers try to release the same HV
                # we will get commit errors which we can ignore.
                pass


def clean_serveradmin(filters):
    Query(filters).delete().commit()


def clean_aws(vm_hostname):
    def _get_instance_status():
        response = ec2.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-code',
                    'Values': [
                        str(AWS_RETURN_CODES['pending']),
                        str(AWS_RETURN_CODES['running']),
                        str(AWS_RETURN_CODES['shutting-down']),
                        str(AWS_RETURN_CODES['terminated']),
                        str(AWS_RETURN_CODES['stopping']),
                        str(AWS_RETURN_CODES['stopped']),
                    ]
                },
            ],
            InstanceIds=[obj['aws_instance_id']],
            DryRun=False)['Reservations'][0]['Instances'][0]['State']['Code']

        return int(response)

    try:
        obj = Query({'hostname': vm_hostname}, ['aws_instance_id']).get()
    except DatasetError:  # No object to clean up
        return

    if not obj['aws_instance_id']:
        return

    timeout = 120
    ec2 = boto3.client('ec2')
    try:
        ec2.stop_instances(
            InstanceIds=[obj['aws_instance_id']], DryRun=False
        )
    except ClientError as e:
        pass  # Not running
    for _ in range(timeout):
        instance_status = _get_instance_status()
        if AWS_RETURN_CODES['stopped'] == instance_status:
            break
        sleep(1)

    ec2.terminate_instances(InstanceIds=[obj['aws_instance_id']])
    for _ in range(timeout):
        instance_status = _get_instance_status()
        if AWS_RETURN_CODES['terminated'] == instance_status:
            break
        sleep(1)


def get_next_address(vm_net, index):
    non_vm_hosts = list(Query({
        'project_network': vm_net,
        'servertype': Not('vm'),
    }, ['intern_ip']))
    offset = 1 if len(non_vm_hosts) > 0 else 0
    subnet_levels = ceil(log(PYTEST_XDIST_WORKER_COUNT + offset, 2))
    project_network = Query({'hostname': vm_net}, ['intern_ip']).get()
    try:
        subnets = list(project_network['intern_ip'].subnets(subnet_levels))
    except ValueError:
        raise IGVMTestError(
            'Can\'t split {} into enough subnets '
            'for {} parallel tests'.format(
                vm_net, PYTEST_XDIST_WORKER_COUNT,
            )
        )
    if len(non_vm_hosts) > subnets[0].num_addresses:
        raise IGVMTestError(
            'Can\'t split {} into enough subnets '
            'for {} parallel tests'.format(
                vm_net, PYTEST_XDIST_WORKER_COUNT,
            )
        )
    return subnets[PYTEST_XDIST_WORKER + 1][index]
