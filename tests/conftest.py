"""igvm - Shared functions

Copyright (c) 2020 InnoGames GmbH
"""
from datetime import datetime, timezone
from math import ceil, log
from re import match
from shlex import quote

from adminapi.dataset import Query
from adminapi.exceptions import DatasetError
from adminapi.filters import Any, Regexp
from libvirt import VIR_DOMAIN_RUNNING

from igvm.hypervisor import Hypervisor
from igvm.settings import HYPERVISOR_ATTRIBUTES
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


def clean_all(route_network, vm_hostname=None):
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
        pattern = '^([0-9]+_)?{}$'.format(
            VM_HOSTNAME_PATTERN.format(JENKINS_EXECUTOR, '[0-9]+'),
        )
    else:
        pattern = '^([0-9]+_)?{}$'.format(vm_hostname)

    # Clean HVs one by one.
    for hv in hvs:
        clean_hv(hv, pattern)

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


def get_next_address(vm_net, index):
    subnet_levels = ceil(log(PYTEST_XDIST_WORKER_COUNT, 2))
    project_network = Query({'hostname': vm_net}, ['intern_ip']).get()

    try:
        subnets = project_network['intern_ip'].subnets(subnet_levels)
    except ValueError:
        raise Exception(
            'Can\'t split {} into enough subnets '
            'for {} parallel tests'.format(
                vm_net, PYTEST_XDIST_WORKER_COUNT,
            )
        )

    return list(subnets)[PYTEST_XDIST_WORKER][index]
