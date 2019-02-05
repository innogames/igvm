"""igvm - KVM Utilities

Copyright (c) 2018 InnoGames GmbH
"""

import logging
import re
import time
import concurrent.futures
from uuid import uuid4
from xml.dom import minidom
from xml.etree import ElementTree
from time import sleep

from libvirt import (
    VIR_DOMAIN_VCPU_MAXIMUM,
    VIR_DOMAIN_AFFECT_LIVE,
    VIR_DOMAIN_AFFECT_CONFIG,
    VIR_MIGRATE_LIVE,
    VIR_MIGRATE_PERSIST_DEST,
    VIR_MIGRATE_CHANGE_PROTECTION,
    VIR_MIGRATE_NON_SHARED_DISK,
    VIR_MIGRATE_AUTO_CONVERGE,
    VIR_MIGRATE_ABORT_ON_ERROR,
    VIR_ERR_OPERATION_ABORTED,
    libvirtError,
    virGetLastError,
)

from igvm.exceptions import HypervisorError, MigrationError, MigrationAborted
from igvm.settings import (
    KVM_DEFAULT_MAX_CPUS,
    KVM_HWMODEL_TO_CPUMODEL,
    MAC_ADDRESS_PREFIX,
    VG_NAME,
    MIGRATE_CONFIG,
)
from igvm.utils import parse_size
from igvm.vm import VM
import igvm.hypervisor

from jinja2 import Environment, PackageLoader

log = logging.getLogger(__name__)


def _del_if_exists(tree, name):
    """
    Removes an XML node from the ElementTree.
    """
    p = name.rfind('/')
    if p >= 0:
        if p >= 0:
            parent = tree.find(name[:p])
            if parent is None:
                return
            name = name[(p + 1):]
        else:
            parent = tree
        elements = parent.findall(name)
        if len(elements) > 0:
            for element in elements:
                parent.remove(element)


def _find_or_create(parent, name):
    el = parent.find(name)
    if el is not None:
        return el
    return ElementTree.SubElement(parent, name)


class DomainProperties(object):
    """Helper class to hold properties of a libvirt VM.
    Several build attributes (NUMA placement, huge pages, ...) can be extracted
    from the running configuration to determine how to perform operations."""
    NUMA_SPREAD = 'spread'
    NUMA_AUTO = 'auto'
    NUMA_UNBOUND = 'unbound'
    NUMA_UNKNOWN = 'unknown'

    def __init__(self, hypervisor, vm):
        self._hypervisor = hypervisor
        self._vm = vm
        self._domain = None
        self.uuid = uuid4()
        self.qemu_version = _get_qemu_version(hypervisor)
        self.hugepages = False
        self.num_nodes = hypervisor.num_numa_nodes()
        self.max_cpus = max(KVM_DEFAULT_MAX_CPUS, vm.dataset_obj['num_cpu'])
        self.max_cpus = min(self.max_cpus, hypervisor.dataset_obj['num_cpu'])
        self.max_mem = hypervisor.vm_max_memory(vm)
        self.numa_mode = self.NUMA_SPREAD
        self.mem_hotplug = (self.qemu_version >= (2, 3))
        self.mem_balloon = False
        if len(vm.dataset_obj['mac']) == 0:
            self.mac_address = _generate_mac_address(
                vm.dataset_obj['object_id']
            )
            vm.dataset_obj['mac'] = [self.mac_address]
        else:
            # Opportunistic algorighm: get *any* MAC from Serveradmin
            self.mac_address = next(iter(vm.dataset_obj['mac']))
        if vm.dataset_obj['os'] in ['wheezy', 'jessie', 'stretch']:
            self.boot_type = 'grub'
            self.kernel_image = '/var/lib/libvirt/boot/grub2.img'
        elif vm.dataset_obj['os'] in ['freebsd10', 'freebsd11']:
            self.boot_type = 'freebsd'

    def info(self):
        """Returns a dictionary with user-exposable information."""
        return {
            k: v
            for k, v in vars(self).items()
            if not k.startswith('_')
        }

    @classmethod
    def from_running(cls, hypervisor, vm, domain):
        xml = domain.XMLDesc()
        tree = ElementTree.fromstring(xml)

        self = cls(hypervisor, vm)
        self._domain = domain
        self.uuid = domain.UUIDString()
        self.hugepages = tree.find('memoryBacking/hugepages') is not None
        self.num_nodes = max(len(tree.findall('cpu/numa/cell')), 1)
        self.max_cpus = domain.vcpusFlags(VIR_DOMAIN_VCPU_MAXIMUM)
        self.mem_hotplug = tree.find('maxMemory') is not None

        memballoon = tree.find('devices/memballoon')
        if memballoon is not None and \
                memballoon.attrib.get('model') == 'virtio':
            self.mem_balloon = True

        # maxMemory() returns the current memory, even if a maxMemory node is
        # present.
        if not self.mem_hotplug:
            self.max_mem = domain.maxMemory()
        else:
            self.max_mem = parse_size(
                tree.find('maxMemory').text +
                tree.find('maxMemory').attrib['unit'],
                'M',
            )

        self.current_memory = parse_size(
            tree.find('memory').text + tree.find('memory').attrib['unit'],
            'M',
        )
        self.mac_address = tree.find('devices/interface/mac').attrib['address']

        if self.num_nodes > 1:
            self.numa_mode = self.NUMA_SPREAD
        elif re.search(r'placement=.?auto', xml):
            self.numa_mode = self.NUMA_AUTO
        # Domain is unbound if it is allowed to run on all available cores.
        elif all(all(p for p in pcpus) for pcpus in domain.vcpuPinInfo()):
            self.numa_mode = self.NUMA_UNBOUND
        else:
            log.warning(
                'Cannot determine NUMA of "{}" for KVM.'
                .format(vm.fqdn)
            )
            self.numa_node = self.NUMA_UNKNOWN
        return self

    def __repr__(self):
        return '<DomainProperties:{}>'.format(self.__dict__)


def set_vcpus(hypervisor, vm, domain, num_cpu):
    """Changes the number of active VCPUs."""
    props = DomainProperties.from_running(hypervisor, vm, domain)
    if num_cpu > props.max_cpus:
        raise HypervisorError(
            'VM can not receive more than {} VCPUs'
            .format(props.max_cpus)
        )

    # Note: We could support the guest agent in here by first trying the
    #       VIR_DOMAIN_VCPU_GUEST flag. This would allow live shrinking.
    #       However, changes via the guest agent are not persisted in the
    #       config (another run with VIR_DOMAIN_AFFECT_CONFIG doesn't help),
    #       so the VM will be back to the old value after the next reboot.

    try:
        domain.setVcpusFlags(
            num_cpu, VIR_DOMAIN_AFFECT_LIVE | VIR_DOMAIN_AFFECT_CONFIG
        )
    except libvirtError as e:
        raise HypervisorError('setVcpus failed: {}'.format(e))

    # Properly pin all new VCPUs
    _live_repin_cpus(domain, props, hypervisor.dataset_obj['num_cpu'])
    # We used to set CPUs online here but now we have udev rule for that.


def _live_repin_cpus(domain, props, max_phys_cpus):
    """Adjusts NUMA pinning of all VCPUs."""
    if props.numa_mode != props.NUMA_SPREAD:
        log.warning(
            'Skipping CPU re-pin, VM is in NUMA mode "{}"'
            .format(props.numa_mode)
        )
        return

    num_nodes = props.num_nodes
    for vcpu, mask in enumerate(domain.vcpuPinInfo()):
        mask = list(mask)
        # Set interleaving NUMA pinning for each VCPU up to the maximum
        for pcpu in range(0, max_phys_cpus):
            mask[pcpu] = (pcpu % num_nodes == vcpu % num_nodes)
        # And disable all above the threshold
        # (Useful when migrating to a host with less CPUs)
        for pcpu in range(max_phys_cpus, len(mask)):
            mask[pcpu] = False
        domain.pinVcpu(vcpu, tuple(mask))


def migrate_background(
    domain, source, destination,
    migrate_params, migrate_flags,
):
    # As it seems it is possible to call multiple functions in parallel
    # from different threads.
    try:
        domain.migrateToURI3(
            MIGRATE_CONFIG.get(
                (source.dataset_obj['os'], destination.dataset_obj['os'])
            )['uri'].format(destination=destination.fqdn),
            migrate_params,
            migrate_flags,
        )
    except libvirtError as e:
        if virGetLastError()[0] == VIR_ERR_OPERATION_ABORTED:
            raise MigrationAborted('Migration aborted by user')
        raise MigrationError(e)


def migrate_live(source, destination, vm, domain):
    """Live-migrates a VM via libvirt."""

    # Reduce CPU pinning to minimum number of available cores on both
    # hypervisors to avoid "invalid cpuset" errors.
    props = DomainProperties.from_running(source, vm, domain)
    _live_repin_cpus(
        domain,
        props,
        min(source.dataset_obj['num_cpu'], destination.dataset_obj['num_cpu']),
    )

    migrate_flags = (
        VIR_MIGRATE_LIVE |  # Do it live
        VIR_MIGRATE_PERSIST_DEST |  # Define the VM on the new host
        VIR_MIGRATE_CHANGE_PROTECTION |  # Protect source VM
        VIR_MIGRATE_NON_SHARED_DISK |  # Copy non-shared storage
        VIR_MIGRATE_AUTO_CONVERGE |  # Slow down VM if can't migrate memory
        VIR_MIGRATE_ABORT_ON_ERROR # Don't tolerate soft errors
    )

    migrate_params = {
    }

    # Append OS-specific migration commands.  They might not exist for some
    # combinations but this should have already been checked by the caller.
    migrate_flags |= MIGRATE_CONFIG.get(
        (source.dataset_obj['os'], destination.dataset_obj['os'])
    )['flags']

    log.info('Starting online migration of vm {} from {} to {}'.format(
        vm, source, destination,
    ))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    future = executor.submit(
        migrate_background,
        domain, source, destination,
        migrate_params, migrate_flags,
    )

    try:
        while future.running():
            try:
                js = domain.jobStats()
            except libvirtError:
                # When migration is finished, jobStats will fail
                break
            if 'memory_total' in js and 'disk_total' in js:
                log.info(
                    (
                        'Migration progress: '
                        'disk {:.0f}% {:.0f}/{:.0f}MiB, '
                        'memory {:.0f}% {:.0f}/{:.0f}MiB, '
                    ).format(
                        js['disk_processed'] / (js['disk_total']+1) * 100,
                        js['disk_processed']/1024/1024,
                        js['disk_total']/1024/1024,
                        js['memory_processed'] / (js['memory_total']+1) * 100,
                        js['memory_processed']/1024/1024,
                        js['memory_total']/1024/1024,
                    ))
            else:
                log.info('Waiting for migration stats to show up')
            time.sleep(1)
    except KeyboardInterrupt:
        domain.abortJob()
        log.info('Awaiting migration to abort')
        future.result()
        # Nothing to log, the function above raised an exception
    else:
        log.info('Awaiting migration to finish')
        future.result() # Exception from slave thread will re-raise here
        log.info('Migration finished')
        
        # And pin again, in case we migrated to a host with more physical cores
        domain = destination._get_domain(vm)
        _live_repin_cpus(domain, props, destination.dataset_obj['num_cpu'])


def set_memory(hypervisor, vm, domain):
    """Changes the amount of memory of a VM."""
    props = DomainProperties.from_running(hypervisor, vm, domain)

    if props.mem_balloon:
        log.info('Attempting to increase memory with ballooning')
        try:
            domain.setMemoryFlags(
                vm.dataset_obj['memory'] * 1024,
                VIR_DOMAIN_AFFECT_LIVE | VIR_DOMAIN_AFFECT_CONFIG,
            )
            return
        except libvirtError:
            log.info(
                'Adding memory via ballooning failed, falling back to hotplug'
            )

    if props.mem_hotplug:
        add_memory = vm.dataset_obj['memory'] - props.current_memory
        assert add_memory > 0
        assert add_memory % (128 * props.num_nodes) == 0
        _attach_memory_dimms(vm, domain, props, add_memory)
        # We used to set DIMMS online here but now we have udev rule for that.
        return

    raise HypervisorError(
        '"{}" does not support any known memory extension strategy. '
        'You will have to power off the machine and do it offline.'
        .format(vm.fqdn)
    )


def _attach_memory_dimms(vm, domain, props, memory_mib):
    """Attaches memory DIMMs of the given size."""

    dimm_size = int(memory_mib / props.num_nodes)
    for i in range(0, props.num_nodes):
        xml = (
            "<memory model='dimm'>"
            "<target><size unit='MiB'>{}</size><node>{}</node></target>"
            "</memory>"
            .format(dimm_size, i)
        )

        domain.attachDeviceFlags(
            xml, VIR_DOMAIN_AFFECT_LIVE | VIR_DOMAIN_AFFECT_CONFIG
        )

    log.info(
        'KVM: Added {} DIMMs with {} MiB each'
        .format(props.num_nodes, dimm_size)
    )


def _generate_mac_address(object_id):
    octets = tuple(object_id >> (8 * i) & 0xff for i in range(0, 3))
    mac_address = MAC_ADDRESS_PREFIX + octets
    assert len(mac_address) == 6
    return ':'.join(format(d, '02x') for d in mac_address)


def generate_domain_xml(hypervisor, vm):
    """Generates the domain XML for a VM."""
    # Note: We make no attempts to import anything from a previously defined
    #       VM, instead the VM is updated to the latest settings.
    #       Every KVM setting should be configurable via Serveradmin anyway.
    props = DomainProperties(hypervisor, vm)
    vlan_network = hypervisor.get_vlan_network(vm.dataset_obj['intern_ip'])

    config = {
        'name': vm.uid_name,
        'disk_pool': VG_NAME,
        'disk_volume': hypervisor.get_volume_by_vm(vm).name(),
        'memory': vm.dataset_obj['memory'],
        'num_cpu': vm.dataset_obj['num_cpu'],
        'props': props,
        'vlan_tag': vlan_network['vlan_tag'],
    }

    jenv = Environment(loader=PackageLoader('igvm', 'templates'))
    domain_xml = jenv.get_template('domain.xml').render(**config)

    tree = ElementTree.fromstring(domain_xml)

    if props.qemu_version >= (2, 3):
        _set_cpu_model(hypervisor, vm, tree)
        _place_numa(hypervisor, vm, tree, props)

    log.info('KVM: VCPUs current: {} max: {} available on host: {}'.format(
        vm.dataset_obj['num_cpu'],
        props.max_cpus,
        hypervisor.dataset_obj['num_cpu'],
    ))
    if props.mem_hotplug:
        _set_memory_hotplug(vm, tree, props)
        log.info('KVM: Memory hotplug enabled, up to {} MiB'.format(
            props.max_mem,
        ))
    else:
        log.info('KVM: Memory hotplug disabled, requires qemu 2.3')

    # Remove whitespace and re-indent properly.
    out = re.sub(b'>\s+<', b'><', ElementTree.tostring(tree))
    domain_xml = minidom.parseString(out).toprettyxml()
    return domain_xml


def _get_qemu_version(hypervisor):
    version = hypervisor.conn().getVersion()
    # According to documentation:
    # value is major * 1,000,000 + minor * 1,000 + release
    release = version % 1000
    minor = int(version / 1000 % 1000)
    major = int(version / 1000000 % 1000000)
    return major, minor, release


def _set_cpu_model(hypervisor, vm, tree):
    """
    Selects CPU model based on hardware model.
    """
    hw_model = hypervisor.dataset_obj['hardware_model']

    for arch, models in KVM_HWMODEL_TO_CPUMODEL.items():
        if hw_model in models:
            cpu = _find_or_create(tree, 'cpu')
            cpu.attrib.update({
                'match': 'exact',
                'mode': 'custom',
            })
            model = _find_or_create(cpu, 'model')
            model.attrib.update({
                'fallback': 'allow',
            })
            model.text = arch
            log.info('KVM: CPU model set to "%s"' % arch)
            return
    raise HypervisorError(
        'No CPU configuration for hardware model "{}"'.format(hw_model)
    )


def _set_memory_hotplug(vm, tree, props):
    tree.find('vcpu').attrib['placement'] = 'static'
    max_memory = _find_or_create(tree, 'maxMemory')
    max_memory.attrib.update({
        'slots': '16',
        'unit': 'MiB',
    })
    max_memory.text = str(props.max_mem)


def _place_numa(hypervisor, vm, tree, props):
    """
    Configures NUMA placement.
    """
    num_vcpus = props.max_cpus

    # Which physical CPU belongs to which physical node
    pcpu_sets = hypervisor.run(
        'cat /sys/devices/system/node/node*/cpulist',
        silent=True,
    ).splitlines()
    num_nodes = len(pcpu_sets)
    assert num_nodes == len(pcpu_sets)
    nodeset = ','.join(str(i) for i in range(0, num_nodes))

    # Clean up stuff we're gonna overwrite anyway.
    _del_if_exists(tree, 'numatune/memnode')
    _del_if_exists(tree, 'cputune/vcpupin')
    _del_if_exists(tree, 'cpu/topology')
    _del_if_exists(tree, 'cpu/numa')

    if props.numa_mode == DomainProperties.NUMA_SPREAD:
        # We currently don't have any other hypervisors, so this script *might*
        # do something weird.
        # You may remove this check if it ever triggers and you've verified
        # that it actually did something sane.
        if len(pcpu_sets) != 2:
            log.warn('WARNING: Found {0} NUMA nodes instead of 2. '
                     'Please double-check the placement!')
            log.warn('Waiting ten seconds to annoy you... :-)')
            time.sleep(10)

        # Virtual node -> virtual cpu
        vcpu_sets = [
            ','.join(str(j) for j in range(i, num_vcpus, num_nodes))
            for i in range(0, num_nodes)
        ]

        # Static vcpu pinning
        tree.find('vcpu').attrib['placement'] = 'static'

        # <cpu>
        # Expose N NUMA nodes (= sockets+ to the guest, each with a
        # proportionate amount of VCPUs.
        cpu = _find_or_create(tree, 'cpu')
        topology = ElementTree.SubElement(cpu, 'topology')
        topology.attrib = {
            'sockets': str(num_nodes),
            'cores': str(num_vcpus // num_nodes),
            'threads': str(1),
        }
        # </cpu>

        # <cputune>
        # Bind VCPUs of each guest node to the corresponding host CPUs on the
        # same node.
        cputune = _find_or_create(tree, 'cputune')
        for i in range(0, num_vcpus):
            vcpupin = ElementTree.SubElement(cputune, 'vcpupin')
            vcpupin.attrib = {
                'vcpu': str(i),
                'cpuset': pcpu_sets[i % num_nodes],
            }
        # </cputune>

        # <numa><cell>
        # Expose equal slices of RAM to each guest node.
        numa = ElementTree.SubElement(cpu, 'numa')
        for i, cpuset in enumerate(vcpu_sets):
            cell = ElementTree.SubElement(numa, 'cell')
            cell.attrib = {
                'id': str(i),
                'cpus': cpuset,
                'memory': str(vm.dataset_obj['memory'] // num_nodes),
                'unit': 'MiB',
            }
        # </cell></numa>
        # </cpu>

        # Hugepages appear to be incompatible with NUMA policies.
        if not props.hugepages:
            # <numatune>
            # Map VCPUs to guest NUMA nodes.
            numatune = _find_or_create(tree, 'numatune')
            memory = _find_or_create(numatune, 'memory')
            memory.attrib['mode'] = 'strict'
            memory.attrib['nodeset'] = nodeset
            for i in range(0, num_nodes):
                memnode = ElementTree.SubElement(numatune, 'memnode')
                memnode.attrib = {
                    'cellid': str(i),
                    'nodeset': str(i),
                    'mode': 'preferred',
                }
            # </numatune>
    else:
        raise NotImplementedError(
            'NUMA mode not supported: {0}'
            .format(props.numa_mode)
        )
