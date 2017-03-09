import logging
import re
import time
import uuid
from xml.dom import minidom
import xml.etree.ElementTree as ET

import libvirt

from igvm.exceptions import (
    HypervisorError,
    IGVMError,
)
from igvm.settings import (
    KVM_DEFAULT_MAX_CPUS,
    KVM_HWMODEL_TO_CPUMODEL,
    MAC_ADDRESS_PREFIX,
)
from igvm.utils.units import parse_size

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
    return ET.SubElement(parent, name)


def _num_numa_nodes(host):
    """Returns the number of NUMA nodes on a host."""
    # TODO: Is there an API way to query number of NUMA nodes via libvirt?
    return int(host.run(
        'cat /sys/devices/system/node/node*/cpulist | wc -l',
        silent=True,
    ))


class DomainProperties(object):
    """Helper class to hold properties of a libvirt VM.
    Several build attributes (NUMA placement, huge pages, ...) can be extracted
    from the running configuration to determine how to perform operations."""
    NUMA_SPREAD = 'spread'
    NUMA_AUTO = 'auto'
    NUMA_UNBOUND = 'unbound'
    NUMA_UNKNOWN = 'unknown'

    def __init__(self, hv, vm):
        self._hv = hv
        self._vm = vm
        self._domain = None
        self.uuid = uuid.uuid1()
        self.qemu_version = _get_qemu_version(hv)
        self.hugepages = False
        self.num_nodes = _num_numa_nodes(hv)
        self.max_cpus = max(KVM_DEFAULT_MAX_CPUS, vm.admintool['num_cpu'])
        self.max_cpus = min(self.max_cpus, hv.num_cpus)
        self.max_mem = hv.vm_max_memory(vm)
        self.numa_mode = self.NUMA_SPREAD
        self.mem_hotplug = (self.qemu_version >= (2, 3))
        self.mem_balloon = False
        self.mac_address = _generate_mac_address(vm.admintool['intern_ip'])

    def info(self):
        """Returns a dictionary with user-exposable information."""
        return {
            k: v
            for k, v in vars(self).iteritems()
            if not k.startswith('_')
        }

    @classmethod
    def from_running(cls, hv, vm, domain):
        xml = domain.XMLDesc()
        tree = ET.fromstring(xml)

        self = cls(hv, vm)
        self._domain = domain
        self.uuid = domain.UUIDString()
        self.hugepages = tree.find('memoryBacking/hugepages') is not None
        self.num_nodes = max(len(tree.findall('cpu/numa/cell')), 1)
        self.max_cpus = domain.vcpusFlags(libvirt.VIR_DOMAIN_VCPU_MAXIMUM)
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
                tree.find('memory').attrib.get('unit', 'KiB'),
                'M'
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
            log.warning('KVM: Cannot determine NUMA of {}'.format(vm.hostname))
            self.numa_node = self.NUMA_UNKNOWN
        return self

    def __repr__(self):
        return '<DomainProperties:{}>'.format(self.__dict__)


def set_vcpus(hv, vm, domain, num_cpu):
    """Changes the number of active VCPUs."""
    props = DomainProperties.from_running(hv, vm, domain)
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
            num_cpu,
            libvirt.VIR_DOMAIN_AFFECT_LIVE |
            libvirt.VIR_DOMAIN_AFFECT_CONFIG,
        )
    except libvirt.libvirtError as e:
        raise HypervisorError('setVcpus failed: {}'.format(e))

    # Properly pin all new VCPUs
    _live_repin_cpus(domain, props, hv.num_cpus)

    # Activate all CPUs in the guest
    log.info('KVM: Activating new CPUs in guest')
    vm.run(
        'echo 1 | tee /sys/devices/system/cpu/cpu*/online'
    )


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


def migrate_live(source_hv, destination_hv, vm, domain):
    """Live-migrates a VM via libvirt."""
    # Unfortunately, virsh provides a global timeout, but what we need it to
    # timeout if it is catching up the dirtied memory.  To be in this stage,
    # it should have coped the initial disk and memory and changes on them.
    timeout = sum((
        # We assume the disk can be copied at 33 MB/s;
        vm.admintool['disk_size_gib'] * 1024 / 33,
        # the memory at 100 MB/s;
        vm.admintool['memory'] / 100,
        # and 5 minutes more for other operations.
        5 * 60,
    ))

    migrate_cmd = (
        'virsh migrate'
        # Do it live!
        ' --live'
        ' --copy-storage-all'
        # Define the VM on the new host
        ' --persistent'
        # Don't let the VM configuration to be changed
        ' --change-protection'
        # Force convergence, # otherwise migrations never end
        ' --auto-converge'
        ' --domain {vm_hostname}'
        # Don't tolerate soft errors
        ' --abort-on-error'
        # We need SSH agent forwarding
        ' --desturi qemu+ssh://{dsthv_hostname}/system'
        # Force guest to suspend, if noting else helped
        ' --timeout {timeout}'
        ' --verbose'
    )

    # Reduce CPU pinning to minimum number of available cores on both HVs to
    # avoid "invalid cpuset" errors.
    props = DomainProperties.from_running(source_hv, vm, domain)
    _live_repin_cpus(
        domain,
        props,
        min(source_hv.num_cpus, destination_hv.num_cpus),
    )

    source_hv.accept_ssh_hostkey(destination_hv)
    source_hv.run(migrate_cmd.format(
        vm_hostname=vm.hostname,
        dsthv_hostname=destination_hv.hostname,
        timeout=timeout,
    ))

    # And pin again, in case we migrated to a host with more physical cores
    domain = destination_hv._domain(vm)
    _live_repin_cpus(domain, props, destination_hv.num_cpus)


def set_memory(hv, vm, domain, memory_mib):
    """Changes the amount of memory of a VM."""
    props = DomainProperties.from_running(hv, vm, domain)

    if props.mem_balloon:
        log.info('Attempting to increase memory with ballooning')
        try:
            domain.setMemoryFlags(
                memory_mib * 1024,
                libvirt.VIR_DOMAIN_AFFECT_LIVE |
                libvirt.VIR_DOMAIN_AFFECT_CONFIG,
            )
            return
        except libvirt.libvirtError:
            log.info(
                'virsh setmem failed, falling back to hotplug'
            )

    if props.mem_hotplug:
        add_memory = memory_mib - vm.admintool['memory']
        assert add_memory > 0
        _attach_memory_dimms(vm, domain, props, add_memory)
        return

    raise HypervisorError(
        '{} does not support any known memory extension strategy. '
        'You will have to power off the machine and do it offline.'
        .format(vm.hostname)
    )


def _attach_memory_dimms(vm, domain, props, memory_mib):
    """Attaches memory DIMMs of the given size."""
    # https://medium.com/@juergen_thomann/memory-hotplug-with-qemu-kvm-and-libvirt-558f1c635972#.sytig6o9h
    if memory_mib % (128 * props.num_nodes):
        raise IGVMError(
            'Added memory must be multiple of 128 MiB * <number of NUMA nodes>'
        )

    dimm_size = int(memory_mib / props.num_nodes)
    for i in range(0, props.num_nodes):
        xml = (
            "<memory model='dimm'>"
            "<target><size unit='MiB'>{}</size><node>{}</node></target>"
            "</memory>"
            .format(dimm_size, i)
        )

        domain.attachDeviceFlags(
            xml,
            libvirt.VIR_DOMAIN_AFFECT_LIVE | libvirt.VIR_DOMAIN_AFFECT_CONFIG,
        )

    log.info(
        'KVM: Added {} DIMMs with {} MiB each'
        .format(props.num_nodes, dimm_size)
    )

    # Now activate all DIMMs in the guest
    log.info('KVM: Activating new DIMMs in guest')
    vm.run(
        'for i in `grep -l offline /sys/devices/system/memory/memory*/state`; '
        'do echo online > $i; done'
    )


def _generate_mac_address(ip):
    assert ip.version == 4, 'intern_ip is IPv4 address'
    ip_octets = tuple(int(c) for c in str(ip).split('.')[-3:])
    mac_address = MAC_ADDRESS_PREFIX + ip_octets
    assert len(mac_address) == 6
    return ':'.join(format(d, '02x') for d in mac_address)


def generate_domain_xml(hv, vm):
    """Generates the domain XML for a VM."""
    # Note: We make no attempts to import anything from a previously defined
    #       VM, instead the VM is updated to the latest settings.
    #       Every KVM setting should be configurable via Serveradmin anyway.
    props = DomainProperties(hv, vm)

    config = {
        'disk_device': hv.vm_disk_path(vm),
        'serveradmin': vm.admintool,
        'props': props,
        'vlan_tag': hv.vlan_for_vm(vm),
    }

    jenv = Environment(loader=PackageLoader('igvm', 'templates'))
    domain_xml = jenv.get_template('domain.xml').render(**config)

    tree = ET.fromstring(domain_xml)

    if props.qemu_version >= (2, 3):
        _set_cpu_model(hv, vm, tree)
        _place_numa(hv, vm, tree, props)

    log.info('KVM: VCPUs current: {} max: {} available on host: {}'.format(
        vm.admintool['num_cpu'], props.max_cpus, hv.num_cpus,
    ))
    if props.mem_hotplug:
        _set_memory_hotplug(vm, tree, props)
        log.info('KVM: Memory hotplug enabled, up to {} MiB'.format(
            props.max_mem,
        ))
    else:
        log.info('KVM: Memory hotplug disabled, requires qemu 2.3')

    # Remove whitespace and re-indent properly.
    out = re.sub('>\s+<', '><', ET.tostring(tree))
    domain_xml = minidom.parseString(out).toprettyxml()
    return domain_xml


def _get_qemu_version(hv):
    version = hv.conn.getVersion()
    # According to documentation:
    # value is major * 1,000,000 + minor * 1,000 + release
    release = version % 1000
    minor = int(version / 1000 % 1000)
    major = int(version / 1000000 % 1000000)
    return major, minor, release


def _set_cpu_model(hv, vm, tree):
    """
    Selects CPU model based on hardware model.
    """
    hw_model = hv.admintool.get('hardware_model')
    if not hw_model:
        return

    for arch, models in KVM_HWMODEL_TO_CPUMODEL.iteritems():
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
            break


def _set_memory_hotplug(vm, tree, props):
    tree.find('vcpu').attrib['placement'] = 'static'
    max_memory = _find_or_create(tree, 'maxMemory')
    max_memory.attrib.update({
        'slots': '16',
        'unit': 'MiB',
    })
    max_memory.text = str(props.max_mem)


def _place_numa(hv, vm, tree, props):
    """
    Configures NUMA placement.
    """
    num_vcpus = props.max_cpus

    # Which physical CPU belongs to which physical node
    pcpu_sets = hv.run(
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
        topology = ET.SubElement(cpu, 'topology')
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
            vcpupin = ET.SubElement(cputune, 'vcpupin')
            vcpupin.attrib = {
                'vcpu': str(i),
                'cpuset': pcpu_sets[i % num_nodes],
            }
        # </cputune>

        # <numa><cell>
        # Expose equal slices of RAM to each guest node.
        numa = ET.SubElement(cpu, 'numa')
        for i, cpuset in enumerate(vcpu_sets):
            cell = ET.SubElement(numa, 'cell')
            cell.attrib = {
                'id': str(i),
                'cpus': cpuset,
                'memory': str(vm.admintool['memory'] // num_nodes),
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
                memnode = ET.SubElement(numatune, 'memnode')
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
