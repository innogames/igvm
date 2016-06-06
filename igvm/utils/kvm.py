import logging
import re
import time
import uuid
from xml.dom import minidom
import xml.etree.ElementTree as ET

from igvm.settings import (
    KVM_DEFAULT_MAX_CPUS,
    KVM_HWMODEL_TO_CPUMODEL,
)

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
            name = name[p+1:]
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


def generate_domain_xml(hv, vm):
    """Generates the domain XML for a VM."""
    version = _get_qemu_version(hv)

    config = {
        'disk_device': hv.vm_disk_path(vm),
        'serveradmin': vm.admintool,
        'uuid': uuid.uuid1(),
        'vlan_tag': hv.vlan_for_vm(vm),
        'version': version,
        'mem_hotplug': version >= (2, 3),
        'max_mem': hv.vm_max_memory(vm),
        'max_cpus': _get_max_cpus(hv, vm),
    }

    jenv = Environment(loader=PackageLoader('igvm', 'templates'))
    domain_xml = jenv.get_template('hv/domain.xml').render(**config)

    tree = ET.fromstring(domain_xml)

    if version >= (2, 3):
        _set_cpu_model(vm, tree)
        _place_numa(vm, tree, config['max_cpus'])

    log.info('KVM: VCPUs current: {} max: {} available on host: {}'.format(
        vm.admintool['num_cpu'], config['max_cpus'], hv.num_cpus,
    ))
    if config['mem_hotplug']:
        _set_memory_hotplug(vm, tree, config)
        log.info('KVM: Memory hotplug enabled, up to {} MiB'.format(
            config['max_mem'],
        ))
    else:
        log.info('KVM: Memory hotplug disabled, requires qemu 2.3')

    # Remove whitespace and re-indent properly.
    out = re.sub('>\s+<', '><', ET.tostring(tree))
    domain_xml = minidom.parseString(out).toprettyxml()
    return domain_xml


def _get_max_cpus(hv, vm):
    max_cpus = max(KVM_DEFAULT_MAX_CPUS, vm.admintool['num_cpu'])
    max_cpus = min(max_cpus, hv.num_cpus)
    return max_cpus


def _get_qemu_version(hv):
    version = hv.conn.getVersion()
    # According to documentation:
    # value is major * 1,000,000 + minor * 1,000 + release
    release = version % 1000
    minor = int(version/1000 % 1000)
    major = int(version/1000000 % 1000000)
    return major, minor, release


def _set_cpu_model(vm, tree):
    """
    Selects CPU model based on hardware model.
    """
    hw_model = vm.admintool.get('hardware_model')
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


def _set_memory_hotplug(vm, tree, config):
    tree.find('vcpu').attrib['placement'] = 'static'
    max_memory = _find_or_create(tree, 'maxMemory')
    max_memory.attrib.update({
        'slots': '16',
        'unit': 'MiB',
    })
    max_memory.text = str(config['max_mem'])


def kvm_adjust_cpuset_pre(config, offline):
    """
    Reduces the cpuset to the minimum number of CPUs on source and destination.
    """
    # TODO: why exactly is this needed?
    if config['dsthv']['hypervisor'] != 'kvm' or offline:
        return
    conn_src = config['srchv_conn']
    conn_dst = config['dsthv_conn']

    dom = conn_src.lookupByName(config['vm_hostname'])
    if re.search(r'placement=.?auto', dom.XMLDesc()):
        log.warning(
            'Skipping cpuset adjustment for old-style VM. '
            'Please rebuild or offline-migrate to apply latest KVM settings.'
        )
        return

    # https://libvirt.org/html/libvirt-libvirt-host.html#virNodeInfo
    num_cpus_src = conn_src.getInfo()[2]
    num_cpus_dst = conn_dst.getInfo()[2]

    if num_cpus_src < num_cpus_dst:
        # After migration we will need to include the additional cores from dst
        config['__postmigrate_expand_cpuset'] = num_cpus_src
        return
    elif num_cpus_src == num_cpus_dst:
        return  # Nothing to do

    log.info(
        'Target hypervisor has less cores, shrinking cpuset from {} to {} CPUs'
        .format(num_cpus_src, num_cpus_dst)
    )
    assert num_cpus_dst >= 4, 'hypervisor has at least four cores'

    for i, mask in enumerate(dom.vcpuPinInfo()):
        # Truncate CPU mask
        dom.pinVcpu(i, mask[:num_cpus_dst])


def kvm_adjust_cpuset_post(config, offline):
    """
    Includes all new physical cores in the cpuset.
    For each new core P, the bit on VCPU V equals the bit of pcpu
    P-<num nodes>.
    """
    start_cpu = config.get('__postmigrate_expand_cpuset', 0)
    if not start_cpu:
        return
    conn = config['dsthv_conn']

    info = conn.getInfo()
    num_cpus = info[2]
    num_nodes = info[4]

    log.info('Expanding cpuset from {} to {} CPUs'.format(start_cpu, num_cpus))

    dom = conn.lookupByName(config['vm_hostname'])
    for i, mask in enumerate(dom.vcpuPinInfo()):
        mask = list(mask)
        for j in range(start_cpu, num_cpus):
            mask[j] = mask[j-num_nodes]
        dom.pinVcpu(i, tuple(mask))


def _place_numa(vm, tree, max_cpus):
    """
    Configures NUMA placement.
    """
    num_vcpus = max_cpus
    numa_mode = 'spread'

    # Which physical CPU belongs to which physical node
    pcpu_sets = vm.hypervisor.run(
        'cat /sys/devices/system/node/node*/cpulist',
        silent=True,
    ).splitlines()
    num_nodes = len(pcpu_sets)
    nodeset = ','.join(str(i) for i in range(0, num_nodes))

    # Clean up stuff we're gonna overwrite anyway.
    _del_if_exists(tree, 'numatune/memnode')
    _del_if_exists(tree, 'cputune/vcpupin')
    _del_if_exists(tree, 'cpu/topology')
    _del_if_exists(tree, 'cpu/numa')

    memory_backing = tree.find('memoryBacking')
    hugepages = (
        memory_backing is not None and
        memory_backing.find('hugepages') is not None
    )

    if numa_mode == 'spread':
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
        if not hugepages:
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
            .format(numa_mode)
        )
