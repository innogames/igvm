import xml.etree.ElementTree as ET

from fabric.api import run
from managevm.signals import on_signal

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


def get_qemu_version(config):
    version = config['dsthv_conn'].getVersion()
    # According to documentation:
    # value is major * 1,000,000 + minor * 1,000 + release
    release = version % 1000
    minor = int(version/1000%1000)
    major = int(version/1000000%1000000)
    return major, minor


@on_signal('populate_config')
def kvm_populate_config(config):
    if config['dsthv']['hypervisor'] != 'kvm':
        return

    version = get_qemu_version(config)
    config['qemu_version'] = version

    config['mem_hotplug'] = (version >= (2, 3))


@on_signal('customize_kvm_xml')
def kvm_hw_model(vm, config, tree):
    """
    Selects CPU model based on hardware model.
    """
    if not 'dsthv_hw_model' in config:
        return

    model2arch = {
        'Nehalem': ['Dell_M610'],
        'SandyBridge': ['Dell_M620', 'Dell_M630']
    }
    for arch, models in model2arch.iteritems():
        if config['dsthv_hw_model'] in models:
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
            break


@on_signal('customize_kvm_xml')
def kvm_memory_hotplug(vm, config, tree):
    """
    Configures memory hotplugging.
    """
    if not config['mem_hotplug']:
        return
    
    tree.find('vcpu').attrib['placement'] = 'static'
    # maxMemory node is part of XML


@on_signal('customize_kvm_xml')
def kvm_place_numa(vm, config, tree):
    """
    Configures NUMA placement.
    """
    if get_qemu_version(config) < (2, 3):
        return

    num_vcpus = int(config['max_cpu'])
    numa_mode = config.get('numa_mode', 'spread')

    # Which physical CPU belongs to which physical node
    pcpu_sets = run('cat /sys/devices/system/node/node*/cpulist').splitlines()
    num_nodes = len(pcpu_sets)
    nodeset = ','.join(str(i) for i in range(0, num_nodes))

    # Clean up stuff we're gonna overwrite anyway.
    _del_if_exists(tree, 'numatune/memnode')
    _del_if_exists(tree, 'cputune/vcpupin')
    _del_if_exists(tree, 'cpu/topology')
    _del_if_exists(tree, 'cpu/numa')

    if numa_mode == 'spread':
        # We currently don't have any other hypervisors, so this script *might* do something weird.
        # You may remove this check if it ever triggers and you've verified that it actually did
        # something sane.
        if len(pcpu_sets) != 2:
            print('WARNING: Found {0} NUMA nodes instead of 2. Please double-check the placement!')
            print('Waiting ten seconds to annoy you... :-)')
            time.sleep(10)
    
        # Virtual node -> virtual cpu
        vcpu_sets = [','.join(str(j) for j in range(i, num_vcpus, num_nodes)) for i in range(0, num_nodes)]

        # Static vcpu pinning
        tree.find('vcpu').attrib['placement'] = 'static'

        # <cpu>
        # Expose N NUMA nodes (= sockets+ to the guest, each with a proportionate amount of VCPUs.
        cpu = _find_or_create(tree, 'cpu')
        topology = ET.SubElement(cpu, 'topology')
        topology.attrib = {
            'sockets': str(num_nodes),
            'cores': str(num_vcpus // num_nodes),
            'threads': str(1),
        }
        # </cpu>

        # <cputune>
        # Bind VCPUs of each guest node to the corresponding host CPUs on the same node.
        cputune = _find_or_create(tree, 'cputune')
        for i in range(0, num_vcpus):
            vcpupin = ET.SubElement(cputune, 'vcpupin')
            vcpupin.attrib = {
                'vcpu': str(i),
                'cpuset': pcpu_sets[i%num_nodes],
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
                'memory': str(config['mem'] // num_nodes),
                'unit': 'MiB',
            }
        # </cell></numa>
        # </cpu>

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
                'mode': 'strict',
            }
        # </numatune>
    else:
        raise Exception('NUMA mode not supported: {0}'.format(numa_mode))

