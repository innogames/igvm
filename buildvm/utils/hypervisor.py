import os
import uuid

from fabric.api import env, run, puts
from fabric.contrib.files import exists, upload_template

from jinja2 import Environment, FileSystemLoader

from buildvm.utils import cmd, fail_gracefully
from buildvm.utils.virtutils import get_virtconn

run = fail_gracefully(run)
exists = fail_gracefully(exists)
upload_template = fail_gracefully(upload_template)

class HypervisorError(Exception):
    pass

def get_hypervisor():
    if exists('/var/lib/libvirt'):
        return 'libvirt-xen'
    elif exists('/proc/xen'):
        return 'xen'
    else:
        raise HypervisorError('No hypervisor found')

def create_sxp(hostname, num_vcpus, mem_size, max_mem, device):
    dest = os.path.join('/etc/xen/domains', hostname + '.sxp')
    upload_template('templates/etc/xen/domains/hostname.sxp', dest, {
        'hostname': hostname,
        'num_vcpus': num_vcpus,
        'mem_size': mem_size,
        'max_mem': max_mem,
        'device': device,
    }, use_jinja=True)

def start_machine_xm(hostname):
    sxp_file = os.path.join('/etc/xen/domains', hostname + '.sxp')
    run(cmd('xm create {0}', sxp_file))

def create_domain_xml(hostname, num_vcpus, mem_size, max_mem, device):
    jenv = Environment(loader=FileSystemLoader('templates'))
    domain_xml = jenv.get_template('libvirt/domain.xml').render(**{
        'hostname': hostname,
        'uuid': uuid.uuid1(),
        'num_vcpus': num_vcpus,
        'mem_size': mem_size,
        'max_mem': max_mem,
        'device': device
    })
    return domain_xml

def create_domain(domain_xml, hypervisor):
    conn = get_virtconn(env.host_string, hypervisor)
    puts('Defining domain on libvirt')
    conn.defineXML(domain_xml)

def start_machine_libvirt(hostname, hypervisor):
    conn = get_virtconn(env.host_string, hypervisor)
    puts('Starting domain on libvirt')
    domain = conn.lookupByName(hostname)
    domain.create()

def create_definition(hostname, num_vcpus, mem_size, max_mem, device, hypervisor):
    if hypervisor == 'libvirt-xen':
        xml = create_domain_xml(hostname, num_vcpus, mem_size, max_mem, device)
        print xml
        return create_domain(xml, hypervisor)
    elif hypervisor == 'xen':
        return create_sxp(hostname, num_vcpus, mem_size, max_mem, device)

def start_machine(hostname, hypervisor):
    if hypervisor == 'libvirt-xen':
        start_machine_libvirt(hostname, hypervisor)
    elif hypervisor == 'xen':
        start_machine_xm(hostname)


