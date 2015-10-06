import os
import time
import uuid
from StringIO import StringIO

from icinga_utils import downtimer

from fabric.api import env, run, puts
from fabric.context_managers import hide
from fabric.contrib.files import exists

from jinja2 import Environment, PackageLoader

from managevm.utils import cmd, fail_gracefully
from managevm.utils.template import upload_template
from managevm.utils.virtutils import get_virtconn, close_virtconns
from managevm.utils.resources import get_cpuinfo

run = fail_gracefully(run)
exists = fail_gracefully(exists)

class HypervisorError(Exception):
    pass

def create_sxp(hostname, num_vcpus, mem_size, max_mem, device, sxp_file=None):
    if sxp_file is None:
        sxp_file = 'etc/xen/domains/hostname.sxp'
    dest = os.path.join('/etc/xen/domains', hostname + '.sxp')
    upload_template(sxp_file, dest, {
        'hostname': hostname,
        'num_vcpus': num_vcpus,
        'mem_size': mem_size,
        'max_mem': max_mem,
        'device': device,
    })

def start_machine_xm(hostname):
    sxp_file = os.path.join('/etc/xen/domains', hostname + '.sxp')
    run(cmd('xm create {0}', sxp_file))

def create_domain_xml(hostname, num_vcpus, mem_size, max_mem, vlan, device):
    jenv = Environment(loader=PackageLoader('managevm', '../templates'))
    domain_xml = jenv.get_template('libvirt/domain.xml').render(**{
        'hostname': hostname,
        'uuid': uuid.uuid1(),
        'num_vcpus': num_vcpus,
        'mem_size': mem_size,
        'max_mem': max_mem,
        'device': device,
        'vlan': vlan,
    })
    return domain_xml

def create_domain(domain_xml, hypervisor):
    conn = get_virtconn(env.host_string, hypervisor)
    puts('Defining domain on libvirt')
    conn.defineXML(domain_xml)

    # Refresh storage pools to register the vm image
    for pool_name in conn.listStoragePools():
        pool = conn.storagePoolLookupByName(pool_name)
        pool.refresh(0)

def start_machine_libvirt(hostname, hypervisor):
    conn = get_virtconn(env.host_string, hypervisor)
    puts('Starting domain on libvirt')
    domain = conn.lookupByName(hostname)
    domain.create()

def create_definition(hostname, num_vcpus, mem_size, max_mem, vlan, device, hypervisor, hypervisor_extra):
    if hypervisor == 'kvm':
        xml = create_domain_xml(hostname, num_vcpus, mem_size, max_mem, vlan, device)
        return create_domain(xml, hypervisor)
    elif hypervisor == 'xen':
        sxp_file = hypervisor_extra.get('sxp_file')
        return create_sxp(hostname, num_vcpus, mem_size, max_mem, device, sxp_file)
    else:
        raise ValueError('Not a valid hypervisor: {0}'.format(hypervisor))

def start_machine(hostname, hypervisor):
    if hypervisor == 'kvm':
        start_machine_libvirt(hostname, hypervisor)
    elif hypervisor == 'xen':
        start_machine_xm(hostname)
    else:
        raise ValueError('Not a valid hypervisor: {0}'.format(hypervisor))

def shutdown_vm_xen(vm):
    run('xm shutdown {0}'.format(vm['hostname']))

    found = False
    for i in range(60, 1, -1):
        print("Waiting for VM to shutdown {0}".format(i))
        xmList = StringIO()
        with hide('running'):
            run("xm list", stdout=xmList)
        xmList.seek(0)
        found = False
        for xmEntry in xmList.readlines():
            if len(xmEntry.split())>=3 and xmEntry.split()[2] == vm['hostname']:
                found = True
        if found == False:
            break
        time.sleep(1)

    if found == True:
        print("WARNING: VM did not shutdown, I'm destroying it by force!")
        run('xm destroy {0}'.format(vm['hostname']))
    else:
        print("VM is shutdown.")

def shutdown_vm_kvm(vm):
    run('virsh shutdown {0}'.format(vm['hostname']))

    found = False
    for i in range(60, 1, -1):
        print("Waiting for VM to shutdown {0}".format(i))
        xmList = StringIO()
        with hide('running'):
            run("virsh list", stdout=xmList)
        xmList.seek(0)
        found = False
        for xmEntry in xmList.readlines():
            if len(xmEntry.split())>=4 and xmEntry.split()[3] == vm['hostname']:
                found = True
        if found == False:
            break
        time.sleep(1)

    if found == True:
        print("WARNING: VM did not shutdown, I'm destroying it by force!")
        run('virsh destroy {0}'.format(vm['hostname']))
    else:
        print("VM is shutdown.")

def shutdown_vm(vm, hypervisor):
    if hypervisor == "xen":
        shutdown_vm_xen(vm)
    elif hypervisor == "kvm":
        shutdown_vm_kvm(vm)
    else:
        raise Exception("Not a valid hypervisor: {0}".format(hypervisor))
    downtimer.call_icinga("down", vm['hostname'], duration=600)

def rename_old_vm(vm, date, offline, hypervisor):
    if hypervisor == "xen":
        run('mv /etc/xen/domains/{0}.sxp /etc/xen/domains/{0}.sxp.migrated.{1}'.format(vm['hostname'], date))
    elif hypervisor == "kvm" and offline == True:
        run('virsh dumpxml {0} > /etc/libvirt/qemu/{0}.xml.migrated.{1}'.format(vm['hostname'], date))
        run('virsh undefine {0}'.format(vm['hostname']))

