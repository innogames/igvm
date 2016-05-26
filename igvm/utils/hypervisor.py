import os
import re
import time
import uuid
from StringIO import StringIO

import xml.etree.ElementTree as ET
from xml.dom import minidom

from fabric.api import run, puts, settings
from fabric.context_managers import hide

from jinja2 import Environment, PackageLoader

from igvm.utils import cmd
from igvm.utils.template import upload_template
from igvm.utils.virtutils import get_virtconn


class HypervisorError(Exception):
    pass


class VM(object):
    """
    Hypervisor interface for VMs.
    """
    def __init__(self, hostname, hypervisor_hostname):
        self.hostname = hostname
        self.hypervisor_hostname = hypervisor_hostname

    def create(self, **kwargs):
        raise NotImplementedError(type(self).__name__)

    def start(self):
        raise NotImplementedError(type(self).__name__)

    def shutdown(self):
        raise NotImplementedError(type(self).__name__)

    def is_running(self):
        raise NotImplementedError(type(self).__name__)

    def undefine(self):
        raise NotImplementedError(type(self).__name__)

    def wait_for_running(self, running=True, timeout=60):
        """
        Waits for the VM to enter the given running state.
        Returns False on timeout, True otherwise.
        """
        action = 'boot' if running else 'shutdown'
        for i in range(timeout, 1, -1):
            print("Waiting for VM to {0} {1}".format(action, i))
            if self.is_running() == running:
                return True
            time.sleep(1)
        else:
            return False

    @staticmethod
    def get(hostname, hypervisor, hypervisor_hostname):
        if hypervisor == 'kvm':
            return KVMVM(hostname, hypervisor_hostname)
        elif hypervisor == 'xen':
            return XenVM(hostname, hypervisor_hostname)
        else:
            raise NotImplementedError('Not a valid hypervisor: {0}'.format(hypervisor))


class KVMVM(VM):
    def create(self, config):
        domain_xml = self.generate_xml(config)
        conn = get_virtconn(self.hypervisor_hostname, 'kvm')
        puts('Defining domain on libvirt')
        conn.defineXML(domain_xml)

        # Refresh storage pools to register the vm image
        for pool_name in conn.listStoragePools():
            pool = conn.storagePoolLookupByName(pool_name)
            pool.refresh(0)

    def generate_xml(self, config):
        if config.get('uuid'):
            config['uuid'] = uuid.uuid1()
        config['hostname'] = self.hostname

        jenv = Environment(loader=PackageLoader('igvm', 'templates'))
        domain_xml = jenv.get_template('libvirt/domain.xml').render(**config)

        tree = ET.fromstring(domain_xml)
        # TODO: Domain XML customization for NUMA etc

        # Remove whitespace and re-indent properly.
        out = re.sub('>\s+<', '><', ET.tostring(tree))
        domain_xml = minidom.parseString(out).toprettyxml()
        return domain_xml

    def start(self):
        conn = get_virtconn(self.hypervisor_hostname, 'kvm')
        puts('Starting domain on libvirt')
        domain = conn.lookupByName(self.hostname)
        domain.create()

    def is_running(self):
        conn = get_virtconn(self.hypervisor_hostname, 'kvm')

        # This only returns list of running domain ids.
        domain_ids = conn.listDomainsID()
        if domain_ids == None:
            raise HypervisorError('Failed to get a list of domain IDs')

        for domain_id in domain_ids:
            if self.hostname == conn.lookupByID(domain_id).name():
                return True
        return False

    def shutdown(self):
        with settings(host_string=self.hypervisor_hostname):
            run('virsh shutdown {0}'.format(self.hostname))

        if not self.wait_for_running(False):
            print("WARNING: VM did not shutdown, I'm destroying it by force!")
            with settings(host_string=self.hypervisor_hostname):
                run('virsh destroy {0}'.format(self.hostname))
        else:
            print("VM is shutdown.")

    def undefine(self):
        with settings(host_string=self.hypervisor_hostname):
            run('virsh undefine {0}'.format(self.hostname))


class XenVM(VM):
    def create(self, config):
        sxp_file = config.get('sxp_file')
        if sxp_file is None:
            sxp_file = 'etc/xen/domains/hostname.sxp'
        config['hostname'] = self.hostname

        dest = os.path.join('/etc/xen/domains', self.hostname + '.sxp')
        upload_template(sxp_file, dest, config)

    def start(self):
        sxp_file = os.path.join('/etc/xen/domains', self.hostname + '.sxp')
        with settings(host_string=self.hypervisor_hostname):
            run(cmd('xm create {0}', sxp_file))

    def is_running(self):
        xmList = StringIO()
        with settings(host_string=self.hypervisor_hostname):
            with hide('running'):
                run("xm list", stdout=xmList)
        xmList.seek(0)
        for xmEntry in xmList.readlines():
            pieces = xmEntry.split()
            if len(pieces) >= 3 and pieces[2] == self.hostname:
                return True
        return False

    def shutdown(self):
        with settings(host_string=self.hypervisor_hostname):
            run('xm shutdown {0}'.format(self.hostname))

        if not self.wait_for_running(False):
            print("WARNING: VM did not shutdown, I'm destroying it by force!")
            with settings(host_string=self.hypervisor_hostname):
                run('xm destroy {0}'.format(self.hostname))
        else:
            print("VM is shutdown.")

    def undefine(self):
        with settings(host_string=self.hypervisor_hostname):
            run('rm /etc/xen/domains/{0}.sxp'.format(self.hostname))
