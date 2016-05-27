import logging
import os
import re
import uuid
from StringIO import StringIO

import xml.etree.ElementTree as ET
from xml.dom import minidom

from adminapi.dataset import ServerObject

from fabric.api import run, puts, settings
from fabric.context_managers import hide

from jinja2 import Environment, PackageLoader

from igvm.host import Host
from igvm.utils import cmd
from igvm.utils.config import get_server
from igvm.utils.template import upload_template
from igvm.utils.virtutils import get_virtconn

log = logging.getLogger(__name__)


class HypervisorError(Exception):
    pass


class Hypervisor(Host):
    """Hypervisor interface."""

    @staticmethod
    def get(hv_admintool):
        """Factory to get matching hypervisor implementation for a VM."""
        if not isinstance(hv_admintool, ServerObject):
            hv_admintool = get_server(hv_admintool)

        if hv_admintool['hypervisor'] == 'kvm':
            cls = KVMHypervisor
        elif hv_admintool['hypervisor'] == 'xen':
            cls = XenHypervisor
        else:
            raise NotImplementedError('Not a valid hypervisor type: {}'.format(
                    hv_admintool['hypervisor']))
        return cls(hv_admintool)

    def create_vm(self, **kwargs):
        raise NotImplementedError(type(self).__name__)

    def start_vm(self, vm):
        raise NotImplementedError(type(self).__name__)

    def vm_running(self, vm):
        raise NotImplementedError(type(self).__name__)

    def stop_vm(self, vm):
        raise NotImplementedError(type(self).__name__)

    def stop_vm_force(self, vm):
        raise NotImplementedError(type(self).__name__)

    def undefine_vm(self, vm):
        raise NotImplementedError(type(self).__name__)


class KVMHypervisor(Hypervisor):
    def create_vm(self, vm, config):
        domain_xml = self.generate_xml(vm, config)
        conn = get_virtconn(self.hostname, 'kvm')
        puts('Defining domain on libvirt')
        conn.defineXML(domain_xml)

        # Refresh storage pools to register the vm image
        for pool_name in conn.listStoragePools():
            pool = conn.storagePoolLookupByName(pool_name)
            pool.refresh(0)

    def generate_xml(self, vm, config):
        if config.get('uuid'):
            config['uuid'] = uuid.uuid1()
        config['hostname'] = vm.hostname

        jenv = Environment(loader=PackageLoader('igvm', 'templates'))
        domain_xml = jenv.get_template('libvirt/domain.xml').render(**config)

        tree = ET.fromstring(domain_xml)
        # TODO: Domain XML customization for NUMA etc

        # Remove whitespace and re-indent properly.
        out = re.sub('>\s+<', '><', ET.tostring(tree))
        domain_xml = minidom.parseString(out).toprettyxml()
        return domain_xml

    def start_vm(self, vm):
        conn = get_virtconn(self.hostname, 'kvm')
        log.info('Starting {} on {}'.format(vm.hostname, self.hostname))
        domain = conn.lookupByName(vm.hostname)
        domain.create()

    def vm_running(self, vm):
        conn = get_virtconn(self.hostname, 'kvm')

        # This only returns list of running domain ids.
        domain_ids = conn.listDomainsID()
        if domain_ids == None:
            raise HypervisorError('Failed to get a list of domain IDs')

        for domain_id in domain_ids:
            if vm.hostname == conn.lookupByID(domain_id).name():
                return True
        return False

    def stop_vm(self, vm):
        log.info('Shutting down {} on {}'.format(
                vm.hostname, self.hostname))
        with settings(host_string=self.hostname):
            run('virsh shutdown {0}'.format(vm.hostname))

    def stop_vm_force(self, vm):
        log.debug('Destroying domain {} on {}'.format(
                vm.hostname, self.hostname))
        with settings(host_string=self.hostname):
            run('virsh destroy {0}'.format(vm.hostname))

    def undefine_vm(self, vm):
        # TODO: Check if still running
        with settings(host_string=self.hostname):
            run('virsh undefine {0}'.format(vm.hostname))


class XenHypervisor(Hypervisor):
    def create_vm(self, vm, config):
        sxp_file = config.get('sxp_file')
        if sxp_file is None:
            sxp_file = 'etc/xen/domains/hostname.sxp'
        config['hostname'] = vm.hostname

        dest = os.path.join('/etc/xen/domains', vm.hostname + '.sxp')
        upload_template(sxp_file, dest, config)

    def start_vm(self, vm):
        log.debug('Starting {} on {}'.format(
                vm.hostname, self.hostname))
        sxp_file = os.path.join('/etc/xen/domains', vm.hostname + '.sxp')
        with settings(host_string=self.hostname):
            run(cmd('xm create {0}', sxp_file))

    def vm_running(self, vm):
        xmList = StringIO()
        with settings(host_string=self.hostname):
            with hide('running'):
                run("xm list", stdout=xmList)
        xmList.seek(0)
        for xmEntry in xmList.readlines():
            pieces = xmEntry.split()
            if len(pieces) >= 3 and pieces[2] == vm.hostname:
                return True
        return False

    def stop_vm(self, vm):
        log.debug('Shutting down {} on {}'.format(
                vm.hostname, self.hostname))
        with settings(host_string=self.hostname):
            run('xm shutdown {0}'.format(vm.hostname))

    def stop_vm_force(self, vm):
        log.debug('Destroying domain {} on {}'.format(
                vm.hostname, self.hostname))
        with settings(host_string=self.hostname):
            run('xm destroy {0}'.format(vm.hostname))

    def undefine_vm(self, vm):
        # TODO: Check if still running
        with settings(host_string=self.hostname):
            run('rm /etc/xen/domains/{0}.sxp'.format(vm.hostname))
