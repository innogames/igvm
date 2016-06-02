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
from igvm.utils.storage import (
    create_storage,
    format_storage,
    get_vm_volume,
    mount_temp,
    remove_logical_volume,
    remove_temp,
    umount_temp,
)

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

    def __init__(self, admintool):
        super(Hypervisor, self).__init__(admintool)

        # Store per-VM path information
        # We cannot store these in the VM object due to migrations.
        self._disk_path = {}
        self._mount_path = {}

    def vm_disk_path(self, vm):
        """Returns the disk device path for a VM."""
        if vm not in self._disk_path:
            self._disk_path[vm] = get_vm_volume(self, vm)
        return self._disk_path[vm]

    def vm_mount_path(self, vm):
        """Returns the mount path for a VM.
        Raises HypervisorError if not mounted."""
        if vm not in self._mount_path:
            raise HypervisorError(
                '{} is not mounted on {}'
                .format(vm.hostname, self.hostname)
            )
        return self._mount_path[vm]
    
    def vlan_for_vm(self, vm):
        """Returns the VLAN number a VM should use on this hypervisor.
        None for untagged."""
        hv_vlans = self.admintool.get('network_vlans', [])
        vm_vlan = vm.network_config['vlan']
        if not hv_vlans:
            if self.network_config['vlan'] != vm_vlan:
                raise HypervisorError(
                    'Destination Hypervisor is not on same VLAN {0} as VM {1}.'
                    .format(self.network_config['vlan'], vm_vlan)
                )
            # For untagged Hypervisors VM must be untagged, too.
            return None

        if vm_vlan not in hv_vlans:
            raise HypervisorError(
                'Destination Hypervisor does not support VLAN {0}.'
                .format(vm_vlan)
            )
        return vm_vlan

    def check_vm(self, vm):
        """Checks whether a VM can run on this hypervisor."""
        # TODO: More checks in here.

        # Proper VLAN?
        self.vlan_for_vm(vm)

    def check_migration(self, vm, dst_hv, offline):
        """Checks whether a VM can be migrated to the given hypervisor."""

        if self.hostname == dst_hv.hostname:
            raise HypervisorError(
                'Source and destination Hypervisor is the same machine {0}!'
                .format(self.hostname)
            )

        if not offline and type(self) != type(dst_hv):
            raise HypervisorError(
                'Online migration between different hypervisor technologies'
                'is not supported.'
            )

    def create_vm(self, **kwargs):
        raise NotImplementedError(type(self).__name__)

    def start_vm(self, vm):
        raise NotImplementedError(type(self).__name__)

    def vm_running(self, vm):
        raise NotImplementedError(type(self).__name__)

    def vm_defined(self, vm):
        raise NotImplementedError(type(self).__name__)

    def stop_vm(self, vm):
        raise NotImplementedError(type(self).__name__)

    def stop_vm_force(self, vm):
        raise NotImplementedError(type(self).__name__)

    def undefine_vm(self, vm):
        raise NotImplementedError(type(self).__name__)

    def create_vm_storage(self, vm):
        """Allocate storage for a VM. Returns the disk path."""
        assert vm not in self._disk_path, 'Disk already created?'

        self._disk_path[vm] = create_storage(self, vm)
        return self._disk_path[vm]

    def format_vm_storage(self, vm):
        """Create new filesystem for VM and mount it. Returns mount path."""
        assert vm not in self._mount_path, 'Filesystem is already mounted'

        if self.vm_defined(vm):
            raise HypervisorError(
                'Refusing to format storage of defined VM {}'
                .format(vm.hostname)
            )

        format_storage(self, self.vm_disk_path(vm))
        return self.mount_vm_storage(vm)

    def mount_vm_storage(self, vm):
        """Mount VM filesystem on host and return mount point."""
        if vm in self._mount_path:
            return self._mount_path[vm]

        if self.vm_defined(vm) and self.vm_running(vm):
            raise HypervisorError('Refusing to mount VM filesystem while VM is powered on')

        self._mount_path[vm] = mount_temp(
            self,
            self.vm_disk_path(vm),
            suffix='-'+vm.hostname,
        )
        return self._mount_path[vm]

    def umount_vm_storage(self, vm):
        """Unmount VM filesystem."""
        if vm not in self._mount_path:
            return
        umount_temp(self, self._mount_path[vm])
        remove_temp(self, self._mount_path[vm])
        del self._mount_path[vm]

    def destroy_vm_storage(self, vm):
        """Delete logical volume of a VM."""
        if self.vm_defined(vm):
            raise HypervisorError(
                'Refusing to delete storage of defined VM {}'
                .format(vm.hostname)
            )
        remove_logical_volume(self, self.vm_disk_path(vm))
        del self._disk_path[vm]


class KVMHypervisor(Hypervisor):
    def check_migration(self, vm, dst_hv, offline):
        super(KVMHypervisor, self).check_migration(vm, dst_hv, offline)

        # Online migration only works with the same VLAN
        if not offline and self.vlan_for_vm(vm) != dst_hv.vlan_for_vm(vm):
            raise HypervisorError(
                'Online migration is not possible with the current network '
                'configuration (different VLAN).'
            )

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
        config['vlan_tag'] = self.vlan_for_vm(vm)

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

    def vm_defined(self, vm):
        # Don't use lookupByName, it prints ugly messages to the console
        conn = get_virtconn(self.hostname, 'kvm')
        return vm.hostname in [dom.name() for dom in conn.listAllDomains()]

    def vm_running(self, vm):
        conn = get_virtconn(self.hostname, 'kvm')

        # This only returns list of running domain ids.
        domain_ids = conn.listDomainsID()
        if domain_ids is None:
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
    def check_migration(self, vm, dst_hv, offline):
        super(XenHypervisor, self).check_migration(vm, dst_hv, offline)
        if not offline:
            raise HypervisorError(
                '{} does not support online migration.'
                .format(self.hostname)
            )

    def _sxp_path(self, vm):
        return os.path.join('/etc/xen/domains', vm.hostname + '.sxp')

    def create_vm(self, vm, config):
        sxp_file = config.get('sxp_file')
        if sxp_file is None:
            sxp_file = 'etc/xen/domains/hostname.sxp'
        config['hostname'] = vm.hostname

        upload_template(sxp_file, self._sxp_path(vm), config)

    def start_vm(self, vm):
        log.debug('Starting {} on {}'.format(
                vm.hostname, self.hostname))
        with settings(host_string=self.hostname):
            run(cmd('xm create {0}', self._sxp_path(vm)))

    def vm_defined(self, vm):
        path = self._sxp_path(vm)
        with settings(host_string=self.hostname):
            return exists(path, use_sudo=False, verbose=False)

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
            run(cmd('rm {0}', self._sxp_path(vm)))
