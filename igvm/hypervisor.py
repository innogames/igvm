import logging
import math
import os
import libvirt

from adminapi.dataset import ServerObject

from fabric.api import run
from fabric.contrib.files import exists

from igvm.exceptions import ConfigError, HypervisorError
from igvm.host import Host, get_server
from igvm.settings import HOST_RESERVED_MEMORY
from igvm.utils import cmd
from igvm.utils.kvm import generate_domain_xml
from igvm.utils.lazy_property import lazy_property
from igvm.utils.storage import (
    create_storage,
    format_storage,
    get_logical_volumes,
    get_vm_volume,
    mount_temp,
    remove_logical_volume,
    remove_temp,
    umount_temp,
)
from igvm.utils.template import upload_template
from igvm.utils.virtutils import get_virtconn

log = logging.getLogger(__name__)


class Hypervisor(Host):
    """Hypervisor interface."""

    @staticmethod
    def get(hv_admintool):
        """Factory to get matching hypervisor implementation for a VM."""
        # TODO We are not validating the servertype of the source and target
        # hypervisor for now, because of the old hypervisors with servertype
        # "db_server" and "frontend_server".  Fix this after the migration is
        # complete.
        if not isinstance(hv_admintool, ServerObject):
            hv_admintool = get_server(hv_admintool)

        if hv_admintool['hypervisor'] == 'kvm':
            cls = KVMHypervisor
        elif hv_admintool['hypervisor'] == 'xen':
            cls = XenHypervisor
        else:
            raise NotImplementedError(
                'Not a valid hypervisor type: {}'
                .format(hv_admintool['hypervisor'])
            )
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

    def vm_block_device_name(self):
        """Returns the name of the rootfs block device, as seen by the guest
        OS."""
        raise NotImplementedError(type(self).__name__)

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

    def vm_max_memory(self, vm):
        """Calculates the max amount of memory in MiB the VM may receive."""
        mem = vm.admintool['memory']
        if mem > 12*1024:
            max_mem = mem + 10*1024
        else:
            max_mem = 16*1024

        # Never go higher than HV
        max_mem = min(self.total_vm_memory(), max_mem)

        return max_mem

    def check_vm(self, vm):
        """Checks whether a VM can run on this hypervisor."""
        if self.admintool['state'] != 'online':
            raise ConfigError(
                'Hypervisor {0} is not in online state ({1}).'
                .format(self.hostname, self.admintool['state'])
            )

        if self.vm_defined(vm):
            raise HypervisorError(
                'VM {0} is already defined on {1}'
                .format(vm.hostname, self.hostname)
            )

        # Enough CPUs?
        if vm.admintool['num_cpu'] > self.num_cpus:
            raise HypervisorError(
                'Not enough CPUs. Destination Hypervisor has {0}, '
                'but VM requires {1}.'
                .format(self.num_cpus, vm.admintool['num_cpu'])
            )

        # Enough memory?
        free_mib = self.free_vm_memory()
        if vm.admintool['memory'] > free_mib:
            raise HypervisorError(
                'Not enough memory. Destination Hypervisor has {0}MiB but VM '
                'requires {1}MiB'
                .format(free_mib, vm.admintool['memory'])
            )

        # TODO: CPU model

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

    def total_vm_memory(self):
        """Returns amount of memory in MiB available to Hypervisor."""
        raise NotImplementedError(type(self).__name__)

    def free_vm_memory(self):
        """Returns MiB memory available (=unallocated) for VMs on the HV."""
        raise NotImplementedError(type(self).__name__)

    def define_vm(self, vm):
        """Creates a VM on the hypervisor."""
        log.info('Defining {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

    def start_vm(self, vm):
        log.info('Starting {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

    def vm_running(self, vm):
        raise NotImplementedError(type(self).__name__)

    def vm_defined(self, vm):
        raise NotImplementedError(type(self).__name__)

    def stop_vm(self, vm):
        log.info('Shutting down {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

    def stop_vm_force(self, vm):
        log.info('Force-stopping {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

    def undefine_vm(self, vm):
        if self.vm_running(vm):
            raise HypervisorError(
                'Refusing to undefine running VM {}'
                .format(vm.hostname)
            )
        log.info('Undefining {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

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
            raise HypervisorError(
                'Refusing to mount VM filesystem while VM is powered on'
            )

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

    def vm_sync_from_hypervisor(self, vm, result):
        """Synchronizes serveradmin information from the actual data on
        the hypervisor.
        :param result: A dictionary-like object that receives the updated
                       values."""
        # Update disk size
        lvs = get_logical_volumes(self)
        for lv in lvs:
            if lv['name'] == vm.hostname:
                assert self._disk_path.get(vm, lv['path']) == lv['path'], \
                    'Inconsistent LV path'
                self._disk_path[vm] = lv['path']
                result['disk_size_gib'] = int(math.ceil(lv['size_MiB'] / 1024))
                break
        else:
            raise HypervisorError(
                'Unable to find source LV and determine its size.'
            )


class KVMHypervisor(Hypervisor):
    @lazy_property
    def conn(self):
        conn = get_virtconn(self.hostname, 'kvm')
        if not conn:
            raise HypervisorError(
                'Unable to connect to Hypervisor {}!'
                .format(self.hostname)
            )
        return conn

    def _find_domain(self, vm):
        domain = self.conn.lookupByName(vm.hostname)
        return domain

    def _domain(self, vm):
        domain_obj = self._find_domain(vm)
        if not domain_obj:
            raise HypervisorError(
                'Unable to find domain {}.'
                .format(vm.hostname)
            )
        return domain_obj

    def vm_block_device_name(self):
        return 'vda1'

    def check_migration(self, vm, dst_hv, offline):
        super(KVMHypervisor, self).check_migration(vm, dst_hv, offline)

        # Online migration only works with the same VLAN
        if not offline and self.vlan_for_vm(vm) != dst_hv.vlan_for_vm(vm):
            raise HypervisorError(
                'Online migration is not possible with the current network '
                'configuration (different VLAN).'
            )

    def total_vm_memory(self):
        # Start with what OS sees as total memory (not installed memory)
        total_mib = self.conn.getMemoryStats(-1)['total'] / 1024
        # Always keep some extra memory free for Hypervisor
        total_mib -= HOST_RESERVED_MEMORY
        return total_mib

    def free_vm_memory(self):
        total_mib = self.total_vm_memory()

        # Calculate memory used by other VMs.
        # We can not trust hv_conn.getFreeMemory(), sum up memory used by
        # each VM instead
        used_kib = 0
        for dom_id in self.conn.listDomainsID():
            dom = self.conn.lookupByID(dom_id)
            used_kib += dom.info()[2]
        free_mib = total_mib - used_kib/1024
        return free_mib

    def define_vm(self, vm):
        super(KVMHypervisor, self).define_vm(vm)
        domain_xml = generate_domain_xml(self, vm)
        self.conn.defineXML(domain_xml)

        # Refresh storage pools to register the vm image
        for pool_name in self.conn.listStoragePools():
            pool = self.conn.storagePoolLookupByName(pool_name)
            pool.refresh(0)

    def start_vm(self, vm):
        super(KVMHypervisor, self).start_vm(vm)
        if self._domain(vm).create() != 0:
            raise HypervisorError('{0} failed to start'.format(vm.hostname))

    def vm_defined(self, vm):
        # Don't use lookupByName, it prints ugly messages to the console
        domains = self.conn.listAllDomains()
        return vm.hostname in [dom.name() for dom in domains]

    def vm_running(self, vm):
        if self._domain(vm).info()[0] == libvirt.VIR_DOMAIN_SHUTOFF:
            return False
        return True

    def stop_vm(self, vm):
        super(KVMHypervisor, self).stop_vm(vm)
        if self._domain(vm).shutdown() != 0:
            raise HypervisorError('Unable to stop {}'.format(vm.hostname))

    def stop_vm_force(self, vm):
        super(KVMHypervisor, self).stop_vm_force(vm)
        if self._domain(vm).destroy() != 0:
            raise HypervisorError(
                'Unable to force-stop {}'.format(vm.hostname)
            )

    def undefine_vm(self, vm):
        super(KVMHypervisor, self).undefine_vm(vm)
        if self._domain(vm).undefine() != 0:
            raise HypervisorError('Unable to undefine {}'.format(vm.hostname))

    def vm_sync_from_hypervisor(self, vm, result):
        super(KVMHypervisor, self).vm_sync_from_hypervisor(vm, result)
        vm_info = self._domain(vm).info()

        mem = int(vm_info[2] / 1024)
        if mem > 0:
            result['memory'] = mem

        num_cpu = vm_info[3]
        if num_cpu > 0:
            result['num_cpu'] = num_cpu


class XenHypervisor(Hypervisor):
    def vm_block_device_name(self):
        return 'xvda1'

    def check_migration(self, vm, dst_hv, offline):
        super(XenHypervisor, self).check_migration(vm, dst_hv, offline)
        if not offline:
            raise HypervisorError(
                '{} does not support online migration.'
                .format(self.hostname)
            )

    def total_vm_memory(self):
        mem = int(self.run(
            "cat /proc/meminfo | grep MemTotal | awk '{ print $2 }'",
            silent=True,
        )) / 1024
        return mem - HOST_RESERVED_MEMORY

    def free_vm_memory(self):
        # FIXME: We don't seem to know, so let's assume it's fine.
        return 99999

    def _sxp_path(self, vm):
        return os.path.join('/etc/xen/domains', vm.hostname + '.sxp')

    def define_vm(self, vm):
        sxp_file = 'etc/xen/domains/hostname.sxp'
        upload_template(sxp_file, self._sxp_path(vm), {
            'disk_device': self.vm_disk_path(vm),
            'serveradmin': vm.admintool,
            'max_mem': self.vm_max_memory(vm),
        })

    def start_vm(self, vm):
        super(XenHypervisor, self).start_vm(vm)
        self.run(cmd('xm create {0}', self._sxp_path(vm)))

    def vm_defined(self, vm):
        path = self._sxp_path(vm)
        with self.fabric_settings():
            return exists(path, use_sudo=False, verbose=False)

    def vm_running(self, vm):
        xm_list = self.run('xm list', silent=True)
        for line in xm_list.split('\n'):
            pieces = line.split()
            if len(pieces) >= 3 and pieces[2] == vm.hostname:
                return True
        return False

    def stop_vm(self, vm):
        super(XenHypervisor, self).stop_vm(vm)
        self.run(cmd('xm shutdown {0}', vm.hostname))

    def stop_vm_force(self, vm):
        super(XenHypervisor, self).stop_vm_force(vm)
        self.run(cmd('xm destroy {0}', vm.hostname))

    def undefine_vm(self, vm):
        super(XenHypervisor, self).undefine_vm(vm)
        self.run(cmd('rm {0}', self._sxp_path(vm)))

    def vm_sync_from_hypervisor(self, vm, result):
        super(XenHypervisor, self).vm_sync_from_hypervisor(vm, result)

        result['num_cpu'] = int(run(
            'xm list --long {0} '
            '| grep \'(online_vcpus \' '
            '| sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''
            .format(vm.hostname)
        ))
        result['memory'] = int(run(
            'xm list --long {0} '
            '| grep \'(memory \' '
            '| sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''
            .format(vm.hostname)
        ))
