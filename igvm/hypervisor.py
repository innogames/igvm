import logging
import math
import os
import time

import libvirt

from adminapi.dataset import query, filters, ServerObject

from fabric.contrib.files import exists

from igvm.exceptions import (
    ConfigError,
    HypervisorError,
    InconsistentAttributeError,
    InvalidStateError,
)
from igvm.host import Host, get_server
from igvm.settings import HOST_RESERVED_MEMORY
from igvm.utils import cmd
from igvm.utils.backoff import retry_wait_backoff
from igvm.utils.kvm import (
    DomainProperties,
    generate_domain_xml,
    migrate_live,
    set_memory,
    set_vcpus,
)
from igvm.utils.lazy_property import lazy_property
from igvm.utils.storage import (
    VG_NAME,
    RESERVED_DISK,
    get_free_disk_size_gib,
    create_storage,
    format_storage,
    get_logical_volumes,
    get_vm_volume,
    lvresize,
    lvrename,
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

        if hv_admintool.get('state') == 'retired':
            raise InvalidStateError(
                'Hypervisor "{0}" is retired.'
                .format(hv_admintool['hostname'])
            )

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
        hv_vlans = []
        if self.admintool.get('vlan_networks'):
            for vlan_network in query(
                hostname=filters.Any(*self.admintool['vlan_networks']),
                vlan_tag=filters.Not(filters.Empty()),
            ).restrict('vlan_tag'):
                hv_vlans.append(vlan_network['vlan_tag'])
        vm_vlan = vm.network_config['vlan_tag']
        if not hv_vlans:
            if self.network_config['vlan_tag'] != vm_vlan:
                raise HypervisorError(
                    'Hypervisor {} is not on same VLAN {} as VM {}.'
                    .format(
                        self.hostname,
                        self.network_config['vlan_tag'],
                        vm_vlan,
                    )
                )
            # For untagged Hypervisors VM must be untagged, too.
            return None

        # On source hypervisor it is unncessary to perform this check.
        # The VLAN is obviously there, even if not in Admintool.
        # This can happen if vlan is remved in Admintool so that nobody creates
        # new VMs on given HV, but the existing ones must be moved out.
        if (
            vm.admintool['xen_host'] != self.hostname and
            vm_vlan not in hv_vlans
        ):
            raise HypervisorError(
                'Hypervisor {} does not support VLAN {}.'
                .format(self.hostname, vm_vlan)
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
            raise InvalidStateError(
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

        # Enough disk?
        free_disk_space = get_free_disk_size_gib(self)
        vm_disk_size = float(vm.admintool['disk_size_gib'])
        if vm_disk_size > free_disk_space:
            raise HypervisorError(
                'Not enough free space in VG {} to build VM while keeping'
                ' {} GiB reserved'
                .format(VG_NAME, RESERVED_DISK)
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

    def vm_migrate_online(self, vm, dst_hv):
        """Online-migrates a VM to the given destination HV."""
        self.check_migration(vm, dst_hv, offline=False)

    def total_vm_memory(self):
        """Returns amount of memory in MiB available to Hypervisor."""
        raise NotImplementedError(type(self).__name__)

    def free_vm_memory(self):
        """Returns MiB memory available (=unallocated) for VMs on the HV."""
        raise NotImplementedError(type(self).__name__)

    def define_vm(self, vm, tx=None):
        """Creates a VM on the hypervisor."""
        log.info('Defining {} on {}'.format(vm.hostname, self.hostname))

        self._define_vm(vm, tx)
        if tx:
            tx.on_rollback('undefine VM', self.undefine_vm, vm)

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
        vm.disconnect()
        # Implementation must be subclassed

    def undefine_vm(self, vm):
        if self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to undefine running VM {}'
                .format(vm.hostname)
            )
        log.info('Undefining {} on {}'.format(vm.hostname, self.hostname))
        # Implementation must be subclassed

    def _check_committed(self, vm):
        """Checks that the given VM has no uncommitted changes."""
        if vm.admintool.is_dirty():
            raise ConfigError(
                'VM object has uncommitted changes, commit them first!'
            )

    def _check_attribute_synced(self, vm, attrib):
        """Compares an attribute value in Serveradmin with the actual value on
        the HV."""
        synced_values = self.vm_sync_from_hypervisor(vm)
        if attrib not in synced_values:
            log.warning('Cannot validate attribute "{}"!'.format(attrib))
            return
        current_value = synced_values[attrib]
        if current_value != vm.admintool[attrib]:
            raise InconsistentAttributeError(vm, attrib, current_value)

    def vm_set_num_cpu(self, vm, num_cpu):
        """Changes the number of CPUs of a VM."""
        self._check_committed(vm)
        self._check_attribute_synced(vm, 'num_cpu')

        if num_cpu < 1:
            raise ConfigError('Invalid num_cpu value: {}'.format(num_cpu))

        log.info('Changing #CPUs of {} on {}: {} -> {}'.format(
            vm.hostname, self.hostname, vm.admintool['num_cpu'], num_cpu))

        # If VM is offline, we can just rebuild the domain
        if not self.vm_running(vm):
            log.info('VM is offline, rebuilding domain with new settings')
            vm.admintool['num_cpu'] = num_cpu
            self.undefine_vm(vm)
            self.define_vm(vm)
        else:
            self._vm_set_num_cpu(vm, num_cpu)

        # Validate changes
        # We can't rely on the HV to provide data on VMs all the time.
        updated_admintool = self.vm_sync_from_hypervisor(vm)
        current_num_cpu = updated_admintool.get('num_cpu', num_cpu)
        if current_num_cpu != num_cpu:
            raise HypervisorError(
                'New CPUs are not visible to hypervisor, '
                'changes will not be committed.'
            )

        vm.admintool['num_cpu'] = num_cpu
        vm.admintool.commit()

    def vm_set_memory(self, vm, memory):
        self._check_committed(vm)
        self._check_attribute_synced(vm, 'memory')

        running = self.vm_running(vm)

        if self.free_vm_memory() < memory - vm.admintool['memory']:
            raise HypervisorError('Not enough free memory on hypervisor.')

        log.info('Changing memory of {} on {}: {} MiB -> {} MiB'.format(
            vm.hostname, self.hostname, vm.admintool['memory'], memory))

        # If VM is offline, we can just rebuild the domain
        if not running:
            log.info('VM is offline, rebuilding domain with new settings')
            vm.admintool['memory'] = memory
            self.undefine_vm(vm)
            self.define_vm(vm)
        else:
            old_total = vm.meminfo()['MemTotal']
            self._vm_set_memory(vm, memory)
            # HV might take some time to propagate memory changes,
            # wait until MemTotal changes.
            retry_wait_backoff(
                lambda: vm.meminfo()['MemTotal'] != old_total,
                'New memory is not yet visible',
            )

        # Validate changes, if possible.
        current_memory = self.vm_sync_from_hypervisor(vm).get('memory', memory)
        if current_memory != memory:
            raise HypervisorError(
                'New memory is not visible to hypervisor, '
                'changes will not be committed.'
            )

        vm.admintool['memory'] = memory
        vm.admintool.commit()

    def vm_set_disk_size_gib(self, vm, new_size_gib):
        """Changes disk size of a VM."""
        if new_size_gib < vm.admintool['disk_size_gib']:
            raise NotImplementedError('Cannot shrink the disk.')
        with self.fabric_settings():
            lvresize(self.vm_disk_path(vm), new_size_gib)

        self._vm_set_disk_size_gib(vm, new_size_gib)

    def create_vm_storage(self, vm, tx=None):
        """Allocate storage for a VM. Returns the disk path."""
        assert vm not in self._disk_path, 'Disk already created?'

        self._disk_path[vm] = create_storage(self, vm)
        if tx:
            tx.on_rollback('destroy storage', self.destroy_vm_storage, vm)
        return self._disk_path[vm]

    def rename_vm_storage(self, vm, new_name):
        with self.fabric_settings():
            lvrename(self.vm_disk_path(vm), new_name)

    def format_vm_storage(self, vm, tx=None):
        """Create new filesystem for VM and mount it. Returns mount path."""
        assert vm not in self._mount_path, 'Filesystem is already mounted'

        if self.vm_defined(vm):
            raise InvalidStateError(
                'Refusing to format storage of defined VM {}'
                .format(vm.hostname)
            )

        format_storage(self, self.vm_disk_path(vm))
        return self.mount_vm_storage(vm, tx)

    def mount_vm_storage(self, vm, tx=None):
        """Mount VM filesystem on host and return mount point."""
        if vm in self._mount_path:
            return self._mount_path[vm]

        if self.vm_defined(vm) and self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to mount VM filesystem while VM is powered on'
            )

        self._mount_path[vm] = mount_temp(
            self,
            self.vm_disk_path(vm),
            suffix='-'+vm.hostname,
        )
        if tx:
            tx.on_rollback('unmount storage', self.umount_vm_storage, vm)
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
            raise InvalidStateError(
                'Refusing to delete storage of defined VM {}'
                .format(vm.hostname)
            )
        remove_logical_volume(self, self.vm_disk_path(vm))
        del self._disk_path[vm]

    def vm_sync_from_hypervisor(self, vm):
        """Synchronizes serveradmin information from the actual data on
        the hypervisor. Returns a dict with all collected values."""
        # Update disk size
        result = {}
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

        self._vm_sync_from_hypervisor(vm, result)
        return result

    def vm_info(self, vm):
        """Return runtime information about a VM."""
        raise NotImplementedError(type(self).__name__)

    def get_free_disk_size_gib(self, safe=True):
        return get_free_disk_size_gib(self, safe)


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

    def vm_migrate_online(self, vm, dst_hv):
        super(KVMHypervisor, self).vm_migrate_online(vm, dst_hv)
        migrate_live(self, dst_hv, vm, self._domain(vm))

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

    def _vm_set_num_cpu(self, vm, num_cpu):
        set_vcpus(self, vm, self._domain(vm), num_cpu)

    def _vm_set_memory(self, vm, memory_mib):
        if self.vm_running(vm) and memory_mib < vm.admintool['memory']:
            raise InvalidStateError(
                'Cannot shrink memory while VM is running'
            )
        set_memory(self, vm, self._domain(vm), memory_mib)

    def _vm_set_disk_size_gib(self, vm, disk_size_gib):
        self.run(
            'virsh blockresize --path {0} --size {1}GiB {2}'
            .format(self.vm_disk_path(vm), disk_size_gib, vm.hostname)
        )
        vm.run('xfs_growfs /')

    def _define_vm(self, vm, tx):
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
        # _domain seems to fail on non-running VMs
        domains = self.conn.listAllDomains()
        for domain in domains:
            if domain.name() != vm.hostname:
                continue

            return domain.info()[0] in (
                libvirt.VIR_DOMAIN_RUNNING,
                libvirt.VIR_DOMAIN_SHUTDOWN,
            )
        raise HypervisorError(
            '{} is not defined on {}'
            .format(vm.hostname, self.hostname)
        )

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

    def _vm_sync_from_hypervisor(self, vm, result):
        vm_info = self._domain(vm).info()

        mem = int(vm_info[2] / 1024)
        if mem > 0:
            result['memory'] = mem

        num_cpu = vm_info[3]
        if num_cpu > 0:
            result['num_cpu'] = num_cpu

    def vm_info(self, vm):
        props = DomainProperties.from_running(self, vm, self._domain(vm))
        return props.info()


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
        # We can't trust the dom0, so let's assume Serveradmin is right.
        return self.admintool['memory'] - HOST_RESERVED_MEMORY

    def free_vm_memory(self):
        # FIXME: We don't seem to know, so let's assume it's fine.
        return 99999

    def _sxp_path(self, vm):
        return os.path.join('/etc/xen/domains', vm.hostname + '.sxp')

    def _define_vm(self, vm, tx):
        sxp_file = 'hv/domain.sxp'
        with self.fabric_settings():
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
            # Newer xm version?
            if pieces[0] == vm.hostname:
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

    def _vm_set_num_cpu(self, vm, num_cpu):
        self.run(cmd('xm vcpu-set {} {}', vm.hostname, num_cpu))

        # Activate all CPUs in the guest
        vm.run(
            'echo 1 | tee /sys/devices/system/cpu/cpu*/online',
            # Xen often throws "invalid argument", but it works anyway
            warn_only=True,
        )

    def _vm_set_memory(self, vm, memory_mib):
        max_mem = self.vm_max_memory(vm)
        if max_mem < memory_mib:
            raise HypervisorError(
                '{} can only receive up to {} MiB'.format(max_mem)
                .format(vm.hostname)
            )
        self.run(cmd('xm mem-max {} {}', vm.hostname, max_mem))
        self.run(cmd('xm mem-set {} {}', vm.hostname, memory_mib))
        # Xen takes time...
        log.info('Waiting 5 seconds for Xen to tell the VM about new memory')
        time.sleep(5)
        # Activate DIMMs
        vm.run(
            'echo online | tee /sys/devices/system/memory/memory*/state',
            silent=True,
            warn_only=True,
        )

    def _vm_set_disk_size_gib(self, vm, disk_size_gib):
        # Xen seems to take a while before disk changes propagate...
        def _try_grow():
            vm.run('mknod /dev/root b 202 1', warn_only=True, silent=True)
            output = vm.run('xfs_growfs /')
            if 'changed' in output:
                return True
            # Otherwise assume the last line is something like:
            # "realtime =none              extsz=4096   blocks=0, rtextents=0"
            if output.strip().splitlines()[-1].count('=') < 3:
                raise HypervisorError(
                    'xfs_growfs yielded unexpected output:\n{}'
                    .format(output)
                )
            return False
        retry_wait_backoff(
            _try_grow,
            'New disk size is not yet visible in VM',
        )

    def _vm_sync_from_hypervisor(self, vm, result):
        # xm only works if the VM is running.
        if not self.vm_running(vm):
            return

        result['num_cpu'] = int(self.run(
            'xm list --long {0} '
            '| grep \'(online_vcpus \' '
            '| sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''
            .format(vm.hostname),
            silent=True,
        ))
        result['memory'] = int(self.run(
            'xm list --long {0} '
            '| grep \'(memory \' '
            '| sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''
            .format(vm.hostname),
            silent=True,
        ))

    def vm_info(self, vm):
        # Any volunteers to still put effort into Xen? :-)
        return {}
