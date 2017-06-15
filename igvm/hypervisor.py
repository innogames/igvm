import logging
import math

from libvirt import VIR_DOMAIN_SHUTOFF, libvirtError

from adminapi.dataset import query, filters

from igvm.exceptions import (
    ConfigError,
    HypervisorError,
    InconsistentAttributeError,
    InvalidStateError,
)
from igvm.host import Host
from igvm.settings import HOST_RESERVED_MEMORY
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
from igvm.utils.virtutils import get_virtconn

log = logging.getLogger(__name__)


class Hypervisor(Host):
    """Hypervisor interface."""
    servertype = 'hypervisor'

    def __init__(self, *args, **kwargs):
        super(Hypervisor, self).__init__(*args, **kwargs)

        if self.server_obj['state'] == 'retired':
            raise InvalidStateError(
                'Hypervisor "{0}" is retired.'.format(self.fqdn)
            )

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
                '"{}" is not mounted on "{}".'
                .format(vm.fqdn, self.fqdn)
            )
        return self._mount_path[vm]

    def vlan_for_vm(self, vm):
        """Returns the VLAN number a VM should use on this hypervisor.
        None for untagged."""
        vlans = []
        if self.server_obj.get('vlan_networks'):
            for vlan_network in query(
                hostname=filters.Any(*self.server_obj['vlan_networks']),
                vlan_tag=filters.Not(filters.Empty()),
            ).restrict('vlan_tag'):
                vlans.append(vlan_network['vlan_tag'])
        vm_vlan = vm.network_config['vlan_tag']
        if not vlans:
            if self.network_config['vlan_tag'] != vm_vlan:
                raise HypervisorError(
                    'Hypervisor "{}" is not on same VLAN {} as VM {}.'
                    .format(
                        self.fqdn,
                        self.network_config['vlan_tag'],
                        vm_vlan,
                    )
                )
            # For untagged Hypervisors VM must be untagged, too.
            return None

        # On source hypervisor, it is unnecessary to perform this check.
        # The VLAN is obviously there, even if not on Serveradmin.  This can
        # happen if VLAN is removed on Serveradmin so that nobody creates
        # new VMs on given hypervisor, but the existing ones must be moved out.
        if (
            vm.server_obj['xen_host'] != self.hostname and
            vm_vlan not in vlans
        ):
            raise HypervisorError(
                'Hypervisor "{}" does not support VLAN {}.'
                .format(self.fqdn, vm_vlan)
            )
        return vm_vlan

    def vm_max_memory(self, vm):
        """Calculates the max amount of memory in MiB the VM may receive."""
        mem = vm.server_obj['memory']
        if mem > 12 * 1024:
            max_mem = mem + 10 * 1024
        else:
            max_mem = 16 * 1024

        # Never go higher than the hypervisor
        max_mem = min(self.total_vm_memory(), max_mem)

        return max_mem

    def check_vm(self, vm):
        """Check whether a VM can run on this hypervisor"""
        if self.server_obj['state'] not in ['online', 'online_reserved']:
            raise InvalidStateError(
                'Hypervisor "{}" is not in online state ({}).'
                .format(self.fqdn, self.server_obj['state'])
            )

        if self.vm_defined(vm):
            raise HypervisorError(
                'VM "{}" is already defined on "{}".'
                .format(vm.fqdn, self.fqdn)
            )

        # Enough CPUs?
        if vm.server_obj['num_cpu'] > self.num_cpus:
            raise HypervisorError(
                'Not enough CPUs. Destination Hypervisor has {0}, '
                'but VM requires {1}.'
                .format(self.num_cpus, vm.server_obj['num_cpu'])
            )

        # Enough memory?
        free_mib = self.free_vm_memory()
        if vm.server_obj['memory'] > free_mib:
            raise HypervisorError(
                'Not enough memory.  Destination Hypervisor has {} MiB but VM '
                'requires {} MiB '
                .format(free_mib, vm.server_obj['memory'])
            )

        # Enough disk?
        free_disk_space = get_free_disk_size_gib(self)
        vm_disk_size = float(vm.server_obj['disk_size_gib'])
        if vm_disk_size > free_disk_space:
            raise HypervisorError(
                'Not enough free space in VG {} to build VM while keeping'
                ' {} GiB reserved'
                .format(VG_NAME, RESERVED_DISK)
            )

        # TODO: CPU model

        # Proper VLAN?
        self.vlan_for_vm(vm)

    def define_vm(self, vm, tx=None):
        """Creates a VM on the hypervisor."""
        log.info('Defining "{}" on "{}"...'.format(vm.fqdn, self.fqdn))

        self._define_vm(vm, tx)
        if tx:
            tx.on_rollback('undefine VM', self.undefine_vm, vm)

    def _check_committed(self, vm):
        """Check that the given VM has no uncommitted changes"""
        if vm.server_obj.is_dirty():
            raise ConfigError(
                'VM object has uncommitted changes, commit them first!'
            )

    def _check_attribute_synced(self, vm, attrib):
        """Compare an attribute value in Serveradmin with the actual value on
        the hypervisor
        """
        synced_values = self.vm_sync_from_hypervisor(vm)
        if attrib not in synced_values:
            log.warning('Cannot validate attribute "{}"!'.format(attrib))
            return
        current_value = synced_values[attrib]
        if current_value != vm.server_obj[attrib]:
            raise InconsistentAttributeError(vm, attrib, current_value)

    def vm_set_num_cpu(self, vm, num_cpu):
        """Change the number of CPUs of a VM"""
        self._check_committed(vm)
        self._check_attribute_synced(vm, 'num_cpu')

        if num_cpu < 1:
            raise ConfigError('Invalid num_cpu value: {}'.format(num_cpu))

        log.info(
            'Changing #CPUs of "{}" on "{}" from {} to {}...'
            .format(vm.fqdn, self.fqdn, vm.server_obj['num_cpu'], num_cpu)
        )

        # If VM is offline, we can just rebuild the domain
        if not self.vm_running(vm):
            log.info('VM is offline, rebuilding domain with new settings')
            vm.server_obj['num_cpu'] = num_cpu
            self.undefine_vm(vm)
            self.define_vm(vm)
        else:
            self._vm_set_num_cpu(vm, num_cpu)

        # Validate changes
        # We can't rely on the hypervisor to provide data on VMs all the time.
        updated_server_obj = self.vm_sync_from_hypervisor(vm)
        current_num_cpu = updated_server_obj.get('num_cpu', num_cpu)
        if current_num_cpu != num_cpu:
            raise HypervisorError(
                'New CPUs are not visible to hypervisor, changes will not be '
                'committed.'
            )

        vm.server_obj['num_cpu'] = num_cpu
        vm.server_obj.commit()

    def vm_set_memory(self, vm, memory):
        self._check_committed(vm)
        self._check_attribute_synced(vm, 'memory')

        running = self.vm_running(vm)

        if self.free_vm_memory() < memory - vm.server_obj['memory']:
            raise HypervisorError('Not enough free memory on hypervisor.')

        log.info(
            'Changing memory of "{}" on "{}" from {} MiB to {} MiB'
            .format(vm.fqdn, self.fqdn, vm.server_obj['memory'], memory)
        )

        # If VM is offline, we can just rebuild the domain
        if not running:
            log.info('VM is offline, rebuilding domain with new settings')
            vm.server_obj['memory'] = memory
            self.undefine_vm(vm)
            self.define_vm(vm)
        else:
            old_total = vm.meminfo()['MemTotal']
            self._vm_set_memory(vm, memory)
            # Hypervisor might take some time to propagate memory changes,
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

        vm.server_obj['memory'] = memory
        vm.server_obj.commit()

    def vm_set_disk_size_gib(self, vm, new_size_gib):
        """Changes disk size of a VM."""
        if new_size_gib < vm.server_obj['disk_size_gib']:
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
                'Refusing to format storage of defined VM "{}".'
                .format(vm.fqdn)
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
            self, self.vm_disk_path(vm), suffix=('-' + vm.fqdn)
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
                'Refusing to delete storage of defined VM "{}".'
                .format(vm.fqdn)
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

    def get_free_disk_size_gib(self, safe=True):
        return get_free_disk_size_gib(self, safe)

    @lazy_property
    def conn(self):
        conn = get_virtconn(self.fqdn)
        if not conn:
            raise HypervisorError(
                'Unable to connect to hypervisor "{}"!'
                .format(self.fqdn)
            )
        return conn

    def _find_domain(self, vm):
        domain = None
        try:
            domain = self.conn.lookupByName(vm.hostname)
        except libvirtError:
            pass
        return domain

    def _domain(self, vm):
        domain_obj = self._find_domain(vm)
        if not domain_obj:
            raise HypervisorError(
                'Unable to find domain "{}".'.format(vm.fqdn)
            )
        return domain_obj

    def vm_block_device_name(self):
        """Get the name of the root file system block device as seen by
        the guest OS"""
        return 'vda1'

    def check_migration(self, vm, hypervisor, offline):
        """Check whether a VM can be migrated to the given hypervisor"""

        if self.fqdn == hypervisor.fqdn:
            raise HypervisorError(
                'Source and destination Hypervisor is the same "{0}"!'
                .format(self.fqdn)
            )

        # Online migration only works with the same VLAN
        if not offline and self.vlan_for_vm(vm) != hypervisor.vlan_for_vm(vm):
            raise HypervisorError(
                'Online migration is not possible with the current network '
                'configuration (different VLAN).'
            )

    def vm_migrate_online(self, vm, hypervisor):
        """Online-migrate a VM to the given destination hypervisor"""
        self.check_migration(vm, hypervisor, offline=False)
        migrate_live(self, hypervisor, vm, self._domain(vm))

    def total_vm_memory(self):
        """Get amount of memory in MiB available to hypervisor"""
        # Start with what OS sees as total memory (not installed memory)
        total_mib = self.conn.getMemoryStats(-1)['total'] / 1024
        # Always keep some extra memory free for Hypervisor
        total_mib -= HOST_RESERVED_MEMORY
        return total_mib

    def free_vm_memory(self):
        """Get memory in MiB available (unallocated) on the hypervisor"""
        total_mib = self.total_vm_memory()

        # Calculate memory used by other VMs.
        # We can not trust conn.getFreeMemory(), sum up memory used by
        # each VM instead
        used_kib = 0
        for dom_id in self.conn.listDomainsID():
            dom = self.conn.lookupByID(dom_id)
            used_kib += dom.info()[2]
        free_mib = total_mib - used_kib / 1024
        return free_mib

    def _vm_set_num_cpu(self, vm, num_cpu):
        set_vcpus(self, vm, self._domain(vm), num_cpu)

    def _vm_set_memory(self, vm, memory_mib):
        if self.vm_running(vm) and memory_mib < vm.server_obj['memory']:
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
        log.info('Starting "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._domain(vm).create() != 0:
            raise HypervisorError('"{0}" failed to start'.format(vm.fqdn))

    def vm_defined(self, vm):
        # Don't use lookupByName, it prints ugly messages to the console
        domains = self.conn.listAllDomains()
        return vm.hostname in [dom.name() for dom in domains]

    def vm_running(self, vm):
        """Check if the VM is kinda running using libvirt

        Libvirt has a state called "RUNNING", but it is not we want in here.
        The callers of this function expect us to cover all possible states
        the VM is somewhat alive.  So we return true for all states before
        "SHUTOFF" state including "SHUTDOWN" which actually only means
        "being shutdown".  If we would return false for this state
        then consecutive start() call would fail.
        """
        # _domain seems to fail on non-running VMs
        domains = self.conn.listAllDomains()
        for domain in domains:
            if domain.name() == vm.hostname:
                return domain.info()[0] < VIR_DOMAIN_SHUTOFF
        raise HypervisorError(
            '"{}" is not defined on "{}".'
            .format(vm.fqdn, self.fqdn)
        )

    def stop_vm(self, vm):
        log.info('Shutting down "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._domain(vm).shutdown() != 0:
            raise HypervisorError('Unable to stop "{}".'.format(vm.fqdn))

    def stop_vm_force(self, vm):
        log.info('Force-stopping "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._domain(vm).destroy() != 0:
            raise HypervisorError(
                'Unable to force-stop "{}".'.format(vm.fqdn)
            )

    def undefine_vm(self, vm):
        if self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to undefine running VM "{}"'.format(vm.fqdn)
            )
        log.info('Undefining "{}" on "{}"'.format(vm.fqdn, self.fqdn))
        if self._domain(vm).undefine() != 0:
            raise HypervisorError('Unable to undefine "{}".'.format(vm.fqdn))

    def _vm_sync_from_hypervisor(self, vm, result):
        vm_info = self._domain(vm).info()

        mem = int(vm_info[2] / 1024)
        if mem > 0:
            result['memory'] = mem

        num_cpu = vm_info[3]
        if num_cpu > 0:
            result['num_cpu'] = num_cpu

    def vm_info(self, vm):
        """Get runtime information about a VM"""
        props = DomainProperties.from_running(self, vm, self._domain(vm))
        return props.info()
