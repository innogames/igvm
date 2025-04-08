"""igvm - Hypervisor Model

Copyright (c) 2018 InnoGames GmbH
"""

import logging
import math
from contextlib import contextmanager
from time import sleep
from datetime import datetime
from xml.etree import ElementTree

from igvm.vm import VM
from libvirt import VIR_DOMAIN_SHUTOFF, virStorageVol, virStoragePool

from igvm.drbd import DRBD
from igvm.exceptions import (
    ConfigError,
    HypervisorError,
    InconsistentAttributeError,
    InvalidStateError,
    RemoteCommandError,
    StorageError,
    XfsMigrationError,
)
from igvm.host import Host
from igvm.kvm import (
    DomainProperties,
    generate_domain_xml,
    migrate_live,
    set_memory,
    set_vcpus,
)
from igvm.libvirt import get_virtconn
from igvm.settings import (
    HOST_RESERVED_MEMORY_MIB,
    IGVM_IMAGE_MD5_URL,
    IGVM_IMAGE_URL,
    IMAGE_PATH,
    KVM_HWMODEL_TO_CPUMODEL,
    MIGRATE_CONFIG,
    RESERVED_DISK,
    DEFAULT_VG_NAME,
    VM_OVERHEAD_MEMORY_MIB,
    XFS_CONFIG,
)
from igvm.transaction import Transaction
from igvm.utils import retry_wait_backoff
from typing import Iterator, Tuple

log = logging.getLogger(__name__)


class Hypervisor(Host):
    """Hypervisor interface."""
    servertype = 'hypervisor'

    def __init__(self, dataset_obj):
        super(Hypervisor, self).__init__(dataset_obj)

        if dataset_obj['state'] == 'retired':
            raise InvalidStateError(
                'Hypervisor "{0}" is retired.'.format(self.fqdn)
            )

        self._mount_path = {}
        self._storage_type = None

    def get_active_storage_pools(self):
        # The 2 used as argument is the value of the VIR_CONNECT_LIST_STORAGE_POOLS_ACTIVE flag.
        return self.conn().listAllStoragePools(2)

    def find_vg_of_vm(self, dataset_obj):
        # Find the storage pool of the VM
        # XXX: This is a slow and non-precise, returning only first match
        host = Host(dataset_obj)
        for pool in self.get_active_storage_pools():
            for vol_name in pool.listVolumes():
                if host.match_uid_name(vol_name):
                    return pool.name()
        return None

    def get_storage_pool(self, vg_name=DEFAULT_VG_NAME) -> virStoragePool:
        # Store per-VM path information
        # We cannot store these in the VM object due to migrations.
        return self.conn().storagePoolLookupByName(vg_name)

    def get_storage_type(self, vg_name=DEFAULT_VG_NAME):
        if self._storage_type:
            return self._storage_type

        self._storage_type = ElementTree.fromstring(
            self.get_storage_pool(vg_name=vg_name).XMLDesc()
        ).attrib['type']

        if (
            self._storage_type not in HOST_RESERVED_MEMORY_MIB
            or self._storage_type not in RESERVED_DISK
        ):
            raise HypervisorError(
                'Unsupported storage type {} on hypervisor {}'
                .format(self._storage_type, self.dataset_obj['hostname'])
            )
        return self._storage_type

    def get_volume_by_vm(self, vm) -> virStorageVol:
        """Get logical volume information of a VM"""
        for vol_name in self.get_storage_pool(vg_name=vm.vg_name).listVolumes():
            # Match the LV based on the object_id encoded within its name
            if vm.match_uid_name(vol_name):
                return self.get_storage_pool(
                    vg_name=vm.vg_name
                ).storageVolLookupByName(vol_name)

        raise StorageError(
            'No existing storage volume found for VM "{}" on "{}".'
            .format(vm.fqdn, self.fqdn)
        )

    def vm_lv_update_name(self, vm):
        """Update the VMs logical volumes name

        While the object_id part of the lv name will always be the same, the
        hostname can get out of date when it's updated on serveradmin. Calling
        this method during vm_restart updates it if required.

        Be aware: This can only be done when the VM is shut off and the
        libvirt domains needs to be redefined afterwards.
        """
        old_name = self.get_volume_by_vm(vm).name()
        new_name = vm.uid_name
        with self.fabric_settings():
            if old_name != new_name:
                self.run(
                    'lvrename {} {}'.format(
                        self.get_volume_by_vm(vm).path(),
                        vm.uid_name
                    )
                )
                self.get_storage_pool(vg_name=vm.vg_name).refresh()

    def vm_mount_path(self, vm):
        """Returns the mount path for a VM or raises HypervisorError if not
        mounted."""
        if vm not in self._mount_path:
            raise HypervisorError(
                '"{}" is not mounted on "{}".'
                .format(vm.fqdn, self.fqdn)
            )
        return self._mount_path[vm]

    def get_vlan_network(self, ip_addr):
        """Find the network for the VM

        We could not get the "route_network" of the VM because it might have
        its IP address changed.
        """
        for vlan_network in self.dataset_obj['vlan_networks']:
            if ip_addr in vlan_network['ipv4'] or ip_addr in vlan_network['ipv6']:
                return vlan_network
        return None

    def check_vm(self, vm, offline):
        """Check whether a VM can run on this hypervisor"""
        # Cheap checks should always be executed first to save time
        # and fail early. Same goes for checks that are more likely to fail.

        # Immediately check whether HV is even supported.
        if not offline:
            # Compatbile OS?
            os_pair = (vm.hypervisor.dataset_obj['os'], self.dataset_obj['os'])
            if os_pair not in MIGRATE_CONFIG:
                raise HypervisorError(
                    '{} to {} migration is not supported online.'
                    .format(*os_pair))

            # Compatible CPU model?
            hw_pair = (
                vm.hypervisor.dataset_obj['hardware_model'],
                self.dataset_obj['hardware_model'],
            )
            cpu_pair = [
                arch
                for arch, models in KVM_HWMODEL_TO_CPUMODEL.items()
                for model in hw_pair
                if model in models
            ]
            if cpu_pair[0] != cpu_pair[1]:
                raise HypervisorError(
                    '{} to {} migration is not supported online.'
                    .format(*hw_pair)
                )

        # HV in supported state?
        if self.dataset_obj['state'] not in ['online', 'online_reserved']:
            raise InvalidStateError(
                'Hypervisor "{}" is not in online state ({}).'
                .format(self.fqdn, self.dataset_obj['state'])
            )

        # Enough CPUs?
        if vm.dataset_obj['num_cpu'] > self.dataset_obj['num_cpu']:
            raise HypervisorError(
                'Not enough CPUs. Destination Hypervisor has {0}, '
                'but VM requires {1}.'
                .format(self.dataset_obj['num_cpu'], vm.dataset_obj['num_cpu'])
            )

        # Proper VLAN?
        if not self.get_vlan_network(vm.dataset_obj['intern_ip']):
            raise HypervisorError(
                'Hypervisor "{}" does not support route_network "{}".'
                .format(self.fqdn, vm.route_network)
            )

        # Those checks below all require libvirt connection,
        # so execute them last to avoid unnecessary overhead if possible.

        # Enough memory?
        free_mib = self.free_vm_memory()
        if vm.dataset_obj['memory'] > free_mib:
            raise HypervisorError(
                'Not enough memory. '
                'Destination Hypervisor has {:.2f} MiB but VM requires {} MiB '
                .format(free_mib, vm.dataset_obj['memory'])
            )

        # Enough disk?
        free_disk_space = self.get_free_disk_size_gib(vg_name=vm.vg_name)
        vm_disk_size = float(vm.dataset_obj['disk_size_gib'])
        if vm_disk_size > free_disk_space:
            raise HypervisorError(
                'Not enough free space in VG {} to build VM while keeping'
                ' {} GiB reserved'
                .format(vm.vg_name, RESERVED_DISK[self.get_storage_type()])
            )

        # VM already defined? Least likely, if at all.
        if self.vm_defined(vm):
            raise HypervisorError(
                'VM "{}" is already defined on "{}".'
                .format(vm.fqdn, self.fqdn)
            )

    def define_vm(self, vm, transaction=None):
        """Creates a VM on the hypervisor."""
        log.info('Defining "{}" on "{}"...'.format(vm.fqdn, self.fqdn))

        self.conn().defineXML(
            generate_domain_xml(hypervisor=self, vm=vm)
        )

        # Refresh storage pools to register the vm image
        for pool_name in self.conn().listStoragePools():
            pool = self.conn().storagePoolLookupByName(pool_name)
            pool.refresh(0)
        if transaction:
            transaction.on_rollback(
                'delete VM', self.undefine_vm, vm, keep_storage=True
            )

    def _check_committed(self, vm):
        """Check that the given VM has no uncommitted changes"""
        if vm.dataset_obj.is_dirty():
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
        if current_value != vm.dataset_obj[attrib]:
            raise InconsistentAttributeError(vm, attrib, current_value)

    def vm_set_num_cpu(self, vm, num_cpu):
        """Change the number of CPUs of a VM"""
        self._check_committed(vm)
        self._check_attribute_synced(vm, 'num_cpu')

        if num_cpu < 1:
            raise ConfigError('Invalid num_cpu value: {}'.format(num_cpu))

        log.info(
            'Changing #CPUs of "{}" on "{}" from {} to {}...'
            .format(vm.fqdn, self.fqdn, vm.dataset_obj['num_cpu'], num_cpu)
        )

        # If VM is offline, we can just rebuild the domain
        if not self.vm_running(vm):
            log.info('VM is offline, rebuilding domain with new settings')
            vm.dataset_obj['num_cpu'] = num_cpu
            self.redefine_vm(vm)
        else:
            set_vcpus(self, vm, self._get_domain(vm), num_cpu)

        # Validate changes
        # We can't rely on the hypervisor to provide data on VMs all the time.
        updated_dataset_obj = self.vm_sync_from_hypervisor(vm)
        current_num_cpu = updated_dataset_obj['num_cpu']
        if current_num_cpu != num_cpu:
            raise HypervisorError(
                'New CPUs are not visible to hypervisor, changes will not be '
                'committed.'
            )

        vm.dataset_obj['num_cpu'] = num_cpu
        vm.dataset_obj.commit()

    def vm_set_memory(self, vm, memory):
        self._check_committed(vm)
        vm.check_serveradmin_config()
        self._check_attribute_synced(vm, 'memory')

        running = self.vm_running(vm)

        if running and memory < vm.dataset_obj['memory']:
            raise InvalidStateError(
                'Cannot shrink memory while VM is running'
            )
        if self.free_vm_memory() < memory - vm.dataset_obj['memory']:
            raise HypervisorError('Not enough free memory on hypervisor.')

        log.info(
            'Changing memory of "{}" on "{}" from {} MiB to {} MiB'
            .format(vm.fqdn, self.fqdn, vm.dataset_obj['memory'], memory)
        )

        vm.dataset_obj['memory'] = memory
        vm.check_serveradmin_config()

        # If VM is offline, we can just rebuild the domain
        if not running:
            log.info('VM is offline, rebuilding domain with new settings')
            self.redefine_vm(vm)
            vm.dataset_obj.commit()
        else:
            old_total = vm.meminfo()['MemTotal']
            set_memory(self, vm, self._get_domain(vm))
            vm.dataset_obj.commit()

            # Hypervisor might take some time to propagate memory changes,
            # wait until MemTotal changes.
            retry_wait_backoff(
                lambda: vm.meminfo()['MemTotal'] != old_total,
                'New memory is not yet visible to virtual machine.',
                max_wait=40
            )

        # Validate changes, if possible.
        current_memory = self.vm_sync_from_hypervisor(vm).get('memory', memory)
        if current_memory != memory:
            raise HypervisorError(
                'Warning: The sanity check to see if libvirt reports the '
                'updated amount of memory for the domain we just changed has'
                'failed. Note that we can not online decrease the domains '
                'memory. The libvirt and serveradmin changes will therefore '
                'not be rolled back.'
            )

    def vm_set_disk_size_gib(self, vm, new_size_gib):
        """Changes disk size of a VM."""
        if new_size_gib < vm.dataset_obj['disk_size_gib']:
            raise NotImplementedError(
                'Cannot shrink the disk. '
                'Use `igvm migrate --offline --offline-transport xfs '
                '--disk-size {} {}`'.format(
                    new_size_gib, vm.fqdn,
                )
            )
        volume = self.get_volume_by_vm(vm)
        if self.get_storage_type() == 'logical':
            # There is no resize function in version of libvirt
            # available in Debian 9.
            self.run('lvresize {} -L {}g'.format(volume.path(), new_size_gib))
            self.get_storage_pool(vg_name=vm.vg_name).refresh()
        else:
            raise NotImplementedError(
                'Storage volume resizing is supported only on LVM storage!'
            )
        self._get_domain(vm).blockResize(
            'vda',
            new_size_gib * 1024 ** 2,  # Yes, it is in KiB
        )
        vm.run('xfs_growfs /')

    def create_vm_storage(
        self, vm, transaction=None, vol_name=None
    ):
        """Allocate storage for a VM. Returns the disk path."""
        vol_name = vm.uid_name if vol_name is None else vol_name
        volume_xml = """
            <volume>
                <name>{name}</name>
                <allocation unit="G">{size}</allocation>
                <capacity unit="G">{size}</capacity>
            </volume>
        """.format(
            name=vol_name,
            size=vm.dataset_obj['disk_size_gib'],
        )

        volume = self.get_storage_pool(vg_name=vm.vg_name).createXML(volume_xml, 0)
        if volume is None:
            raise StorageError(
                'Failed to create storage volume {}/{}'.format(
                    self.get_storage_pool(vg_name=vm.vg_name).name(),
                    vol_name,
                )
            )

        if transaction:
            def destroy_storage():
                vol = self.get_storage_pool(
                    vg_name=vm.vg_name
                ).storageVolLookupByName(vol_name)
                vol.delete()

            transaction.on_rollback('destroy storage', destroy_storage)

        # XXX: When building a VM we use the volumes path to format it right
        # after creation.  Unfortunately the kernel is slow to pick up on zfs
        # volume changes and creates the symlink in /dev/zvol/<pool>/ only
        # after a moment.
        self.run("while [ ! -L '{}' ]; do sleep 1; done".format(volume.path()))

    def format_vm_storage(self, vm, transaction=None):
        """Create new filesystem for VM and mount it. Returns mount path."""

        if self.vm_defined(vm):
            raise InvalidStateError(
                'Refusing to format storage of defined VM "{}".'
                .format(vm.fqdn)
            )

        mkfs_options = XFS_CONFIG.get(vm.dataset_obj['os'])

        if not mkfs_options:
            raise ConfigError(
                'No mkfs options defined for OS {}'.format(
                    vm.dataset_obj['os']
                )
            )

        self.format_storage(
            self.get_volume_by_vm(vm).path(), mkfs_options
        )
        return self.mount_vm_storage(
            vm=vm, transaction=transaction
        )

    def download_and_extract_image(self, image, target_dir):
        """Download image, verify its checsum and extract it

        All operations must be performed with locking, so that parallel
        running igvm won't touch eachothers' images.
        """

        self.run(
            '( '
            'set -e ; '
            'flock -w 120 9 ; '
            'curl -o {img_path}/{img_file}.md5 {md5_url} ; '
            'sed -Ei \'s_ (.*/)?([a-zA-Z0-9\.\-]+)$_ {img_path}/\\2_\' '
            '{img_path}/{img_file}.md5 ; '
            'md5sum -c {img_path}/{img_file}.md5 || '
            'curl -o {img_path}/{img_file} {img_url} ; '
            'tar --xattrs --xattrs-include=\'*\' -xzf {img_path}/{img_file} '
            '-C {dst_path} ;'
            ') 9>/tmp/igvm_image.lock'.format(
                img_path=IMAGE_PATH,
                img_file=image,
                img_url=IGVM_IMAGE_URL.format(image=image),
                md5_url=IGVM_IMAGE_MD5_URL.format(image=image),
                dst_path=target_dir,
            )
        )

    def mount_vm_storage(self, vm, transaction=None):
        """Mount VM filesystem on host and return mount point."""
        if vm in self._mount_path:
            return self._mount_path[vm]

        if self.vm_defined(vm) and self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to mount VM filesystem while VM is powered on'
            )

        self._mount_path[vm] = self.mount_temp(
            self.get_volume_by_vm(vm).path(),
            suffix=('-' + vm.fqdn),
        )
        if transaction:
            transaction.on_rollback(
                'unmount storage', self.umount_vm_storage, vm
            )

        vm.mounted = True
        return self._mount_path[vm]

    def umount_vm_storage(self, vm):
        """Unmount VM filesystem."""
        if vm not in self._mount_path:
            return
        self.umount_temp(self._mount_path[vm])
        self.remove_temp(self._mount_path[vm])
        del self._mount_path[vm]
        vm.mounted = False

    def vm_sync_from_hypervisor(self, vm):
        """Synchronizes serveradmin information from the actual data on
        the hypervisor. Returns a dict with all collected values."""
        # Update disk size
        result = {}
        try:
            vol_size = self.get_volume_by_vm(vm).info()[1]
            result['disk_size_gib'] = int(math.ceil(vol_size / 1024**3))
        except HypervisorError:
            raise HypervisorError(
                'Unable to find source LV and determine its size.'
            )

        self._vm_sync_from_hypervisor(vm, result)
        return result

    def conn(self):
        conn = get_virtconn(self.fqdn)
        if not conn:
            raise HypervisorError(
                'Unable to connect to hypervisor "{}"!'
                .format(self.fqdn)
            )
        return conn

    def num_numa_nodes(self):
        """Return the number of NUMA nodes"""
        return self.conn().getInfo()[4]

    def _find_domain(self, vm):
        """Search and return the domain on hypervisor

        It is erroring out when multiple domains found, and returning None,
        when none found.
        """
        found = None
        # We are not using lookupByName(), because it prints ugly messages to
        # the console.
        for domain in self.conn().listAllDomains():
            name = domain.name()
            # Match the domain based on the object_id encoded in its name
            if not vm.match_uid_name(name):
                continue

            if found is not None:
                raise HypervisorError(
                    'Same VM is defined multiple times as "{}" and "{}".'
                    .format(found.name(), name)
                )
            found = domain
        return found

    def _get_domain(self, vm):
        domain = self._find_domain(vm)
        if not domain:
            raise HypervisorError(
                'Unable to find domain "{}" on hypervisor "{}".'
                .format(vm.fqdn, self.fqdn)
            )
        return domain

    def vm_block_device_name(self):
        """Get the name of the root file system block device as seen by
        the guest OS"""
        return 'vda1'

    def check_migrate_parameters(
        self, vm: VM, offline: bool, offline_transport: str,
        disk_size: int = None,
    ):
        if offline_transport not in ['netcat', 'drbd', 'xfs']:
            raise StorageError(
                'Unknown offline transport method {}!'
                .format(offline_transport)
            )

        if disk_size is None:
            return

        if disk_size < 1:
            raise StorageError('disk_size must be at least 1GiB!')
        if not (offline and offline_transport == 'xfs'):
            raise StorageError(
                'disk_size can be applied only with offline transport xfs!'
            )
        allocated_space = vm.dataset_obj['disk_size_gib'] - vm.disk_free()
        if disk_size < allocated_space:
            raise StorageError(
                'disk_size is lower than allocated space: {} < {}!'
                .format(disk_size, allocated_space)
            )

    def vm_new_disk_size(
        self, vm: VM, offline: bool, offline_transport: str,
        disk_size: int = None,
    ) -> int:
        self.check_migrate_parameters(
            vm, offline, offline_transport, disk_size
        )
        if disk_size is None:
            return vm.dataset_obj['disk_size_gib']
        return disk_size or vm.dataset_obj['disk_size_gib']

    def _vm_apply_new_disk_size(
        self, vm: VM, offline: bool, offline_transport: str,
        transaction: Transaction, disk_size: int = 0,
    ):
        """
        If the new VM disk size is set, checks if it's correct and sufficient
        and commit the new size. Rolls it back on the interrupted migration

        :param VM vm: The migrating VM
        :param str offline_transport: offline migration transport
        :param Transaction transaction: The transaction to rollback
        :param int disk_size: the new disk_size_gib attribute
        """
        size = self.vm_new_disk_size(vm, offline,
                                     offline_transport, disk_size)
        if size == vm.dataset_obj['disk_size_gib']:
            return

        old_size = vm.dataset_obj['disk_size_gib']
        vm.dataset_obj['disk_size_gib'] = size
        vm.dataset_obj.commit()

        if transaction:
            def restore_size():
                vm.dataset_obj['disk_size_gib'] = old_size
                vm.dataset_obj.commit()

            transaction.on_rollback('reset_disk_size', restore_size)

    def _wait_for_shutdown(
        self, vm: VM, no_shutdown: bool, transaction: Transaction,
    ):
        """
        If no_shutdown=True, will wait for the manual VM shutdown. Otherwise
        shoutdown the VM.

        :param VM vm: The migrating VM
        :param bool no_shutdown: if the VM must be shut down manualy
        :param Transaction transaction: The transaction to rollback
        """
        vm.set_state('maintenance', transaction=transaction)
        if vm.is_running():
            if no_shutdown:
                log.info('Please shut down the VM manually now')
                vm.wait_for_running(running=False, timeout=86400)
            else:
                vm.shutdown(
                    check_vm_up_on_transaction=False,
                    transaction=transaction,
                )

    def migrate_vm(
        self, vm: VM, target_hypervisor: 'Hypervisor', offline: bool,
        offline_transport: str, transaction: Transaction, no_shutdown: bool,
        disk_size: int = 0,
    ):
        self._vm_apply_new_disk_size(
            vm, offline, offline_transport, transaction, disk_size
        )

        if offline:
            log.info(
                'Starting offline migration of vm {} from {} to {}'.format(
                    vm, vm.hypervisor, target_hypervisor)
            )
            target_hypervisor.create_vm_storage(vm, transaction)
            if offline_transport == 'drbd':
                is_lvm_storage = (
                    self.get_storage_type() == 'logical'
                    and target_hypervisor.get_storage_type() == 'logical'
                )

                if not is_lvm_storage:
                    raise NotImplementedError(
                        'DRBD migration is supported only between hypervisors '
                        'using LVM storage!'
                    )

                host_drbd = DRBD(self, vm, master_role=True)
                peer_drbd = DRBD(target_hypervisor, vm)
                if vm.hypervisor.vm_running(vm):
                    vm_block_size = vm.get_block_size('/dev/vda')
                    src_block_size = vm.hypervisor.get_block_size(
                        vm.hypervisor.get_volume_by_vm(vm).path()
                    )
                    dst_block_size = target_hypervisor.get_block_size(
                        target_hypervisor.get_volume_by_vm(vm).path()
                    )
                    log.debug(
                        'Block sizes: VM {}, Source HV {}, Destination HV {}'
                        .format(vm_block_size, src_block_size, dst_block_size)
                    )
                    vm.set_block_size('vda', min(
                        vm_block_size,
                        src_block_size,
                        dst_block_size,
                    ))
                with host_drbd.start(peer_drbd), peer_drbd.start(host_drbd):
                    # XXX: Do we really need to wait for the both?
                    host_drbd.wait_for_sync()
                    peer_drbd.wait_for_sync()
                    self._wait_for_shutdown(vm, no_shutdown, transaction)

            elif offline_transport == 'netcat':
                self._wait_for_shutdown(vm, no_shutdown, transaction)
                target_vol = target_hypervisor.get_volume_by_vm(vm)
                with target_hypervisor.netcat_to_device(target_vol) as args:
                    self.device_to_netcat(
                        self.get_volume_by_vm(vm),
                        vm.dataset_obj['disk_size_gib'] * 1024 ** 3,
                        args,
                    )
            elif offline_transport == 'xfs':
                self._wait_for_shutdown(vm, no_shutdown, transaction)
                with target_hypervisor.xfsrestore(
                    vm=vm, transaction=transaction
                ) as listener:
                    self.xfsdump(vm, listener, transaction)

                target_hypervisor.wait_for_xfsrestore(vm)
                target_hypervisor.check_xfsrestore_log(vm)
                target_hypervisor.umount_vm_storage(vm)

            target_hypervisor.define_vm(
                vm=vm, transaction=transaction
            )
        else:
            # For online migrations always use same volume name as VM
            # already has.
            target_hypervisor.create_vm_storage(
                vm,
                transaction,
                vm.hypervisor.get_volume_by_vm(vm).name(),
            )
            migrate_live(self, target_hypervisor, vm, self._get_domain(vm))

    def _get_reserved_hv_memory_mib(self):
        """Get the amount of memory reserved for the hypervisor

        This is determined by both KVM's FS and extra memory reserved for
        Ceph's OSDs.
        """
        total_reserved_mib = HOST_RESERVED_MEMORY_MIB[self.get_storage_type()]
        # ceph_disks is a multi-attribute holding one record for each OSD
        # and their size. For each, we'll reserve 4GiB of memory.
        total_reserved_mib += 4096 * len(self.dataset_obj['ceph_disks'])

        return total_reserved_mib

    def total_vm_memory(self):
        """Get amount of memory in MiB available to hypervisor"""
        # Start with what OS sees as total memory (not installed memory)
        total_mib = self.conn().getMemoryStats(-1)['total'] // 1024
        # Always keep some extra memory free for Hypervisor
        total_mib -= self._get_reserved_hv_memory_mib()
        return total_mib

    def free_vm_memory(self) -> int:
        """Get memory in MiB available (unallocated) on the hypervisor"""
        total_mib = self.total_vm_memory()

        # Calculate memory used by other VMs.
        # We can not trust conn().getFreeMemory(), sum up memory used by
        # each VM instead
        used_mib = 0
        for dom in self.conn().listAllDomains():
            # Since every VM has its own overhead in QEMU, we must account for
            # it accordingly, and not once overall for the whole HV.
            used_mib += dom.info()[2] // 1024 + VM_OVERHEAD_MEMORY_MIB

        return total_mib - used_mib

    def start_vm(self, vm):
        log.info('Starting "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._get_domain(vm).create() != 0:
            raise HypervisorError('"{0}" failed to start'.format(vm.fqdn))

    def vm_defined(self, vm):
        return self._find_domain(vm) is not None

    def vm_running(self, vm):
        """Check if the VM is kinda running using libvirt

        Libvirt has a state called "RUNNING", but it is not we want in here.
        The callers of this function expect us to cover all possible states
        the VM is somewhat alive.  So we return true for all states before
        "SHUTOFF" state including "SHUTDOWN" which actually only means
        "being shutdown".  If we would return false for this state
        then consecutive start() call would fail.
        """
        return self._get_domain(vm).info()[0] < VIR_DOMAIN_SHUTOFF

    def stop_vm(self, vm):
        log.info('Shutting down "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._get_domain(vm).shutdown() != 0:
            raise HypervisorError('Unable to stop "{}".'.format(vm.fqdn))

    def stop_vm_force(self, vm):
        log.info('Force-stopping "{}" on "{}"...'.format(vm.fqdn, self.fqdn))
        if self._get_domain(vm).destroy() != 0:
            raise HypervisorError(
                'Unable to force-stop "{}".'.format(vm.fqdn)
            )

    def undefine_vm(self, vm, keep_storage=False):
        if self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to undefine running VM "{}"'.format(vm.fqdn)
            )
        log.info('Undefining "{}" on "{}"'.format(vm.fqdn, self.fqdn))

        if not keep_storage:
            # XXX: get_volume_by_vm depends on domain names to find legacy
            # domains w/o an uid_name.  The order is therefore important.
            self.get_volume_by_vm(vm).delete()

        if self._get_domain(vm).undefine() != 0:
            raise HypervisorError('Unable to undefine "{}".'.format(vm.fqdn))

    def redefine_vm(self, vm, new_fqdn=None):
        # XXX: vm_lv_update_name depends on domain names to find legacy domains
        # w/o an uid_name.  The order is therefore important.
        self.vm_lv_update_name(vm)
        self.undefine_vm(vm, keep_storage=True)
        # XXX: undefine_vm depends on vm.fqdn beeing the old name for finding
        # legacy domains w/o an uid_name.  The order is therefore important.
        vm.fqdn = new_fqdn or vm.fqdn
        self.define_vm(vm=vm)

    def _vm_sync_from_hypervisor(self, vm, result):
        vm_info = self._get_domain(vm).info()

        mem = int(vm_info[2] / 1024)
        if mem > 0:
            result['memory'] = mem

        num_cpu = vm_info[3]
        if num_cpu > 0:
            result['num_cpu'] = num_cpu

    def vm_info(self, vm):
        """Get runtime information about a VM"""
        props = DomainProperties.from_running(self, vm, self._get_domain(vm))
        return props.info()

    def get_free_disk_size_gib(self, safe=True, vg_name=DEFAULT_VG_NAME):
        """Return free disk space as float in GiB"""
        pool_info = self.get_storage_pool(vg_name=vg_name).info()
        # Floor instead of ceil because we check free instead of used space
        vg_size_gib = math.floor(float(pool_info[3]) / 1024 ** 3)
        if safe is True:
            vg_size_gib -= RESERVED_DISK[self.get_storage_type()]
        return vg_size_gib

    def mount_temp(self, device, suffix=''):
        """Mounts given device into temporary path"""
        mount_dir = self.run('mktemp -d --suffix {}'.format(suffix))
        self.run('mount {0} {1}'.format(device, mount_dir))
        return mount_dir

    def umount_temp(self, device_or_path):
        """Unmounts a device or path

        Sometimes it is impossible to immediately umount a directory due to
        a process still holding it open. It happens often when igvm is stopped.
        Underlying process such puppetrun won't die immediately.
        """

        retry = 10
        for i in range(0, retry):
            if i > 0:
                log.warning(
                    'Umounting {} failed, attempting again in a moment. '
                    '{} attempts left.'
                    .format(device_or_path, retry - i)
                )
                sleep(1)
            res = self.run(
                'umount {0}'.format(device_or_path),
                warn_only=(i < retry - 1),
            )
            if res.succeeded:
                return

    def remove_temp(self, mount_path):
        self.run('rmdir {0}'.format(mount_path))

    def format_storage(self, device, options):
        self.run('mkfs.xfs -f {} {}'.format(' '.join(options), device))

    def check_netcat(self, port):
        pid = self.run(
            'pgrep -f "^/bin/nc.openbsd -l -p {}"'
            .format(port),
            warn_only=True,
            silent=True
        )

        if pid:
            raise StorageError(
                'Listening netcat already found on destination hypervisor.'
            )

    def kill_netcat(self, port):
        self.run(
            'pkill -f "^/bin/nc.openbsd -l -p {}"'.format(port),
            warn_only=True,  # It's fine if the process already dead
        )

    def _netcat_port(self, device: str) -> int:
        """
        Get the minor ID for the device, calculates the netcat listen port for
        it, checks if netcat process already except, and returns the port
        """
        dev_minor = self.run('stat -L -c "%T" {}'.format(device), silent=True)
        dev_minor = int(dev_minor, 16)
        port = 7000 + dev_minor
        self.check_netcat(port)
        return port

    @contextmanager
    def netcat_to_device(self, vol: virStorageVol) -> Iterator[Tuple[str, int]]:
        """
        Spawns the background netcat process on the uniq port and pipes the
        payload to dd process to restore the file system on a target
        hypervisor. Kills the netcat process if exception is caught.

        :param virStorageVol vol: The libvirt volume
        :rtype Iterator[Tuple[str, int]]: (fqdn, port) pair
        """
        port = self._netcat_port(vol.path())

        # Using DD lowers load on device with big enough Block Size
        self.run(
            'nohup /bin/nc.openbsd -l -p {port} 2>>{log_file} '
            '| dd of={vol_path} obs=1048576 2>>{log_file} &'.format(
                port=port,
                log_file=self._log_filename('netcat', vol.name()),
                vol_path=vol.path(),
            ),
            pty=False,  # Has to be here for background processes
        )
        try:
            yield self.fqdn, port
        except BaseException:
            self.kill_netcat(port)
            raise

    def device_to_netcat(
        self, vol: virStorageVol, size: int, listener: Tuple[str, int],
    ):
        """
        Dumps the device via dd and netcat to a remote listener

        :param virStorageVol vol: The libvirt volume
        :param int size: The disk size for pv progress and ETA
        :param Tuple[str, int] listener: (fqdn, port) pair for nc connection
        """
        # Using DD lowers load on device with big enough Block Size
        self.run(
            'dd if={vol_path} ibs=1048576 | pv -f -s {size} '
            '| /bin/nc.openbsd -q 1 {target_host} {target_port}'.format(
                vol_path=vol.path(),
                size=size,
                target_host=listener[0],
                target_port=listener[1],
            ),
        )

    @staticmethod
    def _log_filename(transfer: str, device_name: str) -> str:
        return '/tmp/{}-{}.log'.format(transfer, device_name)

    def check_xfsrestore_log(self, vm: VM):
        """
        Search for WARNING in the xfsrestore log file.
        Raises exception if found
        """
        log_file = self._log_filename(
            'xfsrestore',
            self.get_volume_by_vm(vm).name(),
        )
        self.run('cat {}'.format(log_file))

        try:
            self.run(
                'grep -qE "WARNING|failed: end of recorded data" {} '
                '&& exit 1 || exit 0'.format(
                    log_file,
                ),
            )
        except RemoteCommandError as e:
            raise XfsMigrationError('xfs dump/restore caused warnings') from e

    @contextmanager
    def xfsrestore(
        self, vm: VM, transaction: Transaction = None
    ) -> Iterator[Tuple[str, int]]:
        """
        Formats a vm's storage, mounts it, spawns background netcat process
        and pipes the load to xfsrestore command to restore xfsdump on the
        target HV

        :param VM vm: The migrating VM
        :param Transaction transaction: The transaction to rollback
        :rtype Iterator[Tuple[str, int]]: (fqdn, port) pair
        """
        vol = self.get_volume_by_vm(vm)
        port = self._netcat_port(vol.path())
        mount_dir = self.format_vm_storage(vm, transaction)

        # xfsrestore args:
        #  -F: Don't prompt the operator.
        #  -J: inhibits inventory update
        # xfsrestore needs to output its logs, otherwise it fails
        self.run(
            'nohup /bin/nc.openbsd -l -p {port} 2>>{log_file} '
            '| ionice -c3 xfsrestore -F -J - {mount_dir} 2>>{log_file} 1>&2 &'
            .format(
                port=port,
                mount_dir=mount_dir,
                log_file=self._log_filename('xfsrestore', vol.name()),
            ),
            pty=False,  # Has to be here for background processes
        )
        try:
            yield self.fqdn, port
        except BaseException:
            self.kill_netcat(port)
            raise

    def wait_for_xfsrestore(self, vm: VM):
        """
        On the HV with slow IO the process must wait until xfsrestore is done
        """
        mount_dir = self.vm_mount_path(vm)
        self.run(
            'while pgrep -f "^xfsrestore -F -J - {0}"; do sleep 1; done'
            .format(mount_dir)
        )

    def xfsdump(self, vm: VM, listener, transaction: Transaction = None):
        """
        Mounts the vm's storage, and then dumps the device via xfsdump and
        netcat to a remote listener

        :param VM vm: The migrating VM
        :param Transaction transaction: The transaction to rollback
        """
        mount_dir = self.mount_vm_storage(vm, transaction)

        # xfsdump args:
        #  -l: level 0 is an absolute dump
        #  -F: Don't prompt the operator.
        #  -J: inhibits inventory update
        #  -p: progress update interval
        self.run(
            'ionice -c3 xfsdump -o -l 0 -F -J -p 1 - {mount_dir} '
            '| /bin/nc.openbsd -q 1 {target_host} {target_port}'
            .format(
                mount_dir=mount_dir,
                target_host=listener[0],
                target_port=listener[1],
            ),
        )
        self.umount_vm_storage(vm)

    def estimate_cpu_cores_used(self, vm: VM) -> float:
        """Estimate the number of CPU cores used by the VM

        Estimate the number of CPU cores used by the VM on the Hypervisor
        based on the known data of the past 24 hours by using the mathematical
        quotient of the VM performance value and the Hypervisors
        cpu_perffactor.

        :param: vm: VM object

        :return: number of CPU cores used on Hypervisor
        """

        vm_performance_value = vm.performance_value()

        # Serveradmin can not handle floats right now so we safe them as
        # multiple ones of thousand and just divide them here again.
        hv_cpu_perffactor = self.dataset_obj['cpu_perffactor'] / 1000
        cpu_cores_used = vm_performance_value / hv_cpu_perffactor

        return float(cpu_cores_used)

    def estimate_vm_cpu_usage(self, vm: VM) -> float:
        """Estimate CPU usage of a VM on the Hypervisor

        Estimate the CPU usage (as percent) on the Hypervisor.

        :param: vm: VM object

        :return: CPU usage on Hypervisor (as percent)
        """

        vm_cpu_cores = self.estimate_cpu_cores_used(vm)
        hv_num_cpu = self.dataset_obj['num_cpu']
        cpu_usage = (vm_cpu_cores / hv_num_cpu) * 100

        return float(cpu_usage)

    def estimate_cpu_usage(self, vm: VM) -> float:
        """Estimate the Hypervisor CPU usage with given VM

        Estimate the total CPU usage of the Hypervisor with the given VM on top
        based on the data we have from the past 24 hours.

        :param: vm: VM object

        :return: Cpu utilisation in percent as float
        """

        vm_cpu_usage = self.estimate_vm_cpu_usage(vm)
        hv_cpu_usage = self.dataset_obj['cpu_util_pct']

        # Take into account cpu_util_pct is outdated
        #
        # Take into account recent migrations from and to the Hypervisor to
        # avoid moving too many VMs to the same Hypervisor or discarding the
        # Hypervisor as candidate because the cpu_util_pct is not up-to-date
        # yet.
        #
        # The migration_log logs the migration of the past 24 hours after that
        # the cpu_util_pct should have up-to-date values.
        cpu_usage = sum([
            hv_cpu_usage,
            vm_cpu_usage,
            self.cpu_usage_of_recent_migrations(),
        ])

        # TODO: The sum of estimated CPU usage can be negative because
        #       cpu_usage_of_recent_migrations() can be negative.
        #       Make this more stable and account for this discrepancy.
        #       I.e. some if not all of the inputs are wrong!
        if cpu_usage < 0:
            log.error(
                'Estimated CPU usage for Hypervisor "{}" and VM "{}" is '
                'negative! Beware that this can lead to wrong '
                'assumptions! Setting to zero!'.format(
                    str(self),
                    str(vm),
                )
            )

            cpu_usage = max(0, cpu_usage)

        return float(cpu_usage)

    def cpu_usage_of_recent_migrations(self) -> int:
        """Summarized CPU usage of recent VM migrations

        Summarize the CPU usage of VMs recently migrated from or to this
        Hypervisor and return the total.

        :return: Total CPU usage of recently moved VMs
        """

        migration_log = self.dataset_obj['igvm_migration_log']
        total_cpu_usage = 0
        for vm_migration_log in migration_log:
            cpu_usage = vm_migration_log.split(' ')[1]
            total_cpu_usage = total_cpu_usage + int(cpu_usage)

        return total_cpu_usage

    def log_migration(self, vm: VM, operator: str) -> None:
        """Log migration to or from Hypervisor

        Save the estimated CPU usage of the VM to the migration log to be able
        to take recent migrations into account when this Hypervisor is selected
        as possible candidate.

        :param vm: VM object
        :param operator: plus for migration to HV, minus for migration from HV
        """

        cpu_usage_vm = round(self.estimate_vm_cpu_usage(vm))
        if cpu_usage_vm == 0:
            return

        timestamp = datetime.now().isoformat()
        log_entry = '{} {}{}'.format(timestamp, operator, cpu_usage_vm)

        self.dataset_obj['igvm_migration_log'].add(log_entry)
        self.dataset_obj.commit()
