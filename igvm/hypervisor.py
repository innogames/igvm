"""igvm - Hypervisor Model

Copyright (c) 2018 InnoGames GmbH
"""

import logging
import math
from time import sleep

from libvirt import VIR_DOMAIN_SHUTOFF

from igvm.exceptions import (
    ConfigError,
    HypervisorError,
    InconsistentAttributeError,
    InvalidStateError,
    StorageError,
)
from igvm.drbd import DRBD
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
    HOST_RESERVED_MEMORY,
    VG_NAME,
    RESERVED_DISK,
    IGVM_IMAGE_URL,
    IGVM_IMAGE_MD5_URL,
    IMAGE_PATH,
    MIGRATE_COMMANDS,
    KVM_HWMODEL_TO_CPUMODEL,
)
from igvm.transaction import Transaction
from igvm.utils import retry_wait_backoff

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

        # Store per-VM path information
        # We cannot store these in the VM object due to migrations.
        self._mount_path = {}
        self.storage_pool = self.conn().storagePoolLookupByName(VG_NAME)

    def get_volume_by_vm(self, vm):
        """Get logical volume information of a VM"""
        domain = self._find_domain(vm)
        for vol_name in self.storage_pool.listVolumes():
            if (
                # Match the LV based on the object_id encoded within its name
                vm.match_uid_name(vol_name) or
                # XXX: Deprecated matching for LVs w/o an uid_name
                domain and vol_name == domain.name()
            ):
                return self.storage_pool.storageVolLookupByName(vol_name)

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
                self.storage_pool.refresh()

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
            if ip_addr in vlan_network['intern_ip']:
                return vlan_network
        return None

    def vm_max_memory(self, vm):
        """Calculates the max amount of memory in MiB the VM may receive."""
        mem = vm.dataset_obj['memory']
        if mem > 12 * 1024:
            max_mem = mem + 10 * 1024
        else:
            max_mem = 16 * 1024

        # Never go higher than the hypervisor
        max_mem = min(self.total_vm_memory(), max_mem)

        return max_mem

    def check_vm(self, vm, offline):
        """Check whether a VM can run on this hypervisor"""
        if self.dataset_obj['state'] not in ['online', 'online_reserved']:
            raise InvalidStateError(
                'Hypervisor "{}" is not in online state ({}).'
                .format(self.fqdn, self.dataset_obj['state'])
            )

        if self.vm_defined(vm):
            raise HypervisorError(
                'VM "{}" is already defined on "{}".'
                .format(vm.fqdn, self.fqdn)
            )

        # Enough CPUs?
        if vm.dataset_obj['num_cpu'] > self.dataset_obj['num_cpu']:
            raise HypervisorError(
                'Not enough CPUs. Destination Hypervisor has {0}, '
                'but VM requires {1}.'
                .format(self.dataset_obj['num_cpu'], vm.dataset_obj['num_cpu'])
            )

        # Enough memory?
        free_mib = self.free_vm_memory()
        if vm.dataset_obj['memory'] > free_mib:
            raise HypervisorError(
                'Not enough memory. '
                'Destination Hypervisor has {:.2f} MiB but VM requires {} MiB '
                .format(free_mib, vm.dataset_obj['memory'])
            )

        if not offline:
            # Compatbile OS?
            os_pair = (vm.hypervisor.dataset_obj['os'], self.dataset_obj['os'])
            if os_pair not in MIGRATE_COMMANDS:
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

        # Enough disk?
        free_disk_space = self.get_free_disk_size_gib()
        vm_disk_size = float(vm.dataset_obj['disk_size_gib'])
        if vm_disk_size > free_disk_space:
            raise HypervisorError(
                'Not enough free space in VG {} to build VM while keeping'
                ' {} GiB reserved'
                .format(VG_NAME, RESERVED_DISK)
            )

        # Proper VLAN?
        if not self.get_vlan_network(vm.dataset_obj['intern_ip']):
            raise HypervisorError(
                'Hypervisor "{}" does not support route_network "{}".'
                .format(self.fqdn, vm.dataset_obj['route_network'])
            )

    def define_vm(self, vm, transaction=None):
        """Creates a VM on the hypervisor."""
        log.info('Defining "{}" on "{}"...'.format(vm.fqdn, self.fqdn))

        self.conn().defineXML(generate_domain_xml(self, vm))

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
        else:
            old_total = vm.meminfo()['MemTotal']
            set_memory(self, vm, self._get_domain(vm))
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

        vm.dataset_obj.commit()

    def vm_set_disk_size_gib(self, vm, new_size_gib):
        """Changes disk size of a VM."""
        if new_size_gib < vm.dataset_obj['disk_size_gib']:
            raise NotImplementedError('Cannot shrink the disk.')
        volume = self.get_volume_by_vm(vm)
        # There is no resize function in version of libvirt
        # available in Debian 9.
        self.run('lvresize {} -L {}g'.format(volume.path(), new_size_gib))
        self.storage_pool.refresh()
        self._get_domain(vm).blockResize(
            'vda',
            new_size_gib * 1024 ** 2,  # Yes, it is in KiB
        )
        vm.run('xfs_growfs /')

    def create_vm_storage(self, vm, transaction=None, vol_name=None):
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

        volume = self.storage_pool.createXML(volume_xml, 0)
        if volume is None:
            raise StorageError(
                'Failed to create storage volume {}/{}'.format(
                    self.storage_pool.name(),
                    vol_name,
                )
            )

        if transaction:
            transaction.on_rollback('destroy storage', volume.delete)

    def format_vm_storage(self, vm, transaction=None):
        """Create new filesystem for VM and mount it. Returns mount path."""

        if self.vm_defined(vm):
            raise InvalidStateError(
                'Refusing to format storage of defined VM "{}".'
                .format(vm.fqdn)
            )

        self.format_storage(self.get_volume_by_vm(vm).path())
        return self.mount_vm_storage(vm, transaction)

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
            'sed -i \'s_ /.*/_{img_path}/_\' {img_path}/{img_file}.md5 ; '
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
            self.get_volume_by_vm(vm).path(), suffix=('-' + vm.fqdn)
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
            result['disk_size_gib'] = int(math.ceil(vol_size / 1024 ** 3))
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
            if not (
                # Match the domain based on the object_id encoded in its name
                vm.match_uid_name(name) or
                # XXX: Deprecated matching for domains w/o an uid_name
                vm.fqdn == name or vm.fqdn.startswith(name + '.')
            ):
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

    def migrate_vm(
        self, vm, target_hypervisor, maintenance, offline, offline_transport,
        transaction,
    ):
        if offline_transport not in ['netcat', 'drbd']:
            raise StorageError(
                'Unknown offline transport method {}!'
                .format(offline_transport)
            )

        if offline:
            target_hypervisor.create_vm_storage(vm, transaction)
            if offline_transport == 'drbd':
                self.start_drbd(vm, target_hypervisor)
                try:
                    self.wait_for_sync()
                    if maintenance or offline:
                        vm.set_state('maintenance', transaction=transaction)
                    if vm.is_running():
                        vm.shutdown(transaction)
                finally:
                    self.stop_drbd()

            elif offline_transport == 'netcat':
                if maintenance or offline:
                    vm.set_state('maintenance', transaction=transaction)
                if vm.is_running():
                    vm.shutdown(transaction)
                with Transaction() as subtransaction:
                    nc_listener = target_hypervisor.netcat_to_device(
                        target_hypervisor.get_volume_by_vm(vm).path(),
                        subtransaction
                    )
                    self.device_to_netcat(
                        self.get_volume_by_vm(vm).path(),
                        vm.dataset_obj['disk_size_gib'] * 1024 ** 3,
                        nc_listener,
                        subtransaction,
                    )
            target_hypervisor.define_vm(vm, transaction)
        else:
            # For online migrations always use same volume name as VM
            # already has.
            target_hypervisor.create_vm_storage(
                vm, transaction,
                vm.hypervisor.get_volume_by_vm(vm).name(),
            )
            migrate_live(self, target_hypervisor, vm, self._get_domain(vm))

    def total_vm_memory(self):
        """Get amount of memory in MiB available to hypervisor"""
        # Start with what OS sees as total memory (not installed memory)
        total_mib = self.conn().getMemoryStats(-1)['total'] // 1024
        # Always keep some extra memory free for Hypervisor
        total_mib -= HOST_RESERVED_MEMORY
        return total_mib

    def free_vm_memory(self):
        """Get memory in MiB available (unallocated) on the hypervisor"""
        total_mib = self.total_vm_memory()

        # Calculate memory used by other VMs.
        # We can not trust conn().getFreeMemory(), sum up memory used by
        # each VM instead
        used_kib = 0
        for dom_id in self.conn().listDomainsID():
            dom = self.conn().lookupByID(dom_id)
            used_kib += dom.info()[2]
        free_mib = total_mib - used_kib / 1024
        return free_mib

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
        self.define_vm(vm)

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

    def get_free_disk_size_gib(self, safe=True):
        """Return free disk space as float in GiB"""
        pool_info = self.storage_pool.info()
        # Floor instead of ceil because we check free instead of used space
        vg_size_gib = math.floor(float(pool_info[3]) / 1024 ** 3)
        if safe is True:
            vg_size_gib -= RESERVED_DISK
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
        Underlying process such as mkswap or puppetrun won't die immediately.
        """

        retry = 10
        for i in range(0, retry):
            if i > 0:
                log.warning(
                    'Umounting {} failed, attempting again in a moment. '
                    '{} attempts left.'.format(
                        device_or_path,
                        retry - i,
                        ))
                sleep(1)
            res = self.run(
                'umount {0}'.format(device_or_path),
                warn_only=(i < retry-1),
            )
            if res.succeeded:
                return

    def remove_temp(self, mount_path):
        self.run('rmdir {0}'.format(mount_path))

    def format_storage(self, device):
        self.run('mkfs.xfs -f {}'.format(device))

    def check_netcat(self, port):
        pid = self.run(
            'pgrep -f "^/bin/nc.traditional -l -p {}"'
            .format(port),
            warn_only=True,
            silent=True
        )

        if pid:
            raise StorageError(
                'Listening netcat already found on destination hypervisor.'
            )

    def kill_netcat(self, port):
        self.run('pkill -f "^/bin/nc.traditional -l -p {}"'.format(port))

    def netcat_to_device(self, device, transaction=None):
        dev_minor = self.run('stat -L -c "%T" {}'.format(device), silent=True)
        dev_minor = int(dev_minor, 16)
        port = 7000 + dev_minor

        self.check_netcat(port)

        # Using DD lowers load on device with big enough Block Size
        self.run(
            'nohup /bin/nc.traditional -l -p {0} | dd of={1} obs=1048576 &'
            .format(port, device)
        )
        if transaction:
            transaction.on_rollback('kill netcat', self.kill_netcat, port)
        return self.fqdn, port

    def device_to_netcat(self, device, size, listener, transaction=None):
        # Using DD lowers load on device with big enough Block Size
        self.run(
            'dd if={0} ibs=1048576 | pv -f -s {1} '
            '| /bin/nc.traditional -q 1 {2} {3}'
            .format(device, size, *listener)
        )

    def start_drbd(self, vm, peer):
        # Ensure that current domain name is used, this might be non-FQDN
        # one on source Hypervisor.
        self.host_drbd = DRBD(self, vm, master_role=True)
        self.peer_drbd = DRBD(peer, vm)

        with Transaction() as transaction:
            self.host_drbd.start(self.peer_drbd, transaction)
            self.peer_drbd.start(self.host_drbd, transaction)

    def wait_for_sync(self):
        self.host_drbd.wait_for_sync()
        self.peer_drbd.wait_for_sync()

    def stop_drbd(self):
        self.host_drbd.stop()
        self.peer_drbd.stop()
