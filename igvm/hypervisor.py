"""igvm - Hypervisor Model

Copyright (c) 2018, InnoGames GmbH
"""

import logging
import math
try:
    from urllib.error import URLError
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen, URLError

from libvirt import VIR_DOMAIN_SHUTOFF

from adminapi.dataset import Query
from adminapi.filters import Any, Empty, Not

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
from igvm.utils.backoff import retry_wait_backoff
from igvm.utils.network import get_network_config
from igvm.utils.virtutils import get_virtconn

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

    def vm_disk_path(self, name):
        return '/dev/{}/{}'.format(VG_NAME, name)

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
        vlans = [v['vlan_tag'] for v in Query({
            'hostname': Any(*self.dataset_obj['vlan_networks']),
            'vlan_tag': Not(Empty()),
        }, ['vlan_tag'])]

        vm_vlan = get_network_config(vm.dataset_obj)['vlan_tag']
        if not vlans:
            hypervisor_vlan = get_network_config(self.dataset_obj)['vlan_tag']
            if hypervisor_vlan != vm_vlan:
                raise HypervisorError(
                    'Hypervisor "{}" is not on same VLAN {} as VM {}.'
                    .format(self.fqdn, hypervisor_vlan, vm_vlan)
                )
            # For untagged Hypervisors VM must be untagged, too.
            return None

        # On source hypervisor, it is unnecessary to perform this check.
        # The VLAN is obviously there, even if not on Serveradmin.  This can
        # happen if VLAN is removed on Serveradmin so that nobody creates
        # new VMs on given hypervisor, but the existing ones must be moved out.
        if (
            self.dataset_obj['hostname'] not in [
                vm.dataset_obj['hypervisor'],
                # XXX: Deprecated attribute xen_host
                vm.dataset_obj['xen_host'],
            ] and vm_vlan not in vlans
        ):
            raise HypervisorError(
                'Hypervisor "{}" does not support VLAN {}.'
                .format(self.fqdn, vm_vlan)
            )
        return vm_vlan

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
        self.vlan_for_vm(vm)

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
                'delete VM', self.delete_vm, vm, keep_storage=True
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
        domain = self._get_domain(vm)
        with self.fabric_settings():
            self.lvresize(self.vm_disk_path(domain.name()), new_size_gib)

        self._vm_set_disk_size_gib(vm, new_size_gib)

    def create_vm_storage(self, vm, name, transaction=None):
        """Allocate storage for a VM. Returns the disk path."""
        self.create_storage(name, vm.dataset_obj['disk_size_gib'])
        if transaction:
            transaction.on_rollback(
                'destroy storage', self.lvremove, self.vm_disk_path(name)
            )

    def format_vm_storage(self, vm, transaction=None):
        """Create new filesystem for VM and mount it. Returns mount path."""

        if self.vm_defined(vm):
            raise InvalidStateError(
                'Refusing to format storage of defined VM "{}".'
                .format(vm.fqdn)
            )

        self.format_storage(self.vm_disk_path(vm.fqdn))
        return self.mount_vm_storage(vm, transaction)

    def validate_image_checksum(self, image):
        """Compares the local image checksum against the checksum returned by
        foreman."""
        local_hash = self.run(
            'md5sum {}/{}'.format(IMAGE_PATH, image)
        ).split()[0]

        url = IGVM_IMAGE_MD5_URL.format(image=image)
        try:
            remote_hash = urlopen(url, timeout=2).read().split()[0]
        except URLError as e:
            log.warning(
                'Failed to fetch image checksum at {}: {}'.format(url, e)
            )
            return False

        return local_hash == remote_hash

    def download_image(self, image):
        if (
            self.file_exists('{}/{}'.format(IMAGE_PATH, image)) and not
            self.validate_image_checksum(image)
        ):
            log.warning('Image validation failed, downloading latest version')
            self.run('rm -f {}/{}'.format(IMAGE_PATH, image))

        if not self.file_exists('{}/{}'.format(IMAGE_PATH, image)):
            url = IGVM_IMAGE_URL.format(image=image)
            self.run('wget -P {} -nv {}'.format(IMAGE_PATH, url))

    def extract_image(self, image, target_dir):
        if self.dataset_obj['os'] == 'squeeze':
            self.run(
                'tar xfz {}/{} -C {}'.format(IMAGE_PATH, image, target_dir)
            )
        else:
            self.run(
                "tar --xattrs --xattrs-include='*' -xzf {}/{} -C {}".format(
                    IMAGE_PATH, image, target_dir
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
            self.vm_disk_path(vm.fqdn), suffix=('-' + vm.fqdn)
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
        lvs = self.get_logical_volumes()
        domain = self._get_domain(vm)
        for lv in lvs:
            if lv['name'] == domain.name():
                result['disk_size_gib'] = int(math.ceil(lv['size_MiB'] / 1024))
                break
        else:
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
            if not (vm.fqdn == name or vm.fqdn.startswith(name + '.')):
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
                'Unable to find domain "{}".'.format(vm.fqdn)
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

        domain = self._get_domain(vm)
        if offline:
            target_hypervisor.create_vm_storage(vm, vm.fqdn, transaction)

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
                        self.vm_disk_path(vm.fqdn), subtransaction
                    )
                    self.device_to_netcat(
                        self.vm_disk_path(domain.name()),
                        vm.dataset_obj['disk_size_gib'] * 1024 ** 3,
                        nc_listener,
                        subtransaction,
                    )

            target_hypervisor.define_vm(vm, transaction)
        else:
            target_hypervisor.create_vm_storage(vm, domain.name(), transaction)
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

    def _vm_set_disk_size_gib(self, vm, disk_size_gib):
        # TODO: Use libvirt
        domain = self._get_domain(vm)
        self.run(
            'virsh blockresize --path {} --size {}GiB {}'
            .format(
                self.vm_disk_path(domain.name()), disk_size_gib, domain.name()
            )
        )
        vm.run('xfs_growfs /')

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

    def delete_vm(self, vm, keep_storage=False):
        if self.vm_running(vm):
            raise InvalidStateError(
                'Refusing to undefine running VM "{}"'.format(vm.fqdn)
            )
        log.info('Undefining "{}" on "{}"'.format(vm.fqdn, self.fqdn))
        domain = self._get_domain(vm)
        if domain.undefine() != 0:
            raise HypervisorError('Unable to undefine "{}".'.format(vm.fqdn))
        if not keep_storage:
            self.lvremove(self.vm_disk_path(domain.name()))

    def redefine_vm(self, vm):
        domain = self._get_domain(vm)
        self.delete_vm(vm, keep_storage=True)
        if domain.name() != vm.fqdn:
            with self.fabric_settings():
                self.lvrename(self.vm_disk_path(domain.name()), vm.fqdn)
        self.define_vm(vm)

    def rename_vm(self, vm, new_fqdn):
        domain = self._get_domain(vm)
        self.delete_vm(vm, keep_storage=True)
        with self.fabric_settings():
            self.lvrename(self.vm_disk_path(domain.name()), new_fqdn)
        vm.fqdn = new_fqdn
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

    def get_logical_volumes(self):
        lvolumes = []
        lvs = self.run(
            'lvs --noheadings -o name,vg_name,lv_size --unit b --nosuffix'
            ' 2>/dev/null',
            silent=True
        )
        for lv_line in lvs.splitlines():
            lv_name, vg_name, lv_size = lv_line.split()
            lvolumes.append({
                'path': '/dev/{}/{}'.format(vg_name, lv_name),
                'name': lv_name,
                'vg_name': vg_name,
                'size_MiB': math.ceil(float(lv_size) / 1024 ** 2),
            })
        return lvolumes

    def lvremove(self, lv):
        self.run('lvremove -f {0}'.format(lv))

    def lvresize(self, volume, size_gib):
        """Extend the volume, return the new size"""

        self.run('lvresize {0} -L {1}g'.format(volume, size_gib))

    def lvrename(self, volume, newname):
        self.run('lvrename {0} {1}'.format(volume, newname))

    def get_free_disk_size_gib(self, safe=True):
        """Return free disk space as float in GiB"""
        vgs_line = self.run(
            'vgs --noheadings -o vg_name,vg_free --unit b --nosuffix {0}'
            ' 2>/dev/null'
            .format(VG_NAME),
            silent=True,
        )
        vg_name, vg_size_gib = vgs_line.split()
        # Floor instead of ceil because we check free instead of used space
        vg_size_gib = math.floor(float(vg_size_gib) / 1024 ** 3)
        if safe is True:
            vg_size_gib -= RESERVED_DISK
        assert vg_name == VG_NAME
        return vg_size_gib

    def create_storage(self, name, disk_size_gib):
        self.run('lvcreate -y -L {}g -n {} {}'.format(
            disk_size_gib,
            name,
            VG_NAME,
        ))

    def mount_temp(self, device, suffix=''):
        mount_dir = self.run('mktemp -d --suffix {}'.format(suffix))
        self.run('mount {0} {1}'.format(device, mount_dir))
        return mount_dir

    def umount_temp(self, device_or_path):
        self.run('umount {0}'.format(device_or_path))

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
        domain = self._get_domain(vm)

        self.host_drbd = DRBD(self, VG_NAME, domain.name(), vm.fqdn, True)
        self.peer_drbd = DRBD(peer, VG_NAME, vm.fqdn, vm.fqdn)

        with Transaction() as transaction:
            self.host_drbd.start(self.peer_drbd, transaction)
            self.peer_drbd.start(self.host_drbd, transaction)

    def wait_for_sync(self):
        self.host_drbd.wait_for_sync()
        self.peer_drbd.wait_for_sync()

    def stop_drbd(self):
        self.host_drbd.stop()
        self.peer_drbd.stop()
