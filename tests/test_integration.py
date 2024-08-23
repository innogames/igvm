"""igvm - Integration Tests

Copyright (c) 2020 InnoGames GmbH
"""
from __future__ import print_function

from logging import INFO, basicConfig
from os import environ
from tempfile import NamedTemporaryFile
from unittest import TestCase

from adminapi import api
from adminapi.dataset import Query
from adminapi.filters import Any
from fabric.api import env
from fabric.network import disconnect_all
from mock import patch

from igvm.commands import (
    _get_vm,
    change_address,
    disk_set,
    host_info,
    mem_set,
    vcpu_set,
    vm_build,
    vm_define,
    vm_delete,
    vm_migrate,
    vm_rename,
    vm_restart,
    vm_start,
    vm_stop,
    vm_sync,
)
from igvm.exceptions import (
    IGVMError,
    InconsistentAttributeError,
    InvalidStateError,
    StorageError,
    VMError,
    XfsMigrationError,
)
from igvm.hypervisor import Hypervisor
from igvm.puppet import clean_cert
from igvm.settings import (
    AWS_RETURN_CODES,
    COMMON_FABRIC_SETTINGS,
    HYPERVISOR_ATTRIBUTES,
    HYPERVISOR_CPU_THRESHOLDS,
    KVM_HWMODEL_TO_CPUMODEL,
    VG_NAME,
    VM_ATTRIBUTES
)
from igvm.utils import parse_size
from igvm.vm import VM
from tests import VM_HOSTNAME, VM_NET
from tests.conftest import (
    clean_all,
    cmd,
    get_next_address,
)

basicConfig(level=INFO)
env.update(COMMON_FABRIC_SETTINGS)
environ['IGVM_SSH_USER'] = 'igtesting'  # Enforce user for integration testing
env.user = 'igtesting'
environ['IGVM_MODE'] = 'staging'


def teardown_module():
    disconnect_all()


class IGVMTest(TestCase):
    def setUp(self):
        """Initialize VM object before every test

        Get object from Serveradmin and initialize it to safe defaults.
        Don't assign VM to any of HVs yet!
        """
        super().setUp()

        # Check that enough HVs are available.
        self.route_network = Query(
            {'hostname': VM_NET},
            ['route_network'],
        ).get()['route_network']

        self.datacenter_type = Query(
            {'hostname': self.route_network},
            ['datacenter_type'],
        ).get()['datacenter_type']

        self.hvs = [Hypervisor(o) for o in Query({
            'environment': Any('testing', 'staging'),
            'servertype': 'hypervisor',
            'state': 'online',
            'vlan_networks': self.route_network,
        }, HYPERVISOR_ATTRIBUTES)]

        if self.datacenter_type == 'kvm.dct':
            assert len(self.hvs) >= 2, 'Not enough testing hypervisors found'

        # Cleanup all leftovers from previous tests or failures.
        clean_all(self.route_network, self.datacenter_type, VM_HOSTNAME)

        # Create subject VM object
        self.vm_obj = Query().new_object('vm')
        self.vm_obj['backup_disabled'] = True
        self.vm_obj['disk_size_gib'] = 3
        self.vm_obj['environment'] = 'testing'
        self.vm_obj['hostname'] = VM_HOSTNAME
        self.vm_obj['hypervisor'] = None
        self.vm_obj['intern_ip'] = get_next_address(VM_NET, 1)
        self.vm_obj['memory'] = 2048
        self.vm_obj['no_monitoring'] = True
        self.vm_obj['num_cpu'] = 2
        self.vm_obj['os'] = 'bullseye'
        self.vm_obj['project'] = 'test'
        self.vm_obj['puppet_environment'] = None
        self.vm_obj['puppet_ca'] = 'testing-puppetca.innogames.de'
        self.vm_obj['puppet_master'] = 'puppet-lb.test.innogames.net'
        self.vm_obj['repositories'] = [
            'int:basebuster:stable',
            'int:innogames:stable',
        ]
        self.vm_obj['state'] = 'online'

        if self.datacenter_type == 'aws.dct':
            self.vm_obj['aws_image_id'] = 'ami-0e2b90ca04cae8da5'  # buster
            self.vm_obj['aws_instance_type'] = 't2.micro'
            self.vm_obj['aws_key_name'] = 'eu-central-1-key'
            self.vm_obj['disk_size_gib'] = 8

        self.vm_obj.commit()

        # It would be enough to create SGs in AWS once but with parallel runs
        # we can't really test if sync has already been performed.
        if self.datacenter_type == 'aws.dct':
            fw_api = api.get('firewall')
            fw_api.update_config([self.route_network])

        self.uid_name = '{}_{}'.format(
            self.vm_obj['object_id'],
            self.vm_obj['hostname'],
        )

        # Make sure we can make a fresh build
        clean_cert(self.vm_obj)

    def tearDown(self):
        """Forcibly remove current test's VM from all HVs"""
        super().tearDown()

        clean_cert(self.vm_obj)
        clean_all(self.route_network, self.datacenter_type, VM_HOSTNAME)

    def check_vm_present(self, vm_name=VM_HOSTNAME):
        # Operate on fresh object
        with _get_vm(vm_name) as vm:
            for hv in self.hvs:
                if hv.dataset_obj['hostname'] == vm.dataset_obj['hypervisor']:
                    # Is it on correct HV?
                    self.assertEqual(hv.vm_defined(vm), True)
                    self.assertEqual(hv.vm_running(vm), True)
                else:
                    # Is it gone from other HVs after migration?
                    self.assertEqual(hv.vm_defined(vm), False)
                    hv.run(
                        'test ! -b /dev/{}/{}'.format(VG_NAME, self.uid_name)
                    )

            # Is VM itself alive and fine?
            fqdn = vm.run('hostname -f').strip()
            self.assertEqual(fqdn, vm.fqdn)
            self.assertEqual(vm.dataset_obj.is_dirty(), False)

    def check_vm_absent(self, vm_name=VM_HOSTNAME, hv_name=None):
        # Operate on fresh object
        with _get_vm(vm_name, allow_retired=True) as vm:
            if self.datacenter_type == 'kvm.dct':
                if not hv_name:
                    hv_name = vm.dataset_obj['hypervisor']

                for hv in self.hvs:
                    if hv.dataset_obj['hostname'] == hv_name:
                        self.assertEqual(hv.vm_defined(vm), False)
                        hv.run(
                            'test ! -b /dev/{}/{}'.format(
                                VG_NAME, self.uid_name)
                        )
            elif self.datacenter_type == 'aws.dct':
                self.assertEqual(
                    vm.aws_describe_instance_status(
                        vm.dataset_obj['aws_instance_id']),
                    AWS_RETURN_CODES['terminated']
                )


class SettingsHardwareModelTest(TestCase):
    """Test that all hardware_models of all hypervisors have a cpu threshold
    value defined in HYPERVISOR_CPU_THRESHOLDS dict"""

    def setUp(self):
        self.hardware_models = set([
            x['hardware_model'] for x in Query({
                'servertype': 'hypervisor',
                'project': 'ndco',
                'state': Any('online', 'online_reserved'),
            }, ['hardware_model'])
        ])

    def test_hypervisor_cpu_thresholds(self):
        for model in self.hardware_models:
            self.assertIn(model, HYPERVISOR_CPU_THRESHOLDS)

    def test_kvm_hwmodel_to_cpumodel(self):
        models = [
            cpu_model for cpu_models in KVM_HWMODEL_TO_CPUMODEL.values()
            for cpu_model in cpu_models]
        for model in self.hardware_models:
            self.assertIn(
                model, models,
                msg='Missing hardware_model in KVM_HWMODEL_TO_CPUMODEL')


class BuildTest(IGVMTest):
    """Test many possible VM building scenarios"""

    def test_vm_build(self):
        vm_build(VM_HOSTNAME)
        self.check_vm_present()

    def test_postboot(self):
        with NamedTemporaryFile() as fd:
            fd.write('echo hello > /root/postboot_result'.encode())
            fd.flush()

            vm_build(VM_HOSTNAME, postboot=fd.name)
            self.check_vm_present()

            with _get_vm(VM_HOSTNAME) as vm:
                output = vm.run('cat /root/postboot_result')
            self.assertIn('hello', output)

    def test_delete(self):
        vm_build(VM_HOSTNAME)
        self.check_vm_present()

        # Fails while VM is powered on
        with self.assertRaises(IGVMError):
            vm_delete(VM_HOSTNAME)

        vm_stop(VM_HOSTNAME)
        vm_delete(VM_HOSTNAME, retire=True)

        self.check_vm_absent()

    def test_rollback(self):
        # TODO: consider the usage of self.vm_obj instead of new Query
        obj = Query({'hostname': VM_HOSTNAME}, ['puppet_environment']).get()
        obj['puppet_environment'] = 'doesnotexist'
        obj.commit()

        with self.assertRaises(VMError):
            vm_build(VM_HOSTNAME)

        self.check_vm_absent()

    def test_rebuild(self):
        vm_build(VM_HOSTNAME)

        # Build the VM again, this must fail, as it is already built
        with self.assertRaises(IGVMError):
            vm_build(VM_HOSTNAME)

        # Create files on VM to check later if the VM was really rebuilt
        with _get_vm(VM_HOSTNAME) as vm:
            vm.run('touch /root/initial_canary')
            vm.run('test -f /root/initial_canary')

        # Now stop it and rebuild it
        vm_stop(VM_HOSTNAME)
        vm_build(VM_HOSTNAME, rebuild=True)

        self.check_vm_present()

        # The VM was rebuild and thus the test file must be gone
        with _get_vm(VM_HOSTNAME) as vm:
            vm.run('test ! -f /root/initial_canary')


class CommandTest(IGVMTest):
    def setUp(self):
        super(CommandTest, self).setUp()
        vm_build(VM_HOSTNAME)
        self.check_vm_present()
        with _get_vm(VM_HOSTNAME) as vm:
            # For contacting VM over shell
            self.vm = vm

    def tearDown(self):
        super(CommandTest, self).tearDown()

    def test_start_stop(self):
        # Doesn't fail, but should print a message
        vm_start(VM_HOSTNAME)
        self.check_vm_present()

        vm_restart(VM_HOSTNAME)
        self.check_vm_present()

        vm_stop(VM_HOSTNAME)
        self.assertEqual(self.vm.is_running(), False)

        vm_start(VM_HOSTNAME)
        self.assertEqual(self.vm.is_running(), True)

        vm_stop(VM_HOSTNAME, force=True)
        self.assertEqual(self.vm.is_running(), False)
        vm_start(VM_HOSTNAME)

        vm_restart(VM_HOSTNAME, force=True)
        self.check_vm_present()

    def test_disk_set(self):
        if self.datacenter_type == 'kvm.dct':
            def _get_disk_hv():
                return (
                    self.vm.hypervisor.vm_sync_from_hypervisor(self.vm)
                    ['disk_size_gib']
                )

            def _get_disk_vm():
                return parse_size(
                    self.vm.run("df -h / | tail -n+2 | awk '{ print $2 }'")
                    .strip(),
                    'G'
                )
        elif self.datacenter_type == 'aws.dct':
            def _get_disk_vm():
                partition = self.vm.run('findmnt -nro SOURCE /')
                disk = self.vm.run('lsblk -nro PKNAME {}'.format(partition))
                disk_size = self.vm.run(
                    'lsblk -bdnro size /dev/{}'.format(disk))
                disk_size_gib = int(disk_size) / 1024**3
                return disk_size_gib

        # Initial size same as built
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()
        size = obj['disk_size_gib']
        if self.datacenter_type == 'kvm.dct':
            self.assertEqual(_get_disk_hv(), size)
        self.assertEqual(_get_disk_vm(), size)

        size = size + 1
        disk_set(VM_HOSTNAME, '+1')
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()

        self.assertEqual(obj['disk_size_gib'], size)
        if self.datacenter_type == 'kvm.dct':
            self.assertEqual(_get_disk_hv(), size)
        self.assertEqual(_get_disk_vm(), size)

        size = obj['disk_size_gib'] + 1
        if self.datacenter_type == 'kvm.dct':
            disk_set(VM_HOSTNAME, '{}GB'.format(size))
            obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()
            self.assertEqual(obj['disk_size_gib'], size)
            self.assertEqual(_get_disk_hv(), size)
            self.assertEqual(_get_disk_vm(), size)
        elif self.datacenter_type == 'aws.dct':
            with self.assertRaises(VMError):
                disk_set(VM_HOSTNAME, '{}GB'.format(size))

        if self.datacenter_type == 'kvm.dct':
            with self.assertRaises(Warning):
                disk_set(VM_HOSTNAME, '{}GB'.format(size))

        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()
        size = obj['disk_size_gib']
        with self.assertRaises(NotImplementedError):
            disk_set(VM_HOSTNAME, '{}GB'.format(size - 1))

        with self.assertRaises(NotImplementedError):
            disk_set(VM_HOSTNAME, '-1')

    def test_mem_set(self):
        def _get_mem_hv():
            data = self.vm.hypervisor.vm_sync_from_hypervisor(self.vm)
            return data['memory']

        def _get_mem_vm():
            return float(
                self.vm
                .run("cat /proc/meminfo | grep MemTotal | awk '{ print $2 }'")
                .strip()
            ) // 1024

        # Online
        self.assertEqual(_get_mem_hv(), 2048)
        vm_mem = _get_mem_vm()
        mem_set(VM_HOSTNAME, '+1G')
        self.assertEqual(_get_mem_hv(), 3072)
        self.assertEqual(_get_mem_vm() - vm_mem, 1024)

        with self.assertRaises(Warning):
            mem_set(VM_HOSTNAME, '3G')

        with self.assertRaises(InvalidStateError):
            mem_set(VM_HOSTNAME, '2G')

        with self.assertRaises(IGVMError):
            mem_set(VM_HOSTNAME, '200G')

        # Not dividable
        with self.assertRaises(IGVMError):
            mem_set(VM_HOSTNAME, '4097M')

        self.assertEqual(_get_mem_hv(), 3072)
        vm_mem = _get_mem_vm()
        self.vm.shutdown()

        with self.assertRaises(IGVMError):
            mem_set(VM_HOSTNAME, '200G')

        mem_set(VM_HOSTNAME, '1024M')
        self.assertEqual(_get_mem_hv(), 1024)

        mem_set(VM_HOSTNAME, '2G')
        self.assertEqual(_get_mem_hv(), 2048)
        self.vm.start()
        self.assertEqual(_get_mem_vm() - vm_mem, -1024)

    def test_vcpu_set(self):
        def _get_cpus_hv():
            data = self.vm.hypervisor.vm_sync_from_hypervisor(self.vm)
            return data['num_cpu']

        def _get_cpus_vm():
            return int(
                self.vm.run('cat /proc/cpuinfo | grep vendor_id | wc -l')
                .strip()
            )

        # Online
        self.assertEqual(_get_cpus_hv(), 2)
        self.assertEqual(_get_cpus_vm(), 2)
        obj = Query({'hostname': VM_HOSTNAME}, ['num_cpu']).get()
        self.assertEqual(obj['num_cpu'], 2)
        vcpu_set(VM_HOSTNAME, 3)
        self.assertEqual(_get_cpus_hv(), 3)
        self.assertEqual(_get_cpus_vm(), 3)

        obj = Query({'hostname': VM_HOSTNAME}, ['num_cpu']).get()
        self.assertEqual(obj['num_cpu'], 3)

        with self.assertRaises(Warning):
            vcpu_set(VM_HOSTNAME, 3)

        # Online reduce not implemented yet on KVM
        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, 2)

        # Offline
        vcpu_set(VM_HOSTNAME, 2, offline=True)
        self.assertEqual(_get_cpus_hv(), 2)
        self.assertEqual(_get_cpus_vm(), 2)

        # Impossible amount
        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, 9001)

        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, 0, offline=True)

        # Has to be offline
        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, '-1')

        # Not enough CPUs to remove
        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, '-5', offline=True)

        vcpu_set(VM_HOSTNAME, '+2')
        self.assertEqual(_get_cpus_hv(), 4)
        self.assertEqual(_get_cpus_vm(), 4)

        vcpu_set(VM_HOSTNAME, '-2', offline=True)
        self.assertEqual(_get_cpus_hv(), 2)
        self.assertEqual(_get_cpus_vm(), 2)

    def test_sync(self):
        obj = (
            Query({'hostname': VM_HOSTNAME}, ['disk_size_gib', 'memory']).get()
        )
        expected_disk_size = obj['disk_size_gib']
        obj['disk_size_gib'] += 10

        expected_memory = obj['memory']
        obj['memory'] += 1024

        obj.commit()

        vm_sync(VM_HOSTNAME)

        obj = (
            Query({'hostname': VM_HOSTNAME}, ['disk_size_gib', 'memory']).get()
        )
        self.assertEqual(obj['memory'], expected_memory)
        self.assertEqual(obj['disk_size_gib'], expected_disk_size)

        # Shouldn't do anything, but also shouldn't fail
        vm_sync(VM_HOSTNAME)

    def test_info(self):
        host_info(VM_HOSTNAME)
        self.vm.shutdown()
        host_info(VM_HOSTNAME)

    @patch('igvm.vm.VM.performance_value', return_value=5.0)
    @patch('igvm.hypervisor.time', return_value=1234567890)
    def test_igvm_migration_log(self, performance_value, mock_time):
        for hv in self.hvs:
            hv.dataset_obj['igvm_migration_log'].clear()
            hv.dataset_obj.commit()

        src_hv = self.vm.hypervisor.dataset_obj['hostname']
        cpu_usage_vm_src = self.vm.hypervisor.estimate_vm_cpu_usage(self.vm)
        timestamp = 1234567890

        vm_migrate(
            VM_HOSTNAME,
            offline=True,
            offline_transport='drbd',
        )

        src_hv_obj = (
            Query({'hostname': src_hv}, ['igvm_migration_log']).get()
        )

        self.assertEqual(
            list(src_hv_obj['igvm_migration_log']),
            ['{} -{}'.format(timestamp, round(cpu_usage_vm_src))]
        )

        with _get_vm(VM_HOSTNAME) as vm:
            dest_hv_obj = (
                Query(
                    {'hostname': vm.hypervisor.dataset_obj['hostname']},
                    ['igvm_migration_log']).get()
            )
            cpu_usage_vm_dest = vm.hypervisor.estimate_vm_cpu_usage(vm)
            self.assertIn(
                '{} +{}'.format(timestamp, round(cpu_usage_vm_dest)),
                list(dest_hv_obj['igvm_migration_log'])
            )

    def test_vm_define(self):
        vm_dataset_obj = Query({'hostname': VM_HOSTNAME}, VM_ATTRIBUTES).get()

        hv = Hypervisor(vm_dataset_obj['hypervisor'])
        vm = VM(vm_dataset_obj, hv)

        vm_stop(VM_HOSTNAME)
        hv.undefine_vm(vm, keep_storage=True)

        self.check_vm_absent()
        vm_define(VM_HOSTNAME)
        self.check_vm_present()


class MigrationTest(IGVMTest):
    def setUp(self):
        super(MigrationTest, self).setUp()
        vm_build(VM_HOSTNAME)
        with _get_vm(VM_HOSTNAME) as vm:
            self.old_hv_name = vm.hypervisor.dataset_obj['hostname']

    def _xfs_migrate_wrapper(self, *args, **kwargs):
        """
        xfs dump/restore cause the corrupted files on restored disk from time
        to time. This is relatively rare case and I am not able to find the
        root reason yet. The rollback works quite good, but it fails to
        migrate. This wrapper do two retries on this particullar error
        """
        for _ in range(3):
            try:
                vm_migrate(*args, **kwargs)
                return
            except XfsMigrationError as e:
                exc = e
                if e.args[0] == 'xfs dump/restore caused warnings':
                    continue
                raise exc
        raise BaseException('xfs migration failed in 3 attempts')

    def test_online_migration(self):
        vm_migrate(VM_HOSTNAME)
        self.check_vm_present()

    def test_offline_migration_netcat(self):
        vm_migrate(
            VM_HOSTNAME,
            offline=True,
            offline_transport='netcat',
        )
        self.check_vm_present()

    def test_offline_migration_drbd(self):
        vm_migrate(
            VM_HOSTNAME,
            offline=True,
            offline_transport='drbd',
        )

        self.check_vm_present()

    def test_offline_migration_xfs(self):
        # Migrate without disk resizing
        self._xfs_migrate_wrapper(
            VM_HOSTNAME,
            offline=True,
            offline_transport='xfs',
        )
        self.check_vm_present()

    def test_offline_migration_xfs_disk_increase(self):
        # Increase disk size during migration
        disk_size_gib = self.vm_obj['disk_size_gib']
        self._xfs_migrate_wrapper(
            VM_HOSTNAME,
            offline=True,
            offline_transport='xfs',
            disk_size=disk_size_gib + 1,
        )
        self.check_vm_present()

    def test_offline_migration_xfs_disk_decrease(self):
        # Decreasing disk size back during migration
        disk_size_gib = self.vm_obj['disk_size_gib']
        disk_set(VM_HOSTNAME, '+1')
        self.check_vm_present()

        self._xfs_migrate_wrapper(
            VM_HOSTNAME,
            offline=True,
            offline_transport='xfs',
            disk_size=disk_size_gib,
        )
        self.check_vm_present()

    def test_offline_migration_xfs_disk_resize_failure(self):
        # Attempt to decrease disk size lower than allocated space
        with self.assertRaises(StorageError):
            self._xfs_migrate_wrapper(
                VM_HOSTNAME,
                offline=True,
                offline_transport='xfs',
                disk_size=1,
            )
        self.check_vm_present()

        with self.assertRaises(StorageError):
            self._xfs_migrate_wrapper(
                VM_HOSTNAME,
                offline=True,
                offline_transport='xfs',
                disk_size=-1,
            )
        self.check_vm_present()

        with self.assertRaises(StorageError):
            self._xfs_migrate_wrapper(
                VM_HOSTNAME,
                offline=True,
                offline_transport='xfs',
                disk_size=0,
            )
        self.check_vm_present()

    def test_reject_out_of_sync_serveradmin(self):
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()
        obj['disk_size_gib'] += 1
        obj.commit()

        with self.assertRaises(InconsistentAttributeError):
            vm_migrate(VM_HOSTNAME)

    def test_new_address(self):
        # We don't have a way to ask for new IP address from Serveradmin
        # and lock it for us. The method below will usually work fine.
        # When it starts failing, we must develop retry method.
        new_address = get_next_address(VM_NET, 2)

        change_address(VM_HOSTNAME, new_address, offline=True)

        obj = Query({'hostname': VM_HOSTNAME}, ['intern_ip']).get()
        self.assertEqual(obj['intern_ip'], new_address)
        with _get_vm(VM_HOSTNAME) as vm:
            vm.run(cmd('ip a | grep {}', new_address))
        self.check_vm_present()

    def test_new_address_fail(self):
        with self.assertRaises(IGVMError):
            # A wrong IP address won't be reachable
            change_address(VM_HOSTNAME, '1.2.3.4')

    def test_reject_online_with_puppet(self):
        with self.assertRaises(IGVMError):
            vm_migrate(VM_HOSTNAME, run_puppet=True)

    def test_rollback_netcat(self):
        # TODO: consider the usage of self.vm_obj instead of new Query
        obj = Query({'hostname': VM_HOSTNAME}, ['puppet_environment']).get()
        obj['puppet_environment'] = 'doesnotexist'
        obj.commit()

        with self.assertRaises(IGVMError):
            vm_migrate(
                VM_HOSTNAME,
                offline=True,
                run_puppet=True,
                offline_transport='netcat',
            )

        self.check_vm_present()

    def test_rollback_drbd(self):
        # TODO: consider the usage of self.vm_obj instead of new Query
        obj = Query({'hostname': VM_HOSTNAME}, ['puppet_environment']).get()
        obj['puppet_environment'] = 'doesnotexist'
        obj.commit()

        with self.assertRaises(IGVMError):
            vm_migrate(
                VM_HOSTNAME,
                offline=True,
                run_puppet=True,
                offline_transport='drbd',
            )

        self.check_vm_present()

    def test_rollback_xfs(self):
        # TODO: consider the usage of self.vm_obj instead of new Query
        obj = Query({'hostname': VM_HOSTNAME}, ['puppet_environment']).get()
        obj['puppet_environment'] = 'doesnotexist'
        obj.commit()

        with self.assertRaises(IGVMError):
            self._xfs_migrate_wrapper(
                VM_HOSTNAME,
                offline=True,
                run_puppet=True,
                offline_transport='xfs',
                disk_size=5,
            )

        self.check_vm_present()


class RenameTest(CommandTest):
    """Rename test

    Dedicated class for the vm_rename command to ensure that in case of a
    failure or abortion previous hosts with the "renamed" hostname are also
    cleaned up.
    """

    def setUp(self):
        super(RenameTest, self).setUp()

        # IGVMTest class will make sure puppet certificates for previous
        # hosts have been removed so we only need to take care of left overs
        # of renamed hosts.
        vm = self.vm.dataset_obj
        vm['hostname'] = RenameTest._get_renamed_hostname(vm['hostname'])
        clean_cert(vm)

    def tearDown(self):
        clean_cert(self.vm_obj)
        clean_all(self.route_network, self.datacenter_type, VM_HOSTNAME)

        # Same as in setUp we need to take care of the renamed hosts.
        vm = self.vm.dataset_obj

        # Depending on where it aborts it might still be renamed
        if 'vm-rename' not in vm['hostname']:
            vm['hostname'] = RenameTest._get_renamed_hostname(vm['hostname'])

        clean_cert(vm)
        clean_all(self.route_network, self.datacenter_type, vm['hostname'])

    def test_vm_rename(self):
        """Test vm_rename

        Make sure renaming a VM works as expected without breaking while
        runtime.
        """

        new_name = RenameTest._get_renamed_hostname(VM_HOSTNAME)
        vm_rename(VM_HOSTNAME, new_hostname=new_name, offline=True)
        self.check_vm_present(new_name)

    @staticmethod
    def _get_renamed_hostname(hostname: str) -> str:
        return '{}-{}'.format('vm-rename', hostname)
