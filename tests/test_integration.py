"""igvm - Integration Tests

Copyright (c) 2020 InnoGames GmbH
"""
from __future__ import print_function

from logging import INFO, basicConfig
from os import environ
from tempfile import NamedTemporaryFile
from unittest import TestCase

from adminapi.dataset import Query, Any
from fabric.api import env
from fabric.network import disconnect_all

from igvm.commands import (
    _get_vm,
    change_address,
    disk_set,
    host_info,
    mem_set,
    vcpu_set,
    vm_build,
    vm_delete,
    vm_migrate,
    vm_restart,
    vm_start,
    vm_stop,
    vm_sync,
)
from igvm.exceptions import (
    IGVMError,
    InconsistentAttributeError,
    InvalidStateError,
    VMError,
)
from igvm.hypervisor import Hypervisor
from igvm.puppet import clean_cert
from igvm.settings import (
    COMMON_FABRIC_SETTINGS,
    HYPERVISOR_ATTRIBUTES,
    HYPERVISOR_CPU_THRESHOLDS,
    KVM_HWMODEL_TO_CPUMODEL,
    VG_NAME,
)
from igvm.utils import parse_size
from tests import VM_HOSTNAME, VM_NET
from tests.conftest import (
    clean_all,
    cmd,
    get_next_address,
)
from mock import patch

basicConfig(level=INFO)
env.update(COMMON_FABRIC_SETTINGS)
environ['IGVM_SSH_USER'] = 'igtesting'  # Enforce user for integration testing
env.user = 'igtesting'
environ['IGVM_MODE'] = 'testing'


def teardown_module():
    disconnect_all()  # Will hang on Jessie + Python3


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

        self.hvs = [Hypervisor(o) for o in Query({
            'environment': 'testing',
            'servertype': 'hypervisor',
            'state': 'online',
            'vlan_networks': self.route_network,
        }, HYPERVISOR_ATTRIBUTES)]

        assert len(self.hvs) >= 2, 'Not enough testing hypervisors found'

        # Cleanup all leftovers from previous tests or failures.
        clean_all(self.route_network, VM_HOSTNAME)

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
        self.vm_obj['os'] = 'stretch'
        self.vm_obj['project'] = 'test'
        self.vm_obj['puppet_environment'] = None
        self.vm_obj['puppet_ca'] = 'testing-puppetca.innogames.de'
        self.vm_obj['puppet_master'] = 'puppet-lb.test.ig.local'
        self.vm_obj['repositories'] = [
            'int:basestretch:stable',
            'int:innogames:stable',
        ]
        self.vm_obj['state'] = 'online'
        self.vm_obj['team'] = 'test'
        self.vm_obj.commit()

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
        clean_all(self.route_network, VM_HOSTNAME)

    def check_vm_present(self):
        # Operate on fresh object
        with _get_vm(VM_HOSTNAME) as vm:
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

    def check_vm_absent(self, hv_name=None):
        # Operate on fresh object
        with _get_vm(VM_HOSTNAME, allow_retired=True) as vm:
            if not hv_name:
                hv_name = vm.dataset_obj['hypervisor']

            for hv in self.hvs:
                if hv.dataset_obj['hostname'] == hv_name:
                    self.assertEqual(hv.vm_defined(vm), False)
                    hv.run(
                        'test ! -b /dev/{}/{}'.format(VG_NAME, self.uid_name)
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

    def test_build_stretch(self):
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

        with _get_vm(VM_HOSTNAME) as vm:
            vm.shutdown()
        vm_delete(VM_HOSTNAME, retire=True)

        self.check_vm_absent()

    def test_rollback(self):
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

        # Initial size same as built
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()
        size = obj['disk_size_gib']
        self.assertEqual(_get_disk_hv(), size)
        self.assertEqual(_get_disk_vm(), size)

        size = size + 1
        disk_set(VM_HOSTNAME, '+1')
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()

        self.assertEqual(obj['disk_size_gib'], size)
        self.assertEqual(_get_disk_hv(), size)
        self.assertEqual(_get_disk_vm(), size)

        size = 8
        disk_set(VM_HOSTNAME, '{}GB'.format(size))
        obj = Query({'hostname': VM_HOSTNAME}, ['disk_size_gib']).get()

        self.assertEqual(obj['disk_size_gib'], size)
        self.assertEqual(_get_disk_hv(), size)
        self.assertEqual(_get_disk_vm(), size)

        with self.assertRaises(Warning):
            disk_set(VM_HOSTNAME, '{}GB'.format(size))

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

        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, -5)

        with self.assertRaises(IGVMError):
            vcpu_set(VM_HOSTNAME, -5, offline=True)

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

    @patch('igvm.vm.VM.vm_performance_value', return_value=5.0)
    @patch('igvm.hypervisor.time', return_value=1234567890)
    def test_igvm_migration_log(self, mock_vm_performance_value, mock_time):
        for hv in self.hvs:
            hv.dataset_obj['igvm_migration_log'].clear()
            hv.dataset_obj.commit()

        src_hv = self.vm.hypervisor.dataset_obj['hostname']
        cpu_usage_vm_src = self.vm.hypervisor.hv_predict_vm_cpu_util(self.vm)
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
            cpu_usage_vm_dest = vm.hypervisor.hv_predict_vm_cpu_util(vm)
            self.assertEqual(
                list(dest_hv_obj['igvm_migration_log']),
                ['{} +{}'.format(timestamp, round(cpu_usage_vm_dest))]
            )


class MigrationTest(IGVMTest):
    def setUp(self):
        super(MigrationTest, self).setUp()
        vm_build(VM_HOSTNAME)
        with _get_vm(VM_HOSTNAME) as vm:
            self.old_hv_name = vm.hypervisor.dataset_obj['hostname']

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
