# Integration tests for user-facing igvm commands
from ipaddress import IPv4Address
import logging
import tempfile
import unittest

import adminapi
from adminapi.dataset import query, filters

from fabric.api import env

from igvm.buildvm import buildvm
from igvm.commands import (
    mem_set,
    vm_delete,
    vm_restart,
    vm_start,
    vm_stop,
    vm_sync,
)
from igvm.exceptions import (
    IGVMError,
    InvalidStateError,
    InconsistentAttributeError,
)
from igvm.hypervisor import Hypervisor
from igvm.migratevm import migratevm
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils import cmd
from igvm.vm import VM

logging.basicConfig(level=logging.DEBUG)
env.update(COMMON_FABRIC_SETTINGS)

# Configuration of staging environment
IP1 = '10.20.6.98'
IP2 = '10.20.6.253'
VM1 = 'igvm-integration.test'
HV1 = 'aw-hv-055'  # 48 cores
HV2 = 'aw-hv-057'  # 48 cores
HV3 = 'ah-hv-082'  # 16 cores


def _ensure_ip_unused(ip):
    servers = tuple(query(intern_ip=ip, hostname=filters.Not(VM1)))
    if servers:
        raise AssertionError('IP {} is already used by {}'.format(ip, servers))


def _check_environment():
    _ensure_ip_unused(IP1)
    _ensure_ip_unused(IP2)


def _reset_vm():
    vm = VM(VM1)
    vm.admintool.update({
        'xen_host': HV1,
        'intern_ip': IP1,
        'state': 'online',
        'disk_size_gib': 6,
        'memory': 2048,
    })
    vm.admintool.commit()
    return vm


def _clean_vm(hv, hostname):
    hv.run(
        'virsh destroy {vm}; '
        'virsh undefine {vm}; '
        'umount /dev/xen-data/{vm}; '
        'lvremove -f /dev/xen-data/{vm}'
        .format(vm=hostname),
        warn_only=True,
    )


class IGVMTest(unittest.TestCase):
    def _check_vm(self, hv, vm):
        hostname = vm.run('hostname').strip()
        self.assertEqual(hostname, vm.hostname)

        self.assertEqual(vm.admintool['xen_host'], hv.hostname)
        self.assertEqual(vm.admintool.is_dirty(), False)

        self.assertEqual(hv.vm_defined(vm), True)
        self.assertEqual(hv.vm_running(vm), True)

    def _check_absent(self, hv, vm):
        self.assertEqual(hv.vm_defined(vm), False)
        with self.assertRaises(IGVMError):
            hv.vm_disk_path(vm)
        hv.run('test ! -b /dev/xen-data/{}'.format(vm.hostname))


class BuildTest(IGVMTest):
    def setUp(self):
        adminapi.auth()
        _check_environment()
        self.hv = Hypervisor.get(HV1)
        self.vm = _reset_vm()
        _clean_vm(self.hv, self.vm.hostname)

    def tearDown(self):
        _clean_vm(self.hv, self.vm.hostname)

    def test_simple(self):
        buildvm(self.vm.hostname)

        self.assertEqual(self.vm.hypervisor.hostname, HV1)
        self._check_vm(self.hv, self.vm)

    def test_local_image(self):
        buildvm(self.vm.hostname, localimage='/root/jessie-localimage.tar.gz')

        self.assertEqual(self.vm.hypervisor.hostname, HV1)
        self._check_vm(self.hv, self.vm)

        output = self.vm.run('md5sum /root/local_image_canary')
        self.assertIn('df60e346faccb1afa04b50eea3c1a87c', output)

    def test_postboot(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write('echo hello > /root/postboot_result')
            f.flush()

            buildvm(self.vm.hostname, postboot=f.name)
            self.assertEqual(self.vm.hypervisor.hostname, HV1)
            self._check_vm(self.hv, self.vm)

            output = self.vm.run('cat /root/postboot_result')
            self.assertIn('hello', output)

    def test_delete(self):
        buildvm(self.vm.hostname)

        # Fails while VM is powered on
        with self.assertRaises(IGVMError):
            vm_delete(self.vm.hostname)

        self.vm.shutdown()
        vm_delete(self.vm.hostname)

        self._check_absent(self.hv, self.vm)


class CommandTest(IGVMTest):
    def setUp(self):
        adminapi.auth()
        _check_environment()
        self.hv = Hypervisor.get(HV1)
        self.vm = _reset_vm()
        _clean_vm(self.hv, self.vm.hostname)

    def tearDown(self):
        _clean_vm(self.hv, self.vm.hostname)

    def test_start_stop(self):
        buildvm(self.vm.hostname)

        # Doesn't fail, but should print a message
        vm_start(self.vm.hostname)
        self._check_vm(self.hv, self.vm)

        vm_restart(self.vm.hostname)
        self._check_vm(self.hv, self.vm)

        vm_stop(self.vm.hostname)
        self.assertEqual(self.vm.is_running(), False)

        vm_start(self.vm.hostname)
        self.assertEqual(self.vm.is_running(), True)

        vm_stop(self.vm.hostname, force=True)
        self.assertEqual(self.vm.is_running(), False)
        vm_start(self.vm.hostname)

        vm_restart(self.vm.hostname, force=True)
        self._check_vm(self.hv, self.vm)

    def test_mem_set(self):
        buildvm(self.vm.hostname)

        def _get_mem_hv():
            return self.hv.vm_sync_from_hypervisor(self.vm)['memory']

        def _get_mem_vm():
            return int(float(self.vm.run(
                "cat /proc/meminfo | grep MemTotal | awk '{ print $2 }'"
            ).strip()) / 1024)

        # Online
        self.assertEqual(_get_mem_hv(), 2048)
        vm_mem = _get_mem_vm()
        mem_set(self.vm.hostname, '+1G')
        self.assertEqual(_get_mem_hv(), 3072)
        self.assertEqual(_get_mem_vm() - vm_mem, 1024)

        with self.assertRaises(Warning):
            mem_set(self.vm.hostname, '3G')

        with self.assertRaises(InvalidStateError):
            mem_set(self.vm.hostname, '2G')

        with self.assertRaises(IGVMError):
            mem_set(self.vm.hostname, '200G')

        # Not dividable
        with self.assertRaises(IGVMError):
            mem_set(self.vm.hostname, '4097M')

        self.assertEqual(_get_mem_hv(), 3072)
        vm_mem = _get_mem_vm()
        self.vm.shutdown()

        with self.assertRaises(IGVMError):
            mem_set(self.vm.hostname, '200G')

        mem_set(self.vm.hostname, '1024M')
        self.assertEqual(_get_mem_hv(), 1024)

        mem_set(self.vm.hostname, '2G')
        self.assertEqual(_get_mem_hv(), 2048)
        self.vm.start()
        self.assertEqual(_get_mem_vm() - vm_mem, -1024)

    def test_sync(self):
        buildvm(self.vm.hostname)

        expected_disk_size = self.vm.admintool['disk_size_gib']
        self.vm.admintool['disk_size_gib'] += 10

        expected_memory = self.vm.admintool['memory']
        self.vm.admintool['memory'] += 1024

        self.vm.admintool.commit()
        self.vm.reload()
        self.assertEqual(self.vm.admintool['memory'], expected_memory + 1024)

        vm_sync(self.vm.hostname)
        self.vm.reload()

        self.assertEqual(self.vm.admintool['memory'], expected_memory)
        self.assertEqual(
            self.vm.admintool['disk_size_gib'],
            expected_disk_size,
        )

        # Shouldn't do anything, but also shouldn't fail
        vm_sync(self.vm.hostname)
        self.vm.reload()


class MigrationTest(IGVMTest):
    @classmethod
    def setUpClass(cls):
        adminapi.auth()
        _check_environment()

        cls.hv1 = Hypervisor.get(HV1)
        cls.hv2 = Hypervisor.get(HV2)
        cls.vm = _reset_vm()

        _clean_vm(cls.hv1, cls.vm.hostname)
        _clean_vm(cls.hv2, cls.vm.hostname)
        buildvm(cls.vm.hostname)

    @classmethod
    def tearDownClass(cls):
        _clean_vm(cls.hv1, cls.vm.hostname)
        _clean_vm(cls.hv2, cls.vm.hostname)

    def setUp(self):
        # Make sure we have a clean initial state
        self._check_vm(self.hv1, self.vm)

    def tearDown(self):
        # Make sure we leave with a good state
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_online_migration(self):
        migratevm(self.vm.hostname, HV2)
        self.vm.reload()
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)

        # And back again
        migratevm(self.vm.hostname, HV1)
        self.vm.reload()
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_offline_migration(self):
        migratevm(self.vm.hostname, HV2, offline=True)
        self.vm.reload()
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)

        # And back again
        migratevm(self.vm.hostname, HV1, offline=True)
        self.vm.reload()
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_reject_out_of_sync_serveradmin(self):
        self.vm.admintool['disk_size_gib'] += 1
        self.vm.admintool.commit()

        with self.assertRaises(InconsistentAttributeError):
            migratevm(self.vm.hostname, HV2)

    def test_reject_online_with_new_ip(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.hostname, HV2, newip=IP2)

    def test_reject_new_ip_without_puppet(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.hostname, HV2, offline=True, newip=IP2)

    @unittest.skip("depends on automatic regeneration of ig.local domain")
    def test_new_ip(self):
        migratevm(
            self.vm.hostname,
            HV2,
            offline=True,
            newip=IP2,
            runpuppet=True,
        )
        self.vm.reload()

        self.assertEqual(self.vm.admintool['intern_ip'], IPv4Address(IP2))
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)
        self.vm.run(cmd('ip a | grep {}', IP2))

        # And back again
        migratevm(
            self.vm.hostname,
            HV1,
            offline=True,
            newip=IP1,
            runpuppet=True,
        )
        self.vm.reload()

        self.assertEqual(self.vm.admintool['intern_ip'], IPv4Address(IP1))
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)
        self.vm.run(cmd('ip a | grep {}', IP1))

    def test_reject_online_with_puppet(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.hostname, HV2, runpuppet=True)
