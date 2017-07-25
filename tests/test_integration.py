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
    disk_set,
    host_info,
    mem_set,
    vcpu_set,
    vm_delete,
    vm_rebuild,
    vm_redefine,
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
from igvm.utils.units import parse_size
from igvm.vm import VM

logging.basicConfig(level=logging.WARNING)
env.update(COMMON_FABRIC_SETTINGS)

# Configuration of staging environment
IP1 = IPv4Address('10.20.10.42')    # aw21.igvm
IP2 = IPv4Address('10.20.10.43')    # aw21.igvm
VM1 = 'igvm-integration.test'
HV1 = 'aw-hv-053'  # KVM
HV2 = 'aw-hv-082'  # KVM


def _ensure_ip_unused(ip):
    servers = tuple(query(intern_ip=ip, hostname=filters.Not(VM1)))
    if servers:
        raise AssertionError('IP {} is already used by {}'.format(ip, servers))


def _check_environment():
    _ensure_ip_unused(IP1)
    _ensure_ip_unused(IP2)


def _reset_vm():
    vm = VM(VM1)
    vm.server_obj['xen_host'] = HV1
    vm.server_obj['intern_ip'] = IP1
    vm.server_obj['state'] = 'online'
    vm.server_obj['disk_size_gib'] = 6
    vm.server_obj['memory'] = 2048
    vm.server_obj['num_cpu'] = 2
    vm.server_obj['ssh_users'] = {'root:control-test'}
    vm.server_obj['os'] = 'wheezy'
    if 'puppet_environment' in vm.server_obj:
        del vm.server_obj['puppet_environment']
    vm.server_obj.commit()
    # Might have changed!
    vm.hypervisor = Hypervisor(vm.server_obj['xen_host'])
    return vm


def _clean_vm(hv, fqdn):
    hv.run(
        'virsh destroy {vm}; '
        'virsh undefine {vm}'
        .format(vm=fqdn),
        warn_only=True,
    )
    hv.run(
        'umount /dev/xen-data/{vm}; '
        'lvremove -f /dev/xen-data/{vm}'
        .format(vm=fqdn),
        warn_only=True,
    )


class IGVMTest(unittest.TestCase):
    def _check_vm(self, hv, vm):
        fqdn = vm.run('hostname -f').strip()
        self.assertEqual(fqdn, vm.fqdn)

        self.assertEqual(vm.server_obj.is_dirty(), False)

        self.assertEqual(hv.vm_defined(vm), True)
        self.assertEqual(hv.vm_running(vm), True)

    def _check_absent(self, hv, vm):
        self.assertEqual(hv.vm_defined(vm), False)
        hv.run('test ! -b /dev/xen-data/{}'.format(vm.fqdn))


class BuildTest(object):
    def setUp(self):
        adminapi.auth()
        _check_environment()

    def tearDown(self):
        _clean_vm(self.hv, self.vm.fqdn)

    def test_simple(self):
        buildvm(self.vm.server_obj['hostname'])

        self.assertEqual(self.vm.hypervisor.fqdn, self.hv.fqdn)
        self._check_vm(self.hv, self.vm)

    def test_simple_stretch(self):
        self.vm.server_obj.update({
            'os': 'stretch',
        })
        self.vm.server_obj.commit()
        buildvm(self.vm.server_obj['hostname'])

        self.assertEqual(self.vm.hypervisor.fqdn, self.hv.fqdn)
        self._check_vm(self.hv, self.vm)

    def test_local_image(self):
        buildvm(self.vm.server_obj['hostname'],
                localimage='/root/jessie-localimage.tar.gz')

        self.assertEqual(self.vm.hypervisor.fqdn, self.hv.fqdn)
        self._check_vm(self.hv, self.vm)

        # The file is to be created by `echo 42 > /root/local_image_canary`
        output = self.vm.run('md5sum /root/local_image_canary')
        self.assertIn('50a2fabfdd276f573ff97ace8b11c5f4', output)

    def test_postboot(self):
        with tempfile.NamedTemporaryFile() as f:
            f.write('echo hello > /root/postboot_result')
            f.flush()

            buildvm(self.vm.server_obj['hostname'], postboot=f.name)
            self.assertEqual(self.vm.hypervisor.fqdn, self.hv.fqdn)
            self._check_vm(self.hv, self.vm)

            output = self.vm.run('cat /root/postboot_result')
            self.assertIn('hello', output)

    def test_delete(self):
        buildvm(self.vm.server_obj['hostname'])

        # Fails while VM is powered on
        with self.assertRaises(IGVMError):
            vm_delete(self.vm.server_obj['hostname'])

        self.vm.shutdown()
        vm_delete(self.vm.server_obj['hostname'])

        self._check_absent(self.hv, self.vm)

    def test_rollback(self):
        self.vm.server_obj['puppet_environment'] = 'doesnotexist'
        self.vm.server_obj.commit()

        with self.assertRaises(IGVMError):
            buildvm(self.vm.server_obj['hostname'])

        # Have we cleaned up?
        self._check_absent(self.hv, self.vm)

    def test_image_corruption(self):
        image = '{}-base.tar.gz'.format(self.vm.server_obj['os'])
        self.hv.run(cmd('test -f {}', image))

        self.hv.run(
            cmd('dd if=/dev/urandom of={} bs=1M count=10 seek=5', image)
        )

        buildvm(self.vm.server_obj['hostname'])

    def test_image_missing(self):
        image = '{}-base.tar.gz'.format(self.vm.server_obj['os'])
        self.hv.run(cmd('rm -f {}', image))

        buildvm(self.vm.server_obj['hostname'])

    def test_rebuild(self):
        # Not yet built.
        with self.assertRaises(IGVMError):
            vm_rebuild(self.vm.server_obj['hostname'])

        self.vm.build()

        self.vm.run('touch /root/initial_canary')
        self.vm.run('test -f /root/initial_canary')

        # Fails while online
        with self.assertRaises(IGVMError):
            vm_rebuild(self.vm.server_obj['hostname'])

        self.vm.shutdown()

        vm_rebuild(self.vm.server_obj['hostname'])
        self.vm.reload()
        self._check_vm(self.hv, self.vm)

        # Old contents are gone.
        with self.assertRaises(IGVMError):
            self.vm.run('test -f /root/initial_canary')


class KVMBuildTest(IGVMTest, BuildTest):
    def setUp(self):
        BuildTest.setUp(self)
        self.hv = Hypervisor(HV1)
        self.vm = _reset_vm()
        _clean_vm(self.hv, self.vm.fqdn)


class CommandTest(object):
    def setUp(self):
        adminapi.auth()
        _check_environment()

    def tearDown(self):
        _clean_vm(self.hv, self.vm.fqdn)

    def test_start_stop(self):
        buildvm(self.vm.server_obj['hostname'])

        # Doesn't fail, but should print a message
        vm_start(self.vm.server_obj['hostname'])
        self._check_vm(self.hv, self.vm)

        vm_restart(self.vm.server_obj['hostname'])
        self._check_vm(self.hv, self.vm)

        vm_stop(self.vm.server_obj['hostname'])
        self.assertEqual(self.vm.is_running(), False)

        vm_start(self.vm.server_obj['hostname'])
        self.assertEqual(self.vm.is_running(), True)

        vm_stop(self.vm.server_obj['hostname'], force=True)
        self.assertEqual(self.vm.is_running(), False)
        vm_start(self.vm.server_obj['hostname'])

        vm_restart(self.vm.server_obj['hostname'], force=True)
        self._check_vm(self.hv, self.vm)

    def test_disk_set(self):
        buildvm(self.vm.server_obj['hostname'])

        def _get_hv():
            return self.hv.vm_sync_from_hypervisor(self.vm)['disk_size_gib']

        def _get_vm():
            return parse_size(self.vm.run(
                "df -h / | tail -n+2 | awk '{ print $2 }'"
            ).strip(), 'G')

        self.assertEqual(_get_hv(), 6)
        self.assertEqual(_get_vm(), 6)

        disk_set(self.vm.server_obj['hostname'], '+1')
        self.vm.reload()

        self.assertEqual(self.vm.server_obj['disk_size_gib'], 7)
        self.assertEqual(_get_hv(), 7)
        self.assertEqual(_get_vm(), 7)

        disk_set(self.vm.server_obj['hostname'], '8GB')
        self.vm.reload()

        self.assertEqual(self.vm.server_obj['disk_size_gib'], 8)
        self.assertEqual(_get_hv(), 8)
        self.assertEqual(_get_vm(), 8)

        with self.assertRaises(Warning):
            disk_set(self.vm.server_obj['hostname'], '8GB')

        with self.assertRaises(NotImplementedError):
            disk_set(self.vm.server_obj['hostname'], '7GB')

        with self.assertRaises(NotImplementedError):
            disk_set(self.vm.server_obj['hostname'], '-1')

    def test_mem_set(self):
        buildvm(self.vm.server_obj['hostname'])

        def _get_mem_hv():
            # Xen does not provide values when VM is powered off
            data = self.hv.vm_sync_from_hypervisor(self.vm)
            return data.get('memory', self.vm.server_obj['memory'])

        def _get_mem_vm():
            return int(float(self.vm.run(
                "cat /proc/meminfo | grep MemTotal | awk '{ print $2 }'"
            ).strip()) / 1024)

        # Online
        self.assertEqual(_get_mem_hv(), 2048)
        vm_mem = _get_mem_vm()
        mem_set(self.vm.server_obj['hostname'], '+1G')
        self.vm.reload()
        self.assertEqual(_get_mem_hv(), 3072)
        self.assertEqual(_get_mem_vm() - vm_mem, 1024)

        with self.assertRaises(Warning):
            mem_set(self.vm.server_obj['hostname'], '3G')

        if self.hv.server_obj['hypervisor'] == 'kvm':
            with self.assertRaises(InvalidStateError):
                mem_set(self.vm.server_obj['hostname'], '2G')

        with self.assertRaises(IGVMError):
            mem_set(self.vm.server_obj['hostname'], '200G')

        if self.hv.server_obj['hypervisor'] == 'kvm':
            # Not dividable
            with self.assertRaises(IGVMError):
                mem_set(self.vm.server_obj['hostname'], '4097M')

        self.vm.reload()
        self.assertEqual(_get_mem_hv(), 3072)
        vm_mem = _get_mem_vm()
        self.vm.shutdown()

        with self.assertRaises(IGVMError):
            mem_set(self.vm.server_obj['hostname'], '200G')

        mem_set(self.vm.server_obj['hostname'], '1024M')
        self.vm.reload()
        self.assertEqual(_get_mem_hv(), 1024)

        mem_set(self.vm.server_obj['hostname'], '2G')
        self.vm.reload()
        self.assertEqual(_get_mem_hv(), 2048)
        self.vm.start()
        self.assertEqual(_get_mem_vm() - vm_mem, -1024)

    def test_vcpu_set(self):
        buildvm(self.vm.server_obj['hostname'])

        def _get_hv():
            # Xen does not provide values when VM is powered off
            data = self.hv.vm_sync_from_hypervisor(self.vm)
            return data.get('num_cpu', self.vm.server_obj['num_cpu'])

        def _get_vm():
            return int(self.vm.run(
                "cat /proc/cpuinfo | grep vendor_id | wc -l"
            ).strip())

        # Online
        self.assertEqual(_get_hv(), 2)
        self.assertEqual(_get_vm(), 2)
        self.assertEqual(self.vm.server_obj['num_cpu'], 2)
        vcpu_set(self.vm.server_obj['hostname'], 3)
        self.assertEqual(_get_hv(), 3)
        self.assertEqual(_get_vm(), 3)

        self.vm.reload()
        self.assertEqual(self.vm.server_obj['num_cpu'], 3)

        with self.assertRaises(Warning):
            vcpu_set(self.vm.server_obj['hostname'], 3)

        # Online reduce not implemented yet on KVM
        if self.hv.server_obj['hypervisor'] == 'kvm':
            with self.assertRaises(IGVMError):
                vcpu_set(self.vm.server_obj['hostname'], 2)

        # Offline
        vcpu_set(self.vm.server_obj['hostname'], 2, offline=True)
        self.assertEqual(_get_hv(), 2)
        self.assertEqual(_get_vm(), 2)

        # Impossible amount
        with self.assertRaises(IGVMError):
            vcpu_set(self.vm.server_obj['hostname'], 9001)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm.server_obj['hostname'], 0, offline=True)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm.server_obj['hostname'], -5)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm.server_obj['hostname'], -5, offline=True)

    def test_sync(self):
        buildvm(self.vm.server_obj['hostname'])

        expected_disk_size = self.vm.server_obj['disk_size_gib']
        self.vm.server_obj['disk_size_gib'] += 10

        expected_memory = self.vm.server_obj['memory']
        self.vm.server_obj['memory'] += 1024

        self.vm.server_obj.commit()
        self.vm.reload()
        self.assertEqual(self.vm.server_obj['memory'], expected_memory + 1024)

        vm_sync(self.vm.server_obj['hostname'])
        self.vm.reload()

        self.assertEqual(self.vm.server_obj['memory'], expected_memory)
        self.assertEqual(
            self.vm.server_obj['disk_size_gib'],
            expected_disk_size,
        )

        # Shouldn't do anything, but also shouldn't fail
        vm_sync(self.vm.server_obj['hostname'])
        self.vm.reload()

    def test_info(self):
        # Not built
        host_info(self.vm.server_obj['hostname'])

        buildvm(self.vm.server_obj['hostname'])
        host_info(self.vm.server_obj['hostname'])

        self.vm.shutdown()
        host_info(self.vm.server_obj['hostname'])

    def test_redefine(self):
        # Not built
        with self.assertRaises(IGVMError):
            vm_redefine(self.vm.server_obj['hostname'])

        buildvm(self.vm.server_obj['hostname'])

        # Running
        vm_redefine(self.vm.server_obj['hostname'])
        self._check_vm(self.hv, self.vm)

        # Stopped
        vm_stop(self.vm.server_obj['hostname'])
        vm_redefine(self.vm.server_obj['hostname'])

        self.assertEqual(self.vm.is_running(), False)


class KVMCommandTest(IGVMTest, CommandTest):
    def setUp(self):
        CommandTest.setUp(self)
        self.hv = Hypervisor(HV1)
        self.vm = _reset_vm()
        _clean_vm(self.hv, self.vm.fqdn)


class MigrationTest(IGVMTest):
    @classmethod
    def setUpClass(cls):
        adminapi.auth()
        _check_environment()

        cls.hv1 = Hypervisor(HV1)
        cls.hv2 = Hypervisor(HV2)
        cls.vm = _reset_vm()

        _clean_vm(cls.hv1, cls.vm.fqdn)
        _clean_vm(cls.hv2, cls.vm.fqdn)
        buildvm(cls.vm.server_obj['hostname'])

    @classmethod
    def tearDownClass(cls):
        _clean_vm(cls.hv1, cls.vm.fqdn)
        _clean_vm(cls.hv2, cls.vm.fqdn)

    def setUp(self):
        # Make sure we have a clean initial state
        self._check_vm(self.hv1, self.vm)

    def tearDown(self):
        # Make sure we leave with a good state
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_online_migration(self):
        migratevm(self.vm.server_obj['hostname'], HV2)
        self.vm.reload()
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)

        # And back again
        migratevm(self.vm.server_obj['hostname'], HV1)
        self.vm.reload()
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_offline_migration(self):
        migratevm(self.vm.server_obj['hostname'], HV2, offline=True)
        self.vm.reload()
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)

        # And back again
        migratevm(self.vm.server_obj['hostname'], HV1, offline=True)
        self.vm.reload()
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)

    def test_reject_out_of_sync_serveradmin(self):
        self.vm.server_obj['disk_size_gib'] += 1
        self.vm.server_obj.commit()

        with self.assertRaises(InconsistentAttributeError):
            migratevm(self.vm.server_obj['hostname'], HV2)

    def test_reject_online_with_new_ip(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.server_obj['hostname'], HV2, newip=IP2)

    def test_reject_new_ip_without_puppet(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.server_obj['hostname'], HV2, offline=True,
                      newip=IP2)

    @unittest.skip("depends on automatic regeneration of ig.local domain")
    def test_new_ip(self):
        migratevm(
            self.vm.server_obj['hostname'],
            HV2,
            offline=True,
            newip=IP2,
            runpuppet=True,
        )
        self.vm.reload()

        self.assertEqual(self.vm.server_obj['intern_ip'], IP2)
        self._check_vm(self.hv2, self.vm)
        self._check_absent(self.hv1, self.vm)
        self.vm.run(cmd('ip a | grep {}', IP2))

        # And back again
        migratevm(
            self.vm.server_obj['hostname'],
            HV1,
            offline=True,
            newip=IP1,
            runpuppet=True,
        )
        self.vm.reload()

        self.assertEqual(self.vm.server_obj['intern_ip'], IP1)
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)
        self.vm.run(cmd('ip a | grep {}', IP1))

    def test_reject_online_with_puppet(self):
        with self.assertRaises(IGVMError):
            migratevm(self.vm.server_obj['hostname'], HV2, runpuppet=True)

    def test_rollback(self):
        self.vm.server_obj['puppet_environment'] = 'doesnotexist'
        self.vm.server_obj.commit()

        with self.assertRaises(IGVMError):
            migratevm(self.vm.server_obj['hostname'], HV2, offline=True,
                      runpuppet=True)

        # Have we cleaned up?
        self.vm.reload()
        self._check_vm(self.hv1, self.vm)
        self._check_absent(self.hv2, self.vm)
