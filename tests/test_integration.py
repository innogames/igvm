"""igvm - Integration Tests

Copyright (c) 2018, InnoGames GmbH
"""

from __future__ import print_function

from logging import INFO, basicConfig
from os import environ
from pipes import quote
from tempfile import NamedTemporaryFile
from unittest import TestCase
from uuid import uuid4

from adminapi.dataset import Query
from fabric.api import env

from igvm.buildvm import buildvm
from igvm.commands import (
    disk_set,
    host_info,
    mem_set,
    vcpu_set,
    vm_delete,
    vm_rebuild,
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
from igvm.settings import (
    COMMON_FABRIC_SETTINGS,
    IMAGE_PATH,
)
from igvm.utils.units import parse_size
from igvm.vm import VM

basicConfig(level=INFO)
env.update(COMMON_FABRIC_SETTINGS)
env['user'] = 'igtesting'  # Enforce user for integration testing process
environ['IGVM_MODE'] = 'testing'

# Configuration of VMs used for tests
# Keep in mind that the whole hostname must fit in 64 characters.
VM_HOSTNAME = 'igvm-{}.test.ig.local'.format(uuid4())
VM_NET = 'igvm-net-aw.test.ig.local'


def setUpModule():
    # Automatically find suitable HVs for tests.
    # Terminate if this is impossible - we can't run tests without HVs.
    global HYPERVISORS
    vm_route_net = Query({'hostname': VM_NET}).get()['route_network']

    # We can access HVs as objects but that does not mean we can compare them
    # to any objects returned from igvm - those will be different objects,
    # created from scratch from Serveradmin data.
    HYPERVISORS = [Hypervisor(h['hostname']) for h in Query({
        'servertype': 'hypervisor',
        'environment': 'testing',
        'vlan_networks': vm_route_net,
        'state': 'online',
    })]

    if len(HYPERVISORS) < 2:
        raise Exception('Not enough testing hypervisors found')

    query = Query()
    vm_obj = query.new_object('vm')
    vm_obj['hostname'] = VM_HOSTNAME
    vm_obj['intern_ip'] = Query(
        {'hostname': VM_NET}, ['intern_ip']
    ).get_free_ip_addrs()
    vm_obj['project'] = 'test'
    vm_obj['team'] = 'test'

    query.commit()


def tearDownModule():
    query = Query({'hostname': VM_HOSTNAME}, ['hostname'])
    for obj in query:
        obj.delete()
    query.commit()


def cmd(cmd, *args, **kwargs):
    escaped_args = [quote(str(arg)) for arg in args]

    escaped_kwargs = {}
    for key, value in kwargs.iteritems():
        escaped_kwargs[key] = quote(str(value))

    return cmd.format(*escaped_args, **escaped_kwargs)


class IGVMTest(TestCase):
    def setUp(self):
        """Initialize VM object before every test

        Get object from Serveradmin and initialize it to safe defaults.
        Don't assign VM to any of HVs yet!
        """
        # igvm operates always on hostname of VM and queries it from
        # Serveradmin whenever it needs. Because of that we must never store
        # any igvm objects and query things anew each time.
        self.vm_obj = self.get_vm_obj()

        # Fill in defaults in Serveradmin
        self.vm_obj['state'] = 'online'
        self.vm_obj['disk_size_gib'] = 3
        self.vm_obj['memory'] = 2048
        self.vm_obj['num_cpu'] = 2
        self.vm_obj['os'] = 'jessie'
        self.vm_obj['environment'] = 'testing'
        self.vm_obj['no_monitoring'] = True
        self.vm_obj['xen_host'] = None
        self.vm_obj['repositories'] = [
            'int:basejessie:stable',
            'int:innogames:stable jessie',
        ]
        self.vm_obj['puppet_environment'] = None
        self.vm_obj.commit()

    def tearDown(self):
        """Clean up all HVs after every test"""
        for hv in HYPERVISORS:
            hv.run(
                'virsh destroy {vm}; '
                'virsh undefine {vm}'
                .format(vm=self.vm_obj['hostname']),
                warn_only=True,
            )
            hv.run(
                'umount /dev/xen-data/{vm}; '
                'lvremove -f /dev/xen-data/{vm}'
                .format(vm=self.vm_obj['hostname']),
                warn_only=True,
            )

    def get_vm_obj(self):
        return Query({'hostname': VM_HOSTNAME}).get()

    def check_vm_present(self):
        # Operate on fresh object
        vm = VM(self.get_vm_obj())

        for hv in HYPERVISORS:
            if hv.dataset_obj['hostname'] == vm.dataset_obj['xen_host']:
                # Is it on correct HV?
                self.assertEqual(hv.vm_defined(vm), True)
                self.assertEqual(hv.vm_running(vm), True)
            else:
                # Is it gone from other HVs after migration?
                self.assertEqual(hv.vm_defined(vm), False)
                hv.run('test ! -b /dev/xen-data/{}'.format(vm.fqdn))

        # Is VM itself alive and fine?
        fqdn = vm.run('hostname -f').strip()
        self.assertEqual(fqdn, vm.fqdn)
        self.assertEqual(vm.dataset_obj.is_dirty(), False)

    def check_vm_absent(self, hv_name=None):
        # Operate on fresh object
        vm = VM(self.get_vm_obj())

        if not hv_name:
            hv_name = vm.dataset_obj['xen_host']

        for hv in HYPERVISORS:
            if hv.dataset_obj['hostname'] == hv_name:
                self.assertEqual(hv.vm_defined(vm), False)
                hv.run('test ! -b /dev/xen-data/{}'.format(vm.fqdn))


class BuildTest(IGVMTest):
    """Test many possible VM building scenarios"""

    def setUp(self):
        super(BuildTest, self).setUp()
        # Normally build tests happen on the 1st HV
        self.vm_obj['xen_host'] = HYPERVISORS[0].dataset_obj['hostname']
        self.vm_obj.commit()
        self.vm = VM(self.get_vm_obj())

    def test_build(self):
        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()

    def test_build_auto_find_hypervisor(self):
        # HV is configured for all BuildTest class tests by default.
        # But this test requires it unconfigured.
        self.vm_obj['xen_host'] = None
        self.vm_obj.commit()
        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()

    def test_build_stretch(self):
        self.vm_obj.update({
            'os': 'stretch',
            'repositories': [
                'int:basestretch:stable',
                'int:innogames:stable stretch',
            ]
        })
        self.vm_obj.commit()
        buildvm(self.vm_obj['hostname'])

        self.check_vm_present()

    def test_local_image(self):
        # First prepare a local image for testing
        base_image = 'jessie-base.tar.gz'
        local_image = 'jessie-localimage.tar.gz'
        local_extract = '{}/localimage'.format(IMAGE_PATH)

        self.vm.hypervisor.run('rm -rf {} || true'.format(local_extract))
        self.vm.hypervisor.download_image(base_image)
        self.vm.hypervisor.run('mkdir {}/localimage'.format(IMAGE_PATH))
        self.vm.hypervisor.extract_image(base_image, local_extract)
        self.vm.hypervisor.run(
            'echo 42 > {}/root/local_image_canary'.format(local_extract)
        )
        self.vm.hypervisor.run(
            'tar --remove-files -zcf {}/{} -C {} .'
            .format(IMAGE_PATH, local_image, local_extract)
        )

        buildvm(self.vm_obj['hostname'], localimage=local_image)

        self.check_vm_present()

        output = self.vm.run('md5sum /root/local_image_canary')
        self.assertIn('50a2fabfdd276f573ff97ace8b11c5f4', output)

    def test_postboot(self):
        with NamedTemporaryFile() as fd:
            fd.write('echo hello > /root/postboot_result')
            fd.flush()

            buildvm(self.vm_obj['hostname'], postboot=fd.name)
            self.check_vm_present()

            output = self.vm.run('cat /root/postboot_result')
            self.assertIn('hello', output)

    def test_delete(self):
        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()

        # Fails while VM is powered on
        with self.assertRaises(IGVMError):
            vm_delete(self.vm_obj['hostname'])

        self.vm.shutdown()
        vm_delete(self.vm_obj['hostname'], retire=True)

        self.check_vm_absent()

    def test_rollback(self):
        self.vm_obj['puppet_environment'] = 'doesnotexist'
        self.vm_obj.commit()

        with self.assertRaises(IGVMError):
            buildvm(self.vm_obj['hostname'])

        self.check_vm_absent()

    def test_image_corruption(self):
        """Test re-downloading of broken image"""
        image = '{}/{}-base.tar.gz'.format(
            IMAGE_PATH, self.vm_obj['os']
        )
        self.vm.hypervisor.run(cmd('test -f {}', image))

        self.vm.hypervisor.run(
            cmd('dd if=/dev/urandom of={} bs=1M count=10 seek=5', image)
        )

        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()

    def test_image_missing(self):
        image = '{}/{}-base.tar.gz'.format(
            IMAGE_PATH, self.vm_obj['os']
        )
        self.vm.hypervisor.run(cmd('rm -f {}', image))

        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()

    def test_rebuild(self):
        # VM not built yet, this must fail
        with self.assertRaises(IGVMError):
            vm_rebuild(self.vm_obj['hostname'])

        # Now really build it
        self.vm.build()
        self.check_vm_present()

        self.vm.run('touch /root/initial_canary')
        self.vm.run('test -f /root/initial_canary')

        # Rebuild online VM, this must fail
        with self.assertRaises(IGVMError):
            vm_rebuild(self.vm_obj['hostname'])

        self.vm.shutdown()

        # Finally do a working rebuild
        vm_rebuild(self.vm_obj['hostname'])
        self.vm_obj = self.get_vm_obj()
        self.check_vm_present()

        # The VM was rebuild and thus test file must be gone
        with self.assertRaises(IGVMError):
            self.vm.run('test -f /root/initial_canary')


class CommandTest(IGVMTest):
    def setUp(self):
        super(CommandTest, self).setUp()
        # For every command test build a VM on the 1st HV
        self.vm_obj['xen_host'] = HYPERVISORS[0].dataset_obj['hostname']
        self.vm_obj.commit()
        buildvm(self.vm_obj['hostname'])
        self.check_vm_present()
        self.vm = VM(self.vm_obj)  # For contacting VM and HV over shell

    def test_start_stop(self):
        # Doesn't fail, but should print a message
        vm_start(self.vm_obj['hostname'])
        self.check_vm_present()

        vm_restart(self.vm_obj['hostname'])
        self.check_vm_present()

        vm_stop(self.vm_obj['hostname'])
        self.assertEqual(self.vm.is_running(), False)

        vm_start(self.vm_obj['hostname'])
        self.assertEqual(self.vm.is_running(), True)

        vm_stop(self.vm_obj['hostname'], force=True)
        self.assertEqual(self.vm.is_running(), False)
        vm_start(self.vm_obj['hostname'])

        vm_restart(self.vm_obj['hostname'], force=True)
        self.check_vm_present()

    def test_disk_set(self):
        def _get_hv():
            return (
                self.vm.hypervisor.vm_sync_from_hypervisor(self.vm)
                ['disk_size_gib']
            )

        def _get_vm():
            return parse_size(
                self.vm.run("df -h / | tail -n+2 | awk '{ print $2 }'")
                .strip(),
                'G'
            )

        # Initial size same as built
        size = self.vm_obj['disk_size_gib']
        self.assertEqual(_get_hv(), size)
        self.assertEqual(_get_vm(), size)

        size = size + 1
        disk_set(self.vm_obj['hostname'], '+1')
        self.vm_obj = self.get_vm_obj()

        self.assertEqual(self.vm_obj['disk_size_gib'], size)
        self.assertEqual(_get_hv(), size)
        self.assertEqual(_get_vm(), size)

        size = 8
        disk_set(self.vm_obj['hostname'], '{}GB'.format(size))
        self.vm_obj = self.get_vm_obj()

        self.assertEqual(self.vm_obj['disk_size_gib'], size)
        self.assertEqual(_get_hv(), size)
        self.assertEqual(_get_vm(), size)

        with self.assertRaises(Warning):
            disk_set(self.vm_obj['hostname'], '{}GB'.format(size))

        with self.assertRaises(NotImplementedError):
            disk_set(self.vm_obj['hostname'], '{}GB'.format(size - 1))

        with self.assertRaises(NotImplementedError):
            disk_set(self.vm_obj['hostname'], '-1')

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
        mem_set(self.vm_obj['hostname'], '+1G')
        self.assertEqual(_get_mem_hv(), 3072)
        self.assertEqual(_get_mem_vm() - vm_mem, 1024)

        with self.assertRaises(Warning):
            mem_set(self.vm_obj['hostname'], '3G')

        with self.assertRaises(InvalidStateError):
            mem_set(self.vm_obj['hostname'], '2G')

        with self.assertRaises(IGVMError):
            mem_set(self.vm_obj['hostname'], '200G')

        # Not dividable
        with self.assertRaises(IGVMError):
            mem_set(self.vm_obj['hostname'], '4097M')

        self.assertEqual(_get_mem_hv(), 3072)
        vm_mem = _get_mem_vm()
        self.vm.shutdown()

        with self.assertRaises(IGVMError):
            mem_set(self.vm_obj['hostname'], '200G')

        mem_set(self.vm_obj['hostname'], '1024M')
        self.assertEqual(_get_mem_hv(), 1024)

        mem_set(self.vm_obj['hostname'], '2G')
        self.assertEqual(_get_mem_hv(), 2048)
        self.vm.start()
        self.assertEqual(_get_mem_vm() - vm_mem, -1024)

    def test_vcpu_set(self):
        def _get_hv():
            data = self.vm.hypervisor.vm_sync_from_hypervisor(self.vm)
            return data['num_cpu']

        def _get_vm():
            return int(
                self.vm.run('cat /proc/cpuinfo | grep vendor_id | wc -l')
                .strip()
            )

        # Online
        self.assertEqual(_get_hv(), 2)
        self.assertEqual(_get_vm(), 2)
        self.assertEqual(self.vm_obj['num_cpu'], 2)
        vcpu_set(self.vm_obj['hostname'], 3)
        self.assertEqual(_get_hv(), 3)
        self.assertEqual(_get_vm(), 3)

        self.vm_obj = self.get_vm_obj()
        self.assertEqual(self.vm_obj['num_cpu'], 3)

        with self.assertRaises(Warning):
            vcpu_set(self.vm_obj['hostname'], 3)

        # Online reduce not implemented yet on KVM
        with self.assertRaises(IGVMError):
            vcpu_set(self.vm_obj['hostname'], 2)

        # Offline
        vcpu_set(self.vm_obj['hostname'], 2, offline=True)
        self.assertEqual(_get_hv(), 2)
        self.assertEqual(_get_vm(), 2)

        # Impossible amount
        with self.assertRaises(IGVMError):
            vcpu_set(self.vm_obj['hostname'], 9001)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm_obj['hostname'], 0, offline=True)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm_obj['hostname'], -5)

        with self.assertRaises(IGVMError):
            vcpu_set(self.vm_obj['hostname'], -5, offline=True)

    def test_sync(self):
        expected_disk_size = self.vm_obj['disk_size_gib']
        self.vm_obj['disk_size_gib'] += 10

        expected_memory = self.vm_obj['memory']
        self.vm_obj['memory'] += 1024

        self.vm_obj.commit()
        self.vm_obj = self.get_vm_obj()
        self.assertEqual(self.vm_obj['memory'], expected_memory + 1024)

        vm_sync(self.vm_obj['hostname'])
        self.vm_obj = self.get_vm_obj()

        self.assertEqual(self.vm_obj['memory'], expected_memory)
        self.assertEqual(self.vm_obj['disk_size_gib'], expected_disk_size)

        # Shouldn't do anything, but also shouldn't fail
        vm_sync(self.vm_obj['hostname'])
        self.vm_obj = self.get_vm_obj()

    def test_info(self):
        host_info(self.vm_obj['hostname'])
        self.vm.shutdown()
        host_info(self.vm_obj['hostname'])


class MigrationTest(IGVMTest):
    def setUp(self):
        super(MigrationTest, self).setUp()
        # Every migration gets a freshly built VM on the 1st HV
        self.vm_obj['xen_host'] = HYPERVISORS[0].dataset_obj['hostname']
        self.vm_obj.commit()
        buildvm(self.vm_obj['hostname'])
        # And is performed to the 2nd HV
        # Of course apart from migrations to automatically selected HVs
        self.new_hv_name = HYPERVISORS[1].dataset_obj['hostname']

    def test_online_migration(self):
        migratevm(self.vm_obj['hostname'], self.new_hv_name)
        self.check_vm_present()

    def test_online_migration_auto_find_hypervisor(self):
        # auto find means no target HV is specified
        migratevm(self.vm_obj['hostname'])
        self.check_vm_present()

    def test_offline_migration(self):
        migratevm(self.vm_obj['hostname'], self.new_hv_name, offline=True)
        self.check_vm_present()

    def test_reject_out_of_sync_serveradmin(self):
        self.vm_obj['disk_size_gib'] += 1
        self.vm_obj.commit()

        with self.assertRaises(InconsistentAttributeError):
            migratevm(self.vm_obj['hostname'], self.new_hv_name)

    def test_reject_online_with_new_ip(self):
        with self.assertRaises(IGVMError):
            # Fake IP address is fine, this is a failing test.
            migratevm(
                self.vm_obj['hostname'], self.new_hv_name, newip='1.2.3.4'
            )

    def test_reject_new_ip_without_puppet(self):
        with self.assertRaises(IGVMError):
            # Fake IP address is fine, this is a failing test.
            migratevm(
                self.vm_obj['hostname'],
                self.new_hv_name,
                offline=True,
                newip='1.2.3.4',
            )

    @unittest.skip("Broken until we have new Serveradmin API deployed")
    def test_new_ip(self):
        # We don't have a way to ask for new IP address from Serveradmin
        # and lock it for us. The method below will usually work fine.
        # When it starts failing, we must develop retry method.
        new_address = (
            Query({'hostname': VM_NET}, ['intern_ip']).get_free_ip_addrs()
        )

        migratevm(
            self.vm_obj['hostname'],
            self.new_hv_name,
            offline=True,
            newip=new_address,
            runpuppet=True,
        )
        self.vm_obj = self.get_vm_obj()

        self.assertEqual(self.vm_obj['intern_ip'], new_address)
        self.vm.run(cmd('ip a | grep {}', new_address))
        self.check_vm_present()

    def test_reject_online_with_puppet(self):
        with self.assertRaises(IGVMError):
            migratevm(
                self.vm_obj['hostname'], self.new_hv_name,
                runpuppet=True,
            )

    def test_rollback(self):
        self.vm_obj['puppet_environment'] = 'doesnotexist'
        self.vm_obj.commit()

        with self.assertRaises(IGVMError):
            migratevm(
                self.vm_obj['hostname'],
                self.new_hv_name,
                offline=True,
                runpuppet=True,
            )

        self.check_vm_present()
        self.check_vm_absent(self.new_hv_name)
