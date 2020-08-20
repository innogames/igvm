"""igvm - VM Model

Copyright (c) 2018 InnoGames GmbH
"""
import json
import logging
import os
import time
from base64 import b64decode
from hashlib import sha1, sha256
from io import BytesIO
from pathlib import Path
from re import compile as re_compile
from typing import Optional, List, Union
from uuid import uuid4

import boto3
import tqdm
from botocore.exceptions import ClientError, CapacityNotAvailableError
from fabric.api import cd, get, hide, put, run, settings
from fabric.contrib.files import upload_template
from fabric.exceptions import NetworkError
from json.decoder import JSONDecodeError
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from igvm.exceptions import ConfigError, RemoteCommandError, VMError
from igvm.host import Host
from igvm.settings import (
    AWS_ECU_FACTOR,
    AWS_RETURN_CODES,
    AWS_INSTANCES_OVERVIEW_FILE,
    AWS_INSTANCES_OVERVIEW_FILE_etag,
    AWS_INSTANCES_OVERVIEW_URL,
)
from igvm.transaction import Transaction
from igvm.utils import parse_size, wait_until
from igvm.puppet import clean_cert

log = logging.getLogger(__name__)


class VM(Host):
    """VM interface."""
    servertype = 'vm'

    def __init__(self, dataset_obj, hypervisor=None):
        super(VM, self).__init__(dataset_obj)
        self.hypervisor = hypervisor

        # A flag to keep state of machine consistent between VM methods.
        # Operations on VM like run() or put() will use it to decide
        # upon method of accessing files correctly: mounted image on HV or
        # directly on running VM.
        self.mounted = False

    def vm_host(self):
        """ Return correct ssh host for mounted and unmounted vm """

        if self.mounted:
            return self.hypervisor.fabric_settings()
        else:
            return self.fabric_settings()

    def vm_path(self, path=''):
        """ Append correct prefix to reach VM's / directory """

        if self.mounted:
            return '{}/{}'.format(
                self.hypervisor.vm_mount_path(self),
                path,
            )
        else:
            return '/{}'.format(path)

    def run(self, command, silent=False, with_sudo=True):
        """ Same as Fabric's run() but works on mounted or running vm

            When running in a mounted VM image, run everything in chroot
            and in separate shell inside chroot. Normally Fabric runs shell
            around commands.
        """
        with self.vm_host():
            if self.mounted:
                return self.hypervisor.run(
                    'chroot {} /bin/sh -c \'{}\''.format(
                        self.vm_path(''), command,
                    ),
                    shell=False, shell_escape=True,
                    silent=silent,
                    with_sudo=with_sudo,
                )
            else:
                return super(VM, self).run(command, silent=silent)

    def read_file(self, path):
        """Read a file from a running VM or a mounted image on HV."""
        with self.vm_host():
            return super(VM, self).read_file(self.vm_path(path))

    def upload_template(self, filename, destination, context=None):
        """" Same as Fabric's template() but works on mounted or running vm """
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        with self.vm_host():
            return upload_template(
                filename,
                self.vm_path(destination),
                context,
                backup=False,
                use_jinja=True,
                template_dir=template_dir,
                use_sudo=True,
            )

    def get(self, remote_path, local_path):
        """" Same as Fabric's get() but works on mounted or running vm """
        with self.vm_host():
            return get(self.vm_path(remote_path), local_path, temp_dir='/tmp')

    def put(self, remote_path, local_path, mode='0644'):
        """ Same as Fabric's put() but works on mounted or running vm

            Setting permissions on files and using sudo via Fabric's put()
            seems broken, at least for mounted VM. This is why we run
            extra commands here.
        """
        with self.vm_host():
            tempfile = '/tmp/' + str(uuid4())
            put(local_path, self.vm_path(tempfile))
            self.run('mv {0} {1} ; chmod {2} {1}'.format(
                tempfile, remote_path, mode
            ))

    def set_state(self, new_state, transaction=None):
        """Changes state of VM for LB and Nagios downtimes"""
        self.previous_state = self.dataset_obj['state']
        if self.previous_state == 'retired':
            # Don't set a state closer to online if VM is retired
            return
        if new_state == self.previous_state:
            return
        log.debug('Setting VM to state {}'.format(new_state))
        self.dataset_obj['state'] = new_state
        self.dataset_obj.commit()
        if transaction:
            transaction.on_rollback('reset_state', self.reset_state)

    def reset_state(self):
        """Change state of VM to the original one"""
        # Transaction is not necessary here, because reverting it
        # would set the value to the original one anyway.
        if hasattr(self, 'previous_state'):
            self.set_state(self.previous_state)

    def set_num_cpu(self, num_cpu):
        """Changes the number of CPUs."""
        self.hypervisor.vm_set_num_cpu(self, num_cpu)

    def set_memory(self, memory):
        """Resizes the host memory."""
        self.hypervisor.vm_set_memory(self, memory)

    def set_hostname(self, new_hostname, transaction=None):
        """Changes the hostname of a VM"""
        self.previous_hostname = self.dataset_obj['hostname']

        self.dataset_obj['hostname'] = new_hostname
        self.check_serveradmin_config()

        self.dataset_obj.commit()

        if transaction:
            transaction.on_rollback('revert_hostname', self.revert_hostname)

    def revert_hostname(self):
        """Revert the VM name to the previously defined name"""
        if hasattr(self, 'previous_hostname'):
            self.set_hostname(self.previous_hostname)

    def check_serveradmin_config(self):
        """Validate relevant Serveradmin attributes"""

        mul_numa_nodes = 128 * self.hypervisor.num_numa_nodes()
        validations = [
            (
                'hostname',
                re_compile(r'\A[a-z][a-z0-9\.\-]+\Z').match,
                'invalid hostname',
            ),
            ('memory', lambda v: v > 0, 'memory must be > 0'),
            # https://medium.com/@juergen_thomann/memory-hotplug-with-qemu-kvm-and-libvirt-558f1c635972#.sytig6o9h
            (
                'memory',
                lambda v: v % mul_numa_nodes == 0,
                'memory must be multiple of {}MiB'.format(mul_numa_nodes),
            ),
            ('num_cpu', lambda v: v > 0, 'num_cpu must be > 0'),
            ('os', lambda v: True, 'os must be set'),
            (
                'disk_size_gib',
                lambda v: v > 0,
                'disk_size_gib must be > 0',
            ),
            ('puppet_ca', lambda v: True, 'puppet_ca must be set'),
            ('puppet_master', lambda v: True, 'puppet_master must be set'),
        ]

        for attr, check, err in validations:
            value = self.dataset_obj[attr]
            if not value:
                raise ConfigError('"{}" attribute is not set'.format(attr))
            if not check(value):
                raise ConfigError(err)

    def start(self, force_stop_failed=True, transaction=None):
        self.hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

        host_up = wait_until(
            str(self.dataset_obj['intern_ip']),
            waitmsg='Waiting for SSH to respond',
        )
        if not host_up and force_stop_failed:
            # If there is a network or booting error VM must be destroyed
            # if starting has failed.
            self.hypervisor.stop_vm_force(self)
        if not host_up:
            raise VMError('The server is not reachable with SSH')

        if transaction:
            transaction.on_rollback('stop VM', self.shutdown)

    def aws_start(self):
        """AWS start

        Start a VM in AWS.
        """

        ec2 = boto3.client('ec2')

        try:
            ec2.start_instances(
                InstanceIds=[self.dataset_obj['aws_instance_id']],
                DryRun=True
            )
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        try:
            response = ec2.start_instances(
                InstanceIds=[self.dataset_obj['aws_instance_id']],
                DryRun=False
            )
            current_state = (
                response['StartingInstances'][0]['CurrentState']['Code']
            )
            log.info(response)
            if current_state and current_state == AWS_RETURN_CODES['running']:
                log.info('{} is already running.'.format(
                    self.dataset_obj['hostname']))
                return
        except ClientError as e:
            raise VMError(e)

        host_up = wait_until(
            str(self.dataset_obj['intern_ip']),
            waitmsg='Waiting for SSH to respond',
        )

        if not host_up:
            raise VMError('The server is not reachable with SSH')

    def shutdown(self, check_vm_up_on_transaction=True, transaction=None):
        self.hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            self.hypervisor.stop_vm_force(self)

        if transaction:
            transaction.on_rollback(
                'start VM', self.start,
                force_stop_failed=check_vm_up_on_transaction,
            )

    def aws_shutdown(self, timeout: int = 120) -> None:
        """AWS shutdown

        Shutdown a VM in AWS.

        :param: timeout: Timeout value for VM shutdown
        """
        ec2 = boto3.client('ec2')

        try:
            ec2.stop_instances(
                InstanceIds=[self.dataset_obj['aws_instance_id']],
                DryRun=True
            )
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise

        try:
            response = ec2.stop_instances(
                InstanceIds=[self.dataset_obj['aws_instance_id']],
                DryRun=False
            )
            current_state = response['StoppingInstances'][0][
                'CurrentState']['Code']
            log.info(response)
            if current_state and current_state == AWS_RETURN_CODES['stopped']:
                log.info('{} is already stopped.'.format(
                    self.dataset_obj['hostname']))
        except ClientError as e:
            raise VMError(e)

        for retry in range(timeout):
            if AWS_RETURN_CODES[
                'stopped'] == self.aws_describe_instance_status(
                    self.dataset_obj['aws_instance_id']
            ):
                log.info(
                    '"{}" is stopped.'.format(self.dataset_obj['hostname'])
                )
                break

            log.info(
                'Waiting for VM "{}" to shutdown'
                .format(self.dataset_obj['hostname'])
            )
            time.sleep(1)

    def aws_describe_instance_status(self, instance_id: str) -> int:
        """AWS describe instance status

        Get the actual VM status in AWS, e.g. running, stopped or terminated.

        :param: instance_id: Instance ID to get the status for

        :return: return code of instance state as int
        """

        ec2 = boto3.client('ec2')
        response = ec2.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-code',
                    'Values': [
                        str(AWS_RETURN_CODES['pending']),
                        str(AWS_RETURN_CODES['running']),
                        str(AWS_RETURN_CODES['shutting-down']),
                        str(AWS_RETURN_CODES['terminated']),
                        str(AWS_RETURN_CODES['stopping']),
                        str(AWS_RETURN_CODES['stopped']),
                    ]
                },
            ],
            InstanceIds=[instance_id],
            DryRun=False)

        return int(response['Reservations'][0]['Instances'][0][
                   'State']['Code'])

    def aws_delete(self):
        """AWS delete

        Delete a VM in AWS.
        """

        ec2 = boto3.client('ec2')

        try:
            response = ec2.terminate_instances(
                InstanceIds=[self.dataset_obj['aws_instance_id']])
            log.info(response)
        except ClientError as e:
            raise VMError(e)

    def is_running(self):
        return self.hypervisor.vm_running(self)

    def wait_for_running(self, running=True, timeout=60):
        """
        Waits for the VM to enter the given running state.
        Returns False on timeout, True otherwise.
        """
        action = 'boot' if running else 'shutdown'
        for i in range(timeout, 1, -1):
            print(
                'Waiting for VM "{}" to {}... {} s'.format(
                    self.fqdn, action, i))
            if self.hypervisor.vm_running(self) == running:
                return True
            time.sleep(1)
        return False

    def meminfo(self):
        """Returns a dictionary of /proc/meminfo entries."""
        contents = self.read_file('/proc/meminfo')
        result = {}
        for line in contents.splitlines():
            # XXX: What are we really expecting in here?
            try:
                key, value = map(str.strip, line.decode().split(':'))
            except IndexError:
                continue
            result[key] = value
        return result

    def memory_free(self):
        meminfo = self.meminfo()

        if 'MemAvailable' in meminfo:
            kib_free = parse_size(meminfo['MemAvailable'], 'K')
        # MemAvailable might not be present on old systems
        elif 'MemFree' in meminfo:
            kib_free = parse_size(meminfo['MemFree'], 'K')
        else:
            raise VMError('/proc/meminfo contains no parsable entries')

        return round(float(kib_free) / 1024, 2)

    def disk_free(self):
        """Returns free disk space in GiB"""
        output = self.run(
            "df -k / | tail -n+2 | awk '{ print $4 }'",
            silent=True,
        ).strip()
        if not output.isdigit():
            raise RemoteCommandError('Non-numeric output in disk_free')
        return round(float(output) / 1024 ** 2, 2)

    def info(self):
        result = {
            'hypervisor': self.hypervisor.fqdn,
            'intern_ip': self.dataset_obj['intern_ip'],
            'num_cpu': self.dataset_obj['num_cpu'],
            'memory': self.dataset_obj['memory'],
            'disk_size_gib': self.dataset_obj['disk_size_gib'],
        }

        if self.hypervisor.vm_defined(self) and self.is_running():
            result.update(self.hypervisor.vm_sync_from_hypervisor(self))
            result.update({
                'status': 'running',
                'memory_free': self.memory_free(),
                'disk_free_gib': self.disk_free(),
                'load': self.read_file('/proc/loadavg').split()[:3],
            })
            result.update(self.hypervisor.vm_info(self))
        elif self.hypervisor.vm_defined(self):
            result['status'] = 'stopped'
        else:
            result['status'] = 'new'
        return result

    def build(self, run_puppet=True, debug_puppet=False, postboot=None, cleanup_cert=False):
        """Builds a VM."""
        hypervisor = self.hypervisor
        self.check_serveradmin_config()

        image = self.dataset_obj['os'] + '-base.tar.gz'

        # Can VM run on given hypervisor?
        self.hypervisor.check_vm(self, offline=True)

        if not run_puppet or self.dataset_obj['puppet_disabled']:
            log.warn(
                'Puppet is disabled on the VM.  It will not receive network '
                'configuration.  Expect things to go south.'
            )

        with Transaction() as transaction:
            # Clean up the certificate if the build fails for any reason
            transaction.on_rollback('Clean cert', clean_cert, self.dataset_obj)

            # Perform operations on the hypervisor
            self.hypervisor.create_vm_storage(self, transaction)
            mount_path = self.hypervisor.format_vm_storage(self, transaction)

            self.hypervisor.download_and_extract_image(image, mount_path)

            self.prepare_vm()

            if run_puppet:
                self.run_puppet(clear_cert=cleanup_cert, debug=debug_puppet)

            if postboot is not None:
                self.copy_postboot_script(postboot)

            self.hypervisor.umount_vm_storage(self)
            hypervisor.define_vm(self, transaction)

            # We are updating the information on the Serveradmin, before
            # starting the VM, because the VM would still be on the hypervisor
            # even if it fails to start.
            self.dataset_obj.commit()

            self.start()

        # Perform operations on Virtual Machine
        if postboot is not None:
            self.run('/buildvm-postboot')
            self.run('rm /buildvm-postboot')

        log.info('"{}" is successfully built.'.format(self.fqdn))

    def aws_build(self,
                  run_puppet: bool = True,
                  debug_puppet: bool = False,
                  postboot: Optional[str] = None,
                  timeout_vm_setup: int = 300,
                  timeout_cloud_init: int = 600) -> None:
        """AWS build

        Build a VM in AWS.

        :param: run_puppet: Run puppet (incl. cert clean) after VM creation
        :param: debug_puppet: Run puppet in debug mode
        :param: postboot: cloudinit configuration put as userdata
        :param: timeout_vm_setup: Timeout value for the VM creation
        :param: timeout_cloud_init: Timeout value for the cloudinit
                                    provisioning

        :raises: VMError: Generic exception for VM errors of all kinds
        """

        ec2 = boto3.client('ec2')

        vm_types_overview = self.aws_get_instances_overview()
        if vm_types_overview:
            vm_types = self.aws_get_fitting_vm_types(vm_types_overview)
        else:
            vm_types = [self.dataset_obj['aws_instance_type']]

        for vm_type in vm_types:
            try:
                response = ec2.run_instances(
                    ImageId=self.dataset_obj['aws_image_id'],
                    InstanceType=vm_type,
                    KeyName=self.dataset_obj['aws_key_name'],
                    SecurityGroupIds=list(
                        self.dataset_obj['aws_security_group_ids']),
                    SubnetId=self.dataset_obj['aws_subnet_id'],
                    Placement={
                        'AvailabilityZone': self.dataset_obj['aws_placement']
                    },
                    PrivateIpAddress=str(self.dataset_obj['intern_ip']),
                    UserData='' if postboot is None else postboot,
                    TagSpecifications=[
                        {
                            'ResourceType': 'instance',
                            'Tags': [
                                {
                                    'Key': 'Name',
                                    'Value': self.dataset_obj['hostname'],
                                },
                            ]
                        },
                    ],
                    DryRun=False,
                    MinCount=1,
                    MaxCount=1)
                log.info(response)
                break
            except ClientError as e:
                raise VMError(e)
            except CapacityNotAvailableError as e:
                continue

        if run_puppet:
            self.run_puppet(clear_cert=True, debug=debug_puppet)

        self.dataset_obj['aws_instance_id'] = response['Instances'][0][
            'InstanceId']

        log.info('waiting for {} to be started'.format(
            self.dataset_obj['hostname']))
        vm_setup = tqdm.tqdm(
            total=timeout_vm_setup, desc='vm_setup', position=0)
        cloud_init = tqdm.tqdm(
            total=timeout_cloud_init, desc='cloud_init', position=1)

        # Wait for AWS to declare the VM running
        while (
            timeout_vm_setup and
            AWS_RETURN_CODES['running'] != self.aws_describe_instance_status(
                self.dataset_obj['aws_instance_id']
            )
        ):
            vm_setup.update(1)
            timeout_vm_setup -= 1
            time.sleep(1)
        vm_setup.update(timeout_vm_setup)
        # TODO: Handle overrun timeout

        # Try to provision the VM with cloudinit
        for retry in range(timeout_cloud_init):
            cloud_init.update(1)

            # Only try to connect every 10s to avoid paramiko exceptions
            if retry % 10 != 0:
                time.sleep(1)
                continue

            with settings(
                hide('aborts'),
                host_string=self.dataset_obj['hostname'],
                warn_only=True,
                abort_on_prompts=True,
            ):
                try:
                    if run(
                        'find /var/lib/cloud/instance/boot-finished',
                        quiet=True
                    ).succeeded:
                        cloud_init.update(timeout_cloud_init - retry - 1)
                        break
                except (SystemExit, NetworkError):
                    time.sleep(1)
        # TODO: Handle overrun timeout

        self.create_ssh_keys()

        log.info('"{}" is successfully built in AWS.'.format(self.fqdn))

    def rename(self, new_hostname):
        """Rename the VM"""
        with Transaction() as transaction:
            self.set_hostname(new_hostname, transaction=transaction)
            self.check_serveradmin_config()

            self.shutdown(transaction=transaction)

            self.hypervisor.redefine_vm(self, new_fqdn=new_hostname)
            log.warning(
                'Domain redefinition cannot be rolled back properly. Your '
                'domain is still defined with the new name.'
            )

            self.hypervisor.mount_vm_storage(self, transaction=transaction)

            self.run_puppet()

            self.hypervisor.umount_vm_storage(self)

            self.start(transaction=transaction)

    def prepare_vm(self):
        """Prepare the rootfs for a VM

        VM storage must be mounted on the hypervisor.
        """
        fd = BytesIO()
        fd.write(self.fqdn.encode())
        self.put('/etc/hostname', fd)
        self.put('/etc/mailname', fd)

        self.upload_template('etc/fstab', 'etc/fstab', {
            'blk_dev': self.hypervisor.vm_block_device_name(),
            'type': 'xfs',
            'mount_options': 'defaults'
        })
        self.upload_template('etc/hosts', '/etc/hosts')
        self.upload_template('etc/inittab', '/etc/inittab')

        # Copy resolv.conf from Hypervisor
        fd = BytesIO()
        with self.hypervisor.fabric_settings(
            cd(self.hypervisor.vm_mount_path(self))
        ):
            get('/etc/resolv.conf', fd)
        self.put('/etc/resolv.conf', fd)

        self.create_ssh_keys()

    def create_ssh_keys(self):
        # If we wouldn't do remove those, ssh-keygen would ask us confirm
        # overwrite.
        self.run('rm -f /etc/ssh/ssh_host_*_key*')

        self.dataset_obj['sshfp'] = set()
        key_types = [(1, 'rsa'), (3, 'ecdsa')]
        if self.dataset_obj['os'] != 'wheezy':
            key_types.append((4, 'ed25519'))
        fp_types = [(1, sha1), (2, sha256)]

        # This will also create the public key files.
        for key_id, key_type in key_types:
            self.run(
                'ssh-keygen -q -t {0} -N "" '
                '-f /etc/ssh/ssh_host_{0}_key'.format(key_type))

            fd = BytesIO()
            self.get('/etc/ssh/ssh_host_{0}_key.pub'.format(key_type), fd)
            pub_key = b64decode(fd.getvalue().split(None, 2)[1])
            for fp_id, fp_type in fp_types:
                self.dataset_obj['sshfp'].add('{} {} {}'.format(
                    key_id, fp_id, fp_type(pub_key).hexdigest()
                ))

    def run_puppet(self, clear_cert=False, debug=False):
        """Runs Puppet in chroot on the hypervisor."""

        if clear_cert:
            clean_cert(self.dataset_obj)

        if self.dataset_obj['datacenter_type'] == 'kvm.dct':
            self.block_autostart()

            puppet_command = (
                '( /opt/puppetlabs/puppet/bin/puppet agent '
                '--detailed-exitcodes '
                '--fqdn={} --server={} --ca_server={} '
                '--no-report --waitforcert=60 --onetime --no-daemonize '
                '--skip_tags=chroot_unsafe --verbose{} ) ;'
                '[ $? -eq 2 ]'.format(
                    self.fqdn,
                    self.dataset_obj['puppet_master'],
                    self.dataset_obj['puppet_ca'],
                    ' --debug' if debug else '',
                )
            )

            try:
                self.run(puppet_command)
            except RemoteCommandError as e:
                raise VMError('Initial puppetrun failed') from e

            self.unblock_autostart()


    def block_autostart(self):
        fd = BytesIO()
        fd.write(b'#!/bin/sh\nexit 101\n')
        self.put('/usr/sbin/policy-rc.d', fd, '0755')

    def unblock_autostart(self):
        self.run('rm /usr/sbin/policy-rc.d')

    def copy_postboot_script(self, script):
        self.put('/buildvm-postboot', script, '0755')

    def restore_address(self):
        self.dataset_obj['intern_ip'] = self.old_address
        self.dataset_obj.commit()
        self.route_network = self.old_network

    def change_address(self, new_address, new_network, transaction=None):
        # All queries to Serveradmin are kept in commands.py.
        # That's why this metod receives both new address and new network.
        self.old_address = self.dataset_obj['intern_ip']
        self.old_network = self.route_network
        self.dataset_obj['intern_ip'] = new_address
        self.dataset_obj.commit()
        self.route_network = new_network

        if transaction:
            transaction.on_rollback('restore IP address', self.restore_address)

    def aws_disk_set(self, size: int, timeout_disk_resize: int = 60) -> None:
        """AWS disk set

        Resize a disk in AWS.

        :param: size: New disk_size
        :param: timeout_disk_resize: Timeout to for disk resizing within VM

        :raises: VMError: Generic exception for VM errors of all kinds
        """

        ec2 = boto3.resource('ec2')

        response = ec2.Instance(self.dataset_obj['aws_instance_id'])
        for vol in response.volumes.all():
            volume_id = vol.id
            break

        ec2 = boto3.client('ec2')
        ec2.modify_volume(VolumeId=volume_id, Size=int(size))

        partition = self.run('findmnt -nro SOURCE /')
        disk = self.run('lsblk -nro PKNAME {}'.format(partition))
        new_disk_size = self.run('lsblk -bdnro size /dev/{}'.format(disk))
        new_disk_size_gib = int(new_disk_size) / 1024 / 1024 / 1024

        while timeout_disk_resize and size != new_disk_size_gib:
            timeout_disk_resize -= 1
            time.sleep(1)
            new_disk_size = self.run('lsblk -bdnro size /dev/{}'.format(disk))
            new_disk_size_gib = int(new_disk_size) / 1024 / 1024 / 1024

            if timeout_disk_resize == 0:
                raise VMError('Timeout for disk resize reached')

        with settings(
            host_string=self.dataset_obj['hostname'],
            warn_only=True,
        ):
            disk_resize = self.run('growpart /dev/{} 1'.format(disk))
            if disk_resize.succeeded:
                fs_resize = self.run('resize2fs {}'.format(partition))
                if fs_resize.succeeded:
                    log.info(
                        'successfully resized disk of {} to {}GB'.format(
                            self.dataset_obj['hostname'], size)
                    )
                    return

            raise VMError('disk resize for {} failed'.format(
                self.dataset_obj['hostname'])
            )

    def aws_sync(self) -> dict:
        """AWS sync

        Sync values like memory, disk_size_gib and num_cpu for AWS VMs.

        :return: Values to sync as a dict of tuples
        """

        pricing = boto3.client('pricing', region_name='us-east-1')
        response = pricing.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {
                    'Type': 'TERM_MATCH',
                    'Field': 'instanceType',
                    'Value': self.dataset_obj['aws_instance_type'],
                }
            ],
            MaxResults=1
        )

        price_list = json.loads(response['PriceList'][0])

        memory = int(
            price_list['product']['attributes']['memory'].split()[0]
        ) * 1024

        ec2 = boto3.resource('ec2')

        response = ec2.Instance(self.dataset_obj['aws_instance_id'])
        for vol in response.volumes.all():
            volume_size = vol.size
            break

        cpu_options = response.cpu_options

        sync_values = dict()
        sync_values['memory'] = memory
        sync_values['disk_size_gib'] = volume_size
        sync_values['num_cpu'] = (
            cpu_options['CoreCount'] * cpu_options['ThreadsPerCore']
        )

        return sync_values

    def performance_value(self) -> float:
        """VM performance value

        The performance value is the mathematical product of the load average
        of a VM and a artificial performance factor of a Hypervisor.

        load_99 is the 99 percentile of the load average (1 minute) of the past
        24 hours.

        cpu_perffactor is a artificial factor for the Hypervisor hardware to
        allow comparison between different CPU models. The better the CPU
        the higher the factor.

        See https://github.com/innogames/igcollect -> linux_cpu_perffactor.py

        :return: performance_value of VM as float
        """

        # Serveradmin can not handle floats right now so we safe them as
        # multiple ones of thousand and just divide them here again.

        vm_load_99 = self.dataset_obj['load_99'] / 1000  # Default 0
        vm_num_cpu = self.dataset_obj['num_cpu']
        hv_cpu_perffactor = self.hypervisor.dataset_obj[
                                'cpu_perffactor'] / 1000  # Default 1000

        # If load_99 is higher than the number of vCPUs we use the number of
        # the vCPUs to avoid returning fantastic numbers no hardware can ever
        # serve.
        estimated_load = (
            vm_load_99 if vm_load_99 < vm_num_cpu else vm_num_cpu
        ) * hv_cpu_perffactor

        return float(estimated_load)

    def aws_get_instances_overview(
            self, timeout: int = 5) -> Union[List, None]:
        """AWS Get Instances Overview

        Load or download the latest instances.json, which contains
        a complete overview about all instance_types, their configuration,
        performance and pricing.

        :param: timeout: Timeout value for the head/get request

        :return: VM types overview as list
                 or None, if the parsing/download failed
        """

        url = AWS_INSTANCES_OVERVIEW_URL
        file = Path(AWS_INSTANCES_OVERVIEW_FILE)
        etag_file = Path(AWS_INSTANCES_OVERVIEW_FILE_etag)

        try:
            head_req = Request(url, method='HEAD')
            resp = urlopen(head_req, timeout=timeout)
            if resp.status == 200:
                etag = dict(resp.info())['ETag']
            else:
                log.warning('Could not retrieve ETag from {}'.format(url))
                etag = None

            if file.exists() and etag_file.exists() and etag:
                with open(AWS_INSTANCES_OVERVIEW_FILE_etag) as f:
                    prev_etag = f.read()
                if etag == prev_etag:
                    with open(AWS_INSTANCES_OVERVIEW_FILE) as f:
                        return json.load(f)

            resp = urlopen(url, timeout=timeout)
            if etag:
                with open(AWS_INSTANCES_OVERVIEW_FILE_etag, 'w') as f:
                    f.write(etag)
            with open(AWS_INSTANCES_OVERVIEW_FILE, 'w') as f:
                content = resp.read().decode('utf-8')
                f.write(content)
                return json.loads(content)
        except (HTTPError, JSONDecodeError) as e:
            log.warning('Could not retrieve instances overview')
            log.warning(e)
            log.info('Proceeding with instance_type: {}'.format(
                self.dataset_obj['aws_instance_type'])
            )
            return None

    def aws_get_fitting_vm_types(self, overview: List) -> List:
        """AWS Get Fitting VM types

        Use the performance_value of the VM and multiply it with a static
        factor to get the targeted ECU of AWS +-25% (which is kind of a
        performance_value). Crawl the instances overview for fitting
        vm_types for this targeted ECU range.

        :param: overview: instances.json with a complete
                          instance_types overview

        :return: Fitting VM types as list
        """

        vm_performance_value = self.performance_value()
        region = self.dataset_obj['aws_placement'][:-1]

        ecu_target = {
            'min':
                (vm_performance_value * AWS_ECU_FACTOR) -
                (vm_performance_value * AWS_ECU_FACTOR * 0.25),
            'max':
                (vm_performance_value * AWS_ECU_FACTOR) +
                (vm_performance_value * AWS_ECU_FACTOR * 0.25)
        }

        vm_types = dict()

        for t in overview:
            if region not in t['pricing']:
                continue
            if t['memory'] < (self.dataset_obj['memory'] / 1024):
                continue
            if 'ECU' not in t or not isinstance(t['ECU'], (int, float)):
                continue
            if ecu_target['min'] > t['ECU'] or ecu_target['max'] < t['ECU']:
                continue

            vm_types[t['instance_type']] = t

        vm_types = sorted(vm_types.keys(), key=lambda x: (
            vm_types[x]['pricing'][region]['linux']['ondemand'],
            vm_types[x]['ECU'])
        )

        return vm_types
