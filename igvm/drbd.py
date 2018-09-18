"""igvm - DRBD Transport Internals

Copyright (c) 2018 InnoGames GmbH
"""

from contextlib import contextmanager
from io import BytesIO
from logging import getLogger
from time import sleep

log = getLogger(__name__)


class DRBD(object):
    def __init__(self, hv, vm, master_role=False):
        self.hv = hv
        self.master_role = master_role

        lv = vm.hypervisor.get_volume_by_vm(vm).path()
        lv_name = lv.split('/')
        self.vg_name = lv_name[2]
        self.lv_name = lv_name[3] if self.master_role else vm.uid_name
        self.vm_name = vm.fqdn
        self.meta_disk = vm.fqdn + '_meta'
        self.table_file = '/tmp/{}_{}_table'.format(self.vg_name, self.lv_name)

        # Cached properties
        self.dev_minor = None
        self.mapper_name = None

    def get_device_minor(self):
        if self.dev_minor is None:
            dev_minor = self.hv.run(
                'stat -L -c "%T" /dev/{}/{}'
                .format(self.vg_name, self.lv_name),
                silent=True,
            )
            self.dev_minor = int(dev_minor, 16)
        return self.dev_minor

    def get_device_port(self):
        dev_minor = self.get_device_minor()
        return 8000 + dev_minor

    def get_device_size(self):
        return int(self.hv.run(
            'lvs --noheadings '
            '-o lv_size '
            '--units b --nosuffix {}/{}'
            .format(self.vg_name, self.lv_name)
        ).strip())

    @contextmanager
    def start(self, peer):
        """Start the replication

        This is a context manager that would start the replication and stop
        once we are done with it.  However we can only stop it properly after
        all the initialization steps are successfully completed.  Therefore,
        all of the initialization must handle cleaning up themselves.
        """
        with self.prepare_metadata_device(), self.build_config(peer):
            if self.master_role:
                self.replicate_to_slave()
            else:
                self.replicate_from_master()
        try:
            yield
        finally:
            self.stop()

    @contextmanager
    def prepare_metadata_device(self):
        """Create and zero metadata device for DRBD"""

        # 256MiB of metadata is fine up to 7TiB of synced storage.
        self.hv.run(
            'lvcreate -y -n {} -L256M {}'
            .format(self.meta_disk, self.vg_name)
        )
        try:
            # Meta device must be zeroed, otherwise DRBD might complain
            self.hv.run(
                'dd if=/dev/zero of=/dev/{}/{} bs=1048576 count=256'
                .format(self.vg_name, self.meta_disk)
            )
            if self.master_role:
                with self.prepare_lv_override():
                    yield
            else:
                yield
        except BaseException:
            self.hv.run(
                'lvremove -fy {}/{}'.format(self.vg_name, self.meta_disk)
            )
            raise

    @contextmanager
    def prepare_lv_override(self):
        """Prepare logical volume to be overridden by DRBD device"""

        # Dump mapper parameters of original LV
        self.hv.run(
            'dmsetup table /dev/{}/{} > {}'
            .format(self.vg_name, self.lv_name, self.table_file)
        )

        # Create new device with mapping to location of original LV
        self.hv.run(
            'dmsetup create {}_orig < {}'
            .format(self.lv_name, self.table_file)
        )
        try:
            yield
        except BaseException:
            self.hv.run('dmsetup remove {}_orig'.format(self.lv_name))
            raise

    @contextmanager
    def build_config(self, peer):
        fd = BytesIO()
        fd.write(
            'resource {dev} {{\n'
            '    net {{\n'
            '        protocol C;\n'
            # max-buffers vs MB/s
            # 4k-150, 8k-233, 12k-330, 16K-397, 24k-561, 32k-700
            # 32k seems jumpy and might end up at as low aw 250MB/s
            '        max-buffers 24k;\n'
            # Buffer sizes don't seem to make any difference, at least within
            # one datacenter.
            '#        sndbuf-size 2048k;\n'
            '#        rcvbuf-size 2048k;\n'
            '    }}\n'
            '    disk {{\n'
            # Try maximum speed immediately, no need for the slow-start
            '         c-max-rate 750M;\n'
            '         resync-rate 750M;\n'
            '    }}\n'
            '{src_host}\n'
            '{dst_host}\n'
            '}}\n'
            .format(
                dev=self.vm_name,
                src_host=self.get_host_config(),
                dst_host=peer.get_host_config(),
            ).encode()
        )
        self.hv.put('/etc/drbd.d/{}.res'.format(self.vm_name), fd, '0640')
        try:
            yield
        except BaseException:
            self.hv.run('rm /etc/drbd.d/{}.res'.format(self.vm_name))
            raise

    def get_host_config(self):
        return (
            '    on {host} {{\n'
            '        address   {addr}:{port};\n'
            '        device    /dev/drbd{dm_minor};\n'
            '        disk      /dev/{disk};\n'
            '        meta-disk /dev/{vg_name}/{meta_disk};\n'
            '    }}'
            .format(
                host=self.hv.dataset_obj['hostname'],
                addr=self.hv.dataset_obj['intern_ip'],
                port=self.get_device_port(),
                dm_minor=self.get_device_minor(),
                lv_name=self.lv_name,
                disk=(
                    'mapper/{}_orig'.format(self.lv_name)
                    if self.master_role
                    else '{}/{}'.format(self.vg_name, self.lv_name)
                ),
                vg_name=self.vg_name,
                meta_disk=self.meta_disk,
            )
        )

    def replicate_to_slave(self, transaction=None):
        # Size must be retrieved before suspending device
        dev_size = self.get_device_size()

        # Suspend all traffic to disk from VM
        self.hv.run(
            'dmsetup suspend /dev/{}/{}'.format(self.vg_name, self.lv_name)
        )
        try:
            # Start DRBD on device
            self.hv.run('drbdadm create-md {}'.format(self.vm_name))
            self.hv.run('drbdadm up {}'.format(self.vm_name))

            # Enforce primary operation and sync to secondary with
            # overwriting of data
            self.hv.run(
                'drbdadm -- --overwrite-data-of-peer primary {}'
                .format(self.vm_name)
            )

            # DRBD is finally up, now replace device which VM talks to on-fly.
            # In Device Mapper block is always 512 bytes.
            self.hv.run(
                'dmsetup load /dev/{}/{} --table "0 {} linear /dev/drbd{} 0"'
                .format(
                    self.vg_name, self.lv_name,
                    dev_size // 512,
                    self.get_device_minor(),
                )
            )
            try:
                self.hv.run(
                    'dmsetup resume /dev/{}/{}'
                    .format(self.vg_name, self.lv_name)
                )
            except BaseException:
                # There should be no need for resume because it happens also
                # via another rollback defined above.  Unfortunately, it is
                # needed because DRBD won't allow to be shut down when its
                # device is still held open by somebody.  Also see the comment
                # about active and inactive slots in stop() method.
                # WARNING: Potential race between writes to DRBD and underlying
                # device - potential data loss?
                # TODO: suspend VM for rollback
                self.hv.run(
                    'dmsetup load /dev/{}/{} < {}'
                    .format(self.vg_name, self.lv_name, self.table_file)
                )
                self.hv.run(
                    'dmsetup resume /dev/{}/{}'
                    .format(self.vg_name, self.lv_name)
                )
                raise
        except BaseException:
            # The "up" command might fail due to misconfiguration but the
            # device is started nevertheless. This is why "down" rollback is
            # always performed.
            self.hv.run('drbdadm down {}'.format(self.vm_name))
            self.hv.run(
                'dmsetup resume /dev/{}/{}'.format(self.vg_name, self.lv_name)
            )
            raise

    def replicate_from_master(self, transaction=None):
        self.hv.run('drbdadm create-md {}'.format(self.vm_name))
        self.hv.run('drbdadm up {}'.format(self.vm_name))
        try:
            self.hv.run('drbdadm wait-connect {}'.format(self.vm_name))
        except BaseException:
            self.hv.run('drbdadm down {}'.format(self.vm_name))
            raise

    def wait_for_sync(self):
        # Display a "nice" progress bar
        show_progress = True
        while show_progress:
            lines = iter(self.hv.read_file('/proc/drbd').splitlines())
            for line in lines:
                line = line.decode()
                if '{}: cs:'.format(self.get_device_minor()) in line:
                    if 'ds:UpToDate/UpToDate' in line:
                        show_progress = False
                    try:
                        next(lines)
                        line = next(lines).decode()
                    except StopIteration:
                        log.warning(
                            'Could not find progress bar, '
                            'migrating without it!'
                        )
                        show_progress = False
                    else:
                        log.info(line)
                    break
            else:
                # Exit the loop if status for current device can't be found
                show_progress = False
            sleep(1)

        # Progress bar does not determine if disks were synced.
        # Wait for sync to be reported by DRBD.
        self.hv.run('drbdsetup wait-sync {}'.format(self.get_device_minor()))

    def stop(self):
        if self.master_role:
            self.hv.run(
                'dmsetup load /dev/{}/{} < {}'
                .format(self.vg_name, self.lv_name, self.table_file)
            )
            self.hv.run('dmsetup resume /dev/{}/{}'.format(
                self.vg_name, self.lv_name
            ))

        # One would expect that DRBD must be shut down after table load and
        # before resume. Unfortunately that is impossible because table is
        # loaded to inactive slot and the old table with DRBD device is still
        # there holding it locked. Only after resuming the device its table
        # is fully updated. Do we risk data loss here? Probably yes. But since
        # we shut down source VM before DRBD is stopped and start the target VM
        # only after that, all is safe.
        self.hv.run('drbdadm down {}'.format(self.vm_name))

        if self.master_role:
            self.hv.run('dmsetup remove {}_orig'.format(self.lv_name))

        self.hv.run('lvremove -fy {}/{}'.format(self.vg_name, self.meta_disk))
        self.hv.run('rm /etc/drbd.d/{}.res'.format(self.vm_name))
