import logging

from time import sleep

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

log = logging.getLogger(__name__)


class DRBD(object):
    def __init__(self, hv, vg_name, lv_name, vm_name, master_role, tx=None):
        self.hv = hv
        self.vg_name = vg_name
        self.lv_name = lv_name
        self.vm_name = vm_name
        self.master_role = master_role
        self.meta_disk = self.vm_name + '_meta'
        self.table_file = '/tmp/{}_{}_table'.format(self.vg_name, self.lv_name)
        self.tx = tx

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

    def start(self, peer, tx=None):
        self.prepare_metadata_device()
        if self.master_role:
            self.prepare_lv_override()
        self.build_config(peer)
        if self.master_role:
            self.replicate_to_slave()
        else:
            self.replicate_from_master()

    def prepare_metadata_device(self):
        """Create and zero metadata device for DRBD"""

        # 256MiB of metadata is fine up to 7TiB of synced storage.
        self.hv.run(
            'lvcreate -n {} -L256M {}'
            .format(self.meta_disk, self.vg_name)
        )
        if self.tx:
            self.tx.on_rollback(
                'Remove DRBD meta device', self.hv.run,
                'lvremove -fy {}/{}'.format(self.vg_name, self.meta_disk)
            )

        # Meta device must be zeroed, otherwise DRBD might complain
        self.hv.run(
            'dd if=/dev/zero of=/dev/{}/{} bs=1048576 count=256'
            .format(self.vg_name, self.meta_disk)
        )

    def prepare_lv_override(self):
        """Prepare logical volume to be overriden by DRBD revice"""

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
        if self.tx:
            self.tx.on_rollback(
                'Remove copy of original device', self.hv.run,
                'dmsetup remove {}_orig'.format(self.lv_name)
            )

    def build_config(self, peer):
        fd = StringIO(
            'resource {dev} {{\n'
            '    net {{\n'
            '        protocol A;\n'
            # max-buffers vs MB/s
            # 4k-150, 8k-233, 12k-330, 16K-397, 24k-561, 32k-700
            # 32k seems jumpy and might end up at as low aw 250MB/s
            '        max-buffers 24k;\n'
            # Buffer sizes don't seem to make any difference, at least within
            # one datacenter.
            '#        sndbuf-size 2048k;\n'
            '#        rcvbuf-size 2048k;\n'
            '    }}\n'
            # We don't care for flushes and barriers - we are replicating one
            # way only and if things fail, we will just replicate them again.
            '    disk {{\n'
            '         no-disk-flushes;\n'
            '         no-md-flushes;\n'
            '         no-disk-barrier;\n'
            # Try maximum speed immediately, no need for the slow-start
            # protocol
            '         c-max-rate 750M;\n'
            '         resync-rate 750M;\n'
            '    }}\n'
            '{src_host}\n'
            '{dst_host}\n'
            '}}\n'.format(
                dev=self.vm_name,
                src_host=self.get_host_config(),
                dst_host=peer.get_host_config(),
            )
        )
        self.hv.put('/etc/drbd.d/{}.res'.format(self.vm_name), fd, '0640')
        if self.tx:
            self.tx.on_rollback(
                'Remove configuration file', self.hv.run,
                'rm /etc/drbd.d/{}.res'.format(self.vm_name)
            )

    def get_host_config(self):
        return (
            '    on {host} {{\n'
            '        address   {addr}:{port};\n'
            '        device    /dev/drbd{dm_minor};\n'
            '        disk      /dev/{disk};\n'
            '        meta-disk /dev/{vg_name}/{meta_disk};\n'
            '    }}'.format(
                host=self.hv.dataset_obj['hostname'],
                addr=self.hv.dataset_obj['intern_ip'],
                port=self.get_device_port(),
                dm_minor=self.get_device_minor(),
                vm_name=self.vm_name,
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

    def replicate_to_slave(self):
        # Size must be retrieved before suspending device
        dev_size = self.get_device_size()

        # Suspend all traffic to disk from VM
        self.hv.run('dmsetup suspend /dev/{}/{}'.format(
            self.vg_name, self.lv_name))
        if self.tx:
            self.tx.on_rollback(
                'Resume original device', self.hv.run,
                'dmsetup resume /dev/{}/{}'.format(self.vg_name, self.lv_name)
            )
            # The "up" command might fail due to misconfiguration but the
            # device is started nevertheless. This is why "down" rollback is
            # always performed.
            self.tx.on_rollback(
                'Bring DRBD device down', self.hv.run,
                'drbdadm down {}'.format(self.vm_name)
            )

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
                dev_size / 512,
                self.get_device_minor(),
            )
        )
        if self.tx:
            # There should be no need for resume because it happens also
            # via another rollback defined above. Unfortunately it is
            # needed because DRBD won't allow to be shut down when its
            # device is still held open by somebody. Also see the comment about
            # active and inactive slots in stop() method.
            # WARNING: Potential race between writes to DRBD and underlying
            # device - potential data loss?
            # TODO: suspend VM for rollback
            self.tx.on_rollback(
                'Resume LV device', self.hv.run,
                'dmsetup resume /dev/{}/{}'
                .format(self.vg_name, self.lv_name)
            )
            self.tx.on_rollback(
                'Restore LV device table', self.hv.run,
                'dmsetup load /dev/{}/{} < {}'
                .format(self.vg_name, self.lv_name, self.table_file)
            )

        self.hv.run('dmsetup resume /dev/{}/{}'.format(
            self.vg_name, self.lv_name))

    def replicate_from_master(self):
        self.hv.run('drbdadm create-md {}'.format(self.vm_name))
        self.hv.run('drbdadm up {}'.format(self.vm_name))
        self.tx.on_rollback(
            'Bring DRBD device down', self.hv.run,
            'drbdadm down {}'.format(self.vm_name)
        )
        self.hv.run('drbdadm wait-connect {}'.format(self.vm_name))

    def wait_for_sync(self):
        # Display a "nice" progress bar
        show_progress = True
        while show_progress:
            lines = iter(self.hv.read_file('/proc/drbd').splitlines())
            for line in lines:
                if '{}: cs:'.format(self.get_device_minor()) in line:
                    if 'ds:UpToDate/UpToDate' in line:
                        show_progress = False
                    try:
                        lines.next()
                        line = lines.next()
                    except StopIteration:
                        show_progress = False
                    else:
                        log.info(line)
                    break
            else:
                # Exit the loop if status for current device can't be found
                show_progress = False
            sleep(1)

        # Just in case perform standard waiting
        self.hv.run('drbdsetup wait-sync {}'.format(self.get_device_minor()))

    def stop(self):
        if self.master_role:
            self.hv.run(
                'dmsetup load /dev/{}/{} < {}'
                .format(self.vg_name, self.lv_name, self.table_file)
            )
            self.hv.run('dmsetup resume /dev/{}/{}'.format(
                self.vg_name, self.lv_name))

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

        self.hv.run(
            'lvremove -fy {}/{}'
            .format(self.vg_name, self.meta_disk)
        )
        self.hv.run('rm /etc/drbd.d/{}.res'.format(self.vm_name))
