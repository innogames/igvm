import logging

from fabric.api import execute, run, settings
from fabric.colors import yellow
from time import sleep

from igvm.exceptions import ConfigError, IGVMError
from igvm.hypervisor import Hypervisor
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.config import (
        get_server,
        init_vm_config,
        import_vm_config_from_admintool,
        check_dsthv_vm,
        check_dsthv_memory,
        check_dsthv_cpu,
        check_vm_config,
    )
from igvm.utils.resources import get_ssh_keytypes, get_hw_model
from igvm.utils.storage import get_vm_block_dev
from igvm.utils.image import download_image, extract_image
from igvm.utils.preparevm import (
        prepare_vm,
        copy_postboot_script,
        run_puppet,
    )
from igvm.utils.portping import wait_until
from igvm.utils.virtutils import (
        get_virtconn,
        close_virtconns,
    )
from igvm.vm import VM


log = logging.getLogger(__name__)


def buildvm(*args, **kwargs):
    with settings(**COMMON_FABRIC_SETTINGS):
        return _buildvm(*args, **kwargs)


def _buildvm(vm_hostname, localimage=None, nopuppet=False, postboot=None):
    config = {'vm_hostname': vm_hostname}
    if localimage is not None:
        config['localimage'] = localimage
    config['runpuppet'] = not nopuppet
    if postboot is not None:
        config['postboot_script'] = postboot
    config['vm'] = get_server(vm_hostname, 'vm')
    config['dsthv_hostname'] = config['vm']['xen_host']
    config['dsthv'] = get_server(config['dsthv_hostname'])

    hv = Hypervisor.get(config['dsthv'])
    config['dsthv_object'] = hv

    vm = VM(config['vm'], hv)
    config['vm_object'] = vm

    # Populate initial networking attributes, such as segment.
    vm._set_ip(vm.admintool['intern_ip'])

    # Can VM run on given hypervisor?
    vm.hypervisor.check_vm(vm)

    if not config['vm']['puppet_classes']:
        if nopuppet or config['vm']['puppet_disabled']:
            log.warn(yellow(
                'VM has no puppet_classes and will not receive network '
                'configuration.\n'
                'You have chosen to disable Puppet. Expect things to go south.'
            ))
        else:
            raise ConfigError(
                'VM has no puppet_classes and will not get any network '
                'configuration.'
            )

    init_vm_config(config)
    import_vm_config_from_admintool(config)

    check_vm_config(config)

    # Perform operations on Hypervisor
    execute(setup_dsthv, config, hosts=[config['dsthv_hostname']])

    # Perform operations on Virtual Machine
    execute(setup_vm, config, hosts=[config['vm_hostname']])

    close_virtconns()
    sleep(1)  # For Paramiko's race condition.

    # Return true. This code should throw exceptions on all troubles anyway.
    return True


def setup_dsthv(config):
    if config['dsthv']['hypervisor'] == 'kvm':
        config['dsthv_conn'] = get_virtconn(config['dsthv']['hostname'], 'kvm')

    check_dsthv_vm(config)
    check_dsthv_cpu(config)
    check_dsthv_memory(config)

    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])
    config['dsthv_hw_model'] = get_hw_model(config['dsthv'])

    # Config completely generated -> start doing stuff.
    vm = config['vm_object']
    config['device'] = vm.hypervisor.create_vm_storage(vm)
    mount_path = vm.hypervisor.format_vm_storage(vm)

    if 'localimage' not in config:
        download_image(config['image'])
    else:
        config['image'] = config['localimage']

    extract_image(config['image'], mount_path, config['dsthv']['os'])

    prepare_vm(
        mount_path,
        server=config['vm'],
        mailname=config['mailname'],
        dns_servers=config['dns_servers'],
        network_config=vm.network_config,
        swap_size=config['swap_size'],
        blk_dev=config['vm_block_dev'],
        ssh_keytypes=get_ssh_keytypes(config['os']),
    )

    if config['runpuppet']:
        run_puppet(
            config['dsthv_object'],
            config['vm_object'],
            clear_cert=True,
        )

    if 'postboot_script' in config:
        copy_postboot_script(mount_path, config['postboot_script'])

    vm.hypervisor.umount_vm_storage(vm)
    vm.create(config)
    vm.start()

    host_up = wait_until(
        str(config['vm']['intern_ip']),
        waitmsg='Waiting for guest to boot',
    )

    if not host_up:
        raise IGVMError('Guest did not boot.')


def setup_vm(config):
    if 'postboot_script' in config:
        run('/buildvm-postboot')
        run('rm -f /buildvm-postboot')
