import logging

from fabric.colors import yellow

from igvm.commands import with_fabric_settings
from igvm.exceptions import ConfigError
from igvm.utils.image import download_image, extract_image
from igvm.utils.preparevm import (
    prepare_vm,
    copy_postboot_script,
    run_puppet,
)
from igvm.vm import VM


log = logging.getLogger(__name__)


@with_fabric_settings
def buildvm(vm_hostname, localimage=None, nopuppet=False, postboot=None):
    vm = VM(vm_hostname)
    hv = vm.hypervisor

    vm.check_serveradmin_config()

    if localimage is not None:
        image = localimage
    else:
        image = vm.admintool['os'] + '-base.tar.gz'

    # Populate initial networking attributes, such as segment.
    vm._set_ip(vm.admintool['intern_ip'])

    # Can VM run on given hypervisor?
    vm.hypervisor.check_vm(vm)

    if not vm.admintool['puppet_classes']:
        if nopuppet or vm.admintool['puppet_disabled']:
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

    # Perform operations on Hypervisor
    vm.hypervisor.create_vm_storage(vm)
    mount_path = vm.hypervisor.format_vm_storage(vm)

    with hv.fabric_settings():
        if not localimage:
            download_image(image)
        extract_image(image, mount_path, hv.admintool['os'])

    prepare_vm(hv, vm)

    if not nopuppet:
        run_puppet(hv, vm, clear_cert=True)

    if postboot is not None:
        copy_postboot_script(hv, vm, postboot)

    vm.hypervisor.umount_vm_storage(vm)
    hv.define_vm(vm)

    # We are updating the information on the Serveradmin, before starting
    # the VM, because the VM would still be on the hypervisor even if it
    # fails to start.
    vm.admintool.commit()

    vm.start()

    # Perform operations on Virtual Machine
    if postboot is not None:
        vm.run('/buildvm-postboot')
        vm.run('rm -f /buildvm-postboot')

    log.info('{} successfully built.'.format(vm_hostname))
