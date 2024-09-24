"""igvm -  Puppet interaction class

Copyright (c) 2020 InnoGames GmbH
"""
import random
from logging import getLogger
from time import sleep

from adminapi.dataset import Query, DatasetObject
from fabric.api import settings
from fabric.operations import sudo

from igvm.exceptions import ConfigError
from igvm.settings import COMMON_FABRIC_SETTINGS

logger = getLogger(__name__)


def get_puppet_ca(vm: DatasetObject) -> str:
    puppet_ca_type = Query(
        {'hostname': vm['puppet_ca']},
        ['servertype'],
    ).get()['servertype']

    if puppet_ca_type not in ['vm', 'public_domain', 'loadbalancer']:
        raise ConfigError(
            'Servertype {} not supported for puppet_ca'.format(
                puppet_ca_type,
            ),
        )

    if puppet_ca_type == 'vm':
        return vm['puppet_ca']

    if puppet_ca_type == 'public_domain':
        _filter = {'domain': vm['puppet_ca']}
    else:
        _filter = {'hostname': vm['puppet_ca']}

    ca_query = Query(_filter, [{'lb_nodes': ['hostname', 'state']}])
    ca_hosts = [
        lb_node['hostname']
        for res in ca_query
        for lb_node in res['lb_nodes']
        if lb_node['state'] in ['online', 'deploy_online']
    ]
    random.shuffle(ca_hosts)

    return ca_hosts[0]


def clean_cert(vm: DatasetObject, retries: int = 10) -> None:
    ca_host = get_puppet_ca(vm)
    vm_host = vm['hostname']
    logger.info(f'Cleaning puppet certificate for {vm_host} on {ca_host}..')

    # Detect Puppet executables and version
    puppet_exe = find_puppet_executable(ca_host)
    puppetserver_exe = find_puppetserver_executable(ca_host)
    puppet_version = run_cmd(ca_host, f'{puppet_exe} --version').stdout
    is_puppet_v5 = int(puppet_version.split('.')[0]) < 6

    # Every signing and revoking will have the CA regenerate the CRL
    # file. There are already known problems in Puppet with dealing
    # with such CRLs. Now if we revoke and/or sign some certificates
    # in parallel, there is a chance we receive an OpenSSL error (3).
    # In that case we cannot do anything but retry the operation.
    for retry in range(1, retries + 1):
        if retry > 1:
            logger.debug(f'Trying to clear certificate (try {retry}/{retries})')

        # Try to clean the certificate
        if is_puppet_v5:
            success = clean_cert_v5(ca_host, vm_host, puppet_exe)
        else:
            success = clean_cert_v6(ca_host, vm_host, puppetserver_exe)
        if success:
            if retry > 1:
                logger.info(
                    f'Cleaned certificate for {vm_host} after {retry} tries',
                )
            else:
                logger.info(f'Cleaned certificate for {vm_host}')
            return

        # Retry after one second
        sleep(1)

    # Failed all the way
    logger.error(
        f'Failed to clear certificate for {vm_host} after {retries} tries',
    )


def run_cmd(host: str, cmd: str):
    if 'user' in COMMON_FABRIC_SETTINGS:
        user = COMMON_FABRIC_SETTINGS['user']
    else:
        user = None

    with settings(host_string=host, user=user, warn_only=True):
        return sudo(cmd, quiet=True, pty=False, shell=False)


def find_puppet_executable(host: str) -> str:
    paths = ['/usr/bin/puppet', '/opt/puppetlabs/puppet/bin/puppet']
    return find_executable(host, paths)


def find_puppetserver_executable(host: str) -> str:
    paths = ['/usr/bin/puppetserver', '/opt/puppetlabs/bin/puppetserver']
    return find_executable(host, paths)


def find_executable(host: str, paths: list) -> str:
    find_cmd = f'/usr/bin/find {" ".join(paths)} -xtype f 2>/dev/null | grep .'
    res = run_cmd(host, find_cmd)
    if res.failed:
        raise RuntimeError('Could not find requested Puppet executable')
    return res.stdout.splitlines()[0]


def clean_cert_v5(ca_host: str, vm_host: str, puppet_exe: str) -> bool:
    # Check whether there is a valid certificate to be cleaned at all.
    verify_cmd = f'{puppet_exe} cert verify {vm_host}'
    res = run_cmd(ca_host, verify_cmd)

    # Exit code 24 means there is no valid certificate. In such a case
    # we can skip the revoking entirely and prevent the CA from
    # scanning the whole CRL (which can be lengthy).
    if res.return_code == 24:
        logger.debug(
            f'Skip revoking of {vm_host} because there is no valid '
            'certificate known to the CA',
        )
        return True

    # Try to clean the cert
    clean_cmd = f'{puppet_exe} cert clean {vm_host}'
    res = run_cmd(ca_host, clean_cmd)

    return res.return_code != 3


def clean_cert_v6(
    ca_host: str,
    vm_host: str,
    puppetserver_exe: str,
) -> bool:
    clean_cmd = f'{puppetserver_exe} ca clean --certname {vm_host}'
    res = run_cmd(ca_host, clean_cmd)

    # Check if the cleaning was successful or if there was nothing to
    # clean in the first place
    if res.return_code == 1 and 'Could not find files to clean' in res:
        logger.debug(
            f'Skip revoking of {vm_host} because there is no valid '
            'certificate known to the CA',
        )
        return True

    return res.return_code == 0
