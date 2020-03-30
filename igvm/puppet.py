"""igvm -  Puppet interaction class

Copyright (c) 2020 InnoGames GmbH
"""
import random
from logging import getLogger
from time import sleep

from adminapi.dataset import Query
from fabric.api import settings
from fabric.operations import sudo, run

from igvm.exceptions import ConfigError
from igvm.settings import COMMON_FABRIC_SETTINGS

logger = getLogger(__name__)


def get_puppet_ca(vm):
    puppet_ca_type = Query(
        {'hostname': vm['puppet_ca']},
        ['servertype'],
    ).get()['servertype']

    if puppet_ca_type not in ['vm', 'public_domain']:
        raise ConfigError(
            'Servertype {} not supported for puppet_ca'.format(
                puppet_ca_type,
            ),
        )

    if puppet_ca_type == 'vm':
        return vm['puppet_ca']

    ca_query = Query(
        {'domain': vm['puppet_ca']},
        [{'lb_nodes': ['hostname', 'state']}],
    )
    ca_hosts = [
        lb_node['hostname']
        for res in ca_query
        for lb_node in res['lb_nodes']
        if lb_node['state'] in ['online', 'deploy_online']
    ]
    random.shuffle(ca_hosts)

    return ca_hosts[0]


def clean_cert(vm, user=None, retries=60):
    if 'user' in COMMON_FABRIC_SETTINGS:
        user = COMMON_FABRIC_SETTINGS['user']

    ca_host = get_puppet_ca(vm)

    logger.info('Cleaning puppet certificate for {} on {}'.format(
        vm['hostname'], ca_host,
    ))

    with settings(
        host_string=ca_host,
        user=user,
        warn_only=True,
    ):
        version = sudo('/usr/bin/puppet --version', shell=False, quiet=True)

        if not version.succeeded or int(version.split('.')[0]) < 6:
            # Every signing and revoking will have the CA regenerate the CRL
            # file. There are already known problems in Puppet with dealing
            # with such CRLs. Now if we revoke and/or sign some certificates
            # in parallel, there is a chance we receive an OpenSSL error (3).
            # In that case we cannot do anything but retry the operation.
            for retry in range(1, retries + 1):
                logger.info(
                    'Trying to clear certificate ({}/{})'.format(
                        retry, retries,
                    )
                )
                res = run(
                    'sudo /usr/bin/puppet cert verify {} '
                    '&& sudo /usr/bin/puppet cert clean {}'.format(
                        vm['hostname'], vm['hostname'],
                    ),
                    shell=False,
                )
                if res.return_code != 3:
                    break

                sleep(1)
            else:
                logger.error(
                    'Failed to clear certificate for {} after {} tries'.format(
                        vm['hostname'], retries,
                    ),
                )
        else:
            sudo(
                '/opt/puppetlabs/bin/puppetserver ca clean '
                '--certname {}'.format(vm['hostname']),
                shell=False,
            )
