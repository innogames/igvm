"""igvm -  Puppet interaction class

Copyright (c) 2018 InnoGames GmbH
"""

from __future__ import division

import random

from fabric.api import settings
from fabric.operations import sudo

from adminapi.dataset import Query
from igvm.exceptions import ConfigError
from igvm.settings import COMMON_FABRIC_SETTINGS

def clean_cert(vm, user=None):
    if 'user' in COMMON_FABRIC_SETTINGS:
        user = COMMON_FABRIC_SETTINGS['user']

    puppet_ca_type = Query(
        {
            'hostname': vm['puppet_ca'],
        },
        ['servertype'],
    ).get()['servertype']

    if puppet_ca_type not in ['vm', 'public_domain']:
        raise ConfigError(
            'Servertype {} not supported for puppet_ca'.format(
                puppet_ca_type,
            ),
        )

    if puppet_ca_type == 'vm':
        ca_host = vm['puppet_ca']
    else:
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

        ca_host = ca_hosts[0]
    with settings(
        host_string=ca_host,
        user=user,
        warn_only=True,
    ):
        version = sudo('/usr/bin/puppet --version', shell=False, quiet=True)

        if not version.succeeded or int(version.split('.')[0]) < 6:
            sudo('/usr/bin/puppet cert clean {}'.format(
                vm['hostname'],
            ), shell=False)
        else:
            sudo(
                '/opt/puppetlabs/bin/puppetserver ca clean '
                '--certname {}'.format(vm['hostname']),
                shell=False,
            )
