import logging

from adminapi.dataset import query
from adminapi.filters import Contains, Not

log = logging.getLogger(__name__)


def get_network_config(server):
    ret = {}
    # It is impossible to use server['route_network']
    # if IP address of a server was changed via --newip.
    route_network = query(
        servertype='route_network',
        state=Not('retired'),
        intern_ip=Contains(server['intern_ip']),
    ).restrict(
        'hostname',
        'intern_ip',
        'default_gateway',
        'internal_gateway',
        'primary_ip6',
        'vlan_tag',
    ).get()

    default_gateway_route, internal_gateway_route = get_gateways(route_network)

    # For server installation internal gateway is the default and the only one
    if server.get('intern_ip'):
        ret['ipv4_address'] = server['intern_ip']
        ret['ipv4_netmask'] = route_network['intern_ip'].prefixlen
        ret['ipv4_default_gw'] = internal_gateway_route.get('intern_ip', None)

    if server.get('primary_ip6'):
        ret['ipv6_address'] = server['primary_ip6']
        ret['ipv6_netmask'] = route_network['primary_ip6'].prefixlen
        ret['ipv6_default_gw'] = internal_gateway_route.get(
            'primary_ip6', None
        )

    ret['vlan_tag'] = route_network['vlan_tag']
    ret['vlan_name'] = route_network['hostname']

    return ret


def get_gateways(network):
    """ Get default and internal gateway Serveradmin objects
        for given network. If they are not defined, return
        empty dictionaries to simulate Serveradmin objects.
    """

    if network.get('default_gateway'):
        default_gateway = query(
            state=Not('retired'),
            hostname=network['default_gateway'],
        ).get()
    else:
        default_gateway = {}

    if network.get('internal_gateway'):
        internal_gateway = query(
            state=Not('retired'),
            hostname=network['internal_gateway'],
        ).get()
    else:
        internal_gateway = {}

    return default_gateway, internal_gateway
