import logging
from os import environ
from time import sleep
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime, timedelta
from json import loads as json_loads, dumps as json_dumps, JSONDecodeError

from libvirt import open as libvirt_open, libvirtError
from libvirt import VIR_DOMAIN_SHUTOFF
from adminapi.dataset import Query
from adminapi.filters import Any
from igvm.settings import VM_ATTRIBUTES

log = logging.getLogger(__name__)

HYPERVISOR_OBJECT_ID = int(environ['HYPERVISOR_OBJECT_ID'])
VM_DELETION_THRESHOLD = timedelta(days=7)
IGVM_STATE_PATH = '/var/lib/igvmd/retired_vms.json'

# TODO: Add some way for nagios to run this, checking the VMs state and
# generating a nagios compatible output. Maybe a good first RPC feature?


def main():
    # TODO: Configure proper logging.
    logging.basicConfig(level=logging.DEBUG)
    while True:
        with libvirt_connection('qemu:///system') as conn:
            vms = get_vms(conn)
            vms_by_state = sort_vms_by_state(vms)
            start_stop_vms(vms, vms_by_state)
            cleanup_retired_vms(vms, vms_by_state)
        sleep(60 * 5)


@contextmanager
def libvirt_connection(*args, **kwargs):
    """Wrap libvirt.open into a contextmanager

    Make sure the libvirt connection is closed on finish and error.
    """

    conn = libvirt_open(*args, **kwargs)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except libvirtError as e:
            log.error('Failed closing libvirt connection: {}'.format(e))


def get_vms(conn):
    """Collect VM data from libvirt and serveradmin

    We take each libvirt domain defined, try to get a matching dataset object
    from serveradmin and return a joined dictionary mapping from object_id to
    a second dictionary with the keys 'domain', 'domain_name' and 'dataset_obj'

    Note that 'dataset_obj' might be None if no matching object on serveradmin
    could be found.

    FIXME: We are in a dilemma because legacy domains do not yet have the
    object_id prefix in front on their domain name. Adding a fallback to
    search by hostname is not a good idea as this daemon might end up
    starting old VMs if a new VM with the same name as the old VM was created
    on serveradmin.
    """

    # Get all libvirt domains
    vms = {
        int(domain.name().split('_', 1)[0]): {
            'domain': domain,
            'domain_name': domain.name(),
            'dataset_obj': None,
        } for domain in conn.listAllDomains()
    }


    # FIXME: Drop this once we can reach serveradmin
    vms[235073]['dataset_obj'] = {
        'object_id': 235073,
        'state': 'retired',
        'hypervisor': {
            'object_id': 52517,
            'hostname': 'hv-awtest-2c5-2-11.ndco.ig.local',
        }
    }
    return vms


    # Try to get a dataset object for each domain and join the data
    for dataset_obj in Query(
        {'object_id': Any(*vms.keys())}, VM_ATTRIBUTES
    ):
        vms[dataset_obj['object_id']]['dataset_obj'] = dataset_obj

    return vms


def sort_vms_by_state(vms):
    vms_by_state = defaultdict(list)
    for object_id, vm in vms.items():
        if not vm['dataset_obj']:
            vms_by_state['not_in_serveradmin'].append(object_id)

        elif (
            vm['dataset_obj']['hypervisor']['object_id'] !=
            HYPERVISOR_OBJECT_ID
        ):
            vms_by_state['wrong_hypervisor'].append(object_id)

        elif vm['dataset_obj']['state'] == 'retired':
            vms_by_state['retired'].append(object_id)

        else:
            vms_by_state['online'].append(object_id)

    return vms_by_state


def start_stop_vms(vms, vms_by_state):
    def _enforce_vm_state(domain, should_run, message, is_warning=False):
        is_running = domain.info()[0] < VIR_DOMAIN_SHUTOFF
        if is_running != should_run:
            log.log(30 if is_warning else 20, message)

            if should_run:
                if domain.create() != 0:
                    log.error('Unable to start "{}"'.format(domain.name()))
            else:
                if domain.destroy() != 0:
                    log.error('Unable to stop "{}".'.format(domain.name()))

    for object_id in vms_by_state['not_in_serveradmin']:
        _enforce_vm_state(vms[object_id]['domain'], should_run=False, message=(
            'The libvirt domain {} could not be found in serveradmin. '
            'The object has probably been deleted from serveradmin '
            'before deleting this libvirt domain. Stopping...'
            .format(vms[object_id]['domain_name'])
        ), is_warning=True)

    for object_id in vms_by_state['wrong_hypervisor']:
        _enforce_vm_state(vms[object_id]['domain'], should_run=False, message=(
            'The libvirt domain {} should not be running on this '
            'hypervisor, but on {} instead. Stopping...'.format(
                vms[object_id]['domain_name'],
                vms[object_id]['dataset_obj']['hypervisor']['hostname']
            )
        ), is_warning=True)

    for object_id in vms_by_state['retired']:
        _enforce_vm_state(vms[object_id]['domain'], should_run=False, message=(
            'The libvirt domain {} is in state retired and should therefore '
            'be stopped. Stopping...'.format(vms[object_id]['domain_name'])
        ))

    for object_id in vms_by_state['online']:
        _enforce_vm_state(vms[object_id]['domain'], should_run=True, message=(
            'The libvirt domain {} should be running. Starting...'
            .format(vms[object_id]['domain_name'])
        ))


def cleanup_retired_vms(vms, vms_by_state):
    # Note down the current timestamp for all retired VMs on this hypervisor
    now = int(datetime.utcnow().timestamp())
    retired_vms = {
        # XXX: The object_id is a string as we JSON serialize this dict. JSON
        # only allows dict keys to be strings and python json.dumps will
        # convert integers dict keys to strings anyway.
        str(object_id): now
        for object_id in vms_by_state['retired']
    }

    try:
        with open(IGVM_STATE_PATH, 'r') as fd:
            previously_retired_vms = json_loads(fd.read())
    except FileNotFoundError:
        previously_retired_vms = dict()
    except (OSError, JSONDecodeError) as e:
        log.error('Loading retired VMs state file failed: {}'.format(e))
        return

    # Copy over first seen times for VMs that we already noted as retired in
    # previous checks.
    for object_id, first_seen in previously_retired_vms.items():
        if object_id in retired_vms:
            retired_vms[object_id] = first_seen

    try:
        with open(IGVM_STATE_PATH, 'w') as fd:
            fd.write(json_dumps(retired_vms))
    except OSError as e:
        log.error('Saving retired VMs state file failed: {}'.format(e))
        # We do not return here. If we came this far, we might as well
        # delete the VMs that are past their best by date.

    for object_id, first_seen in retired_vms.items():
        time_in_state_retired = timedelta(seconds=now - first_seen)
        if time_in_state_retired > VM_DELETION_THRESHOLD:
            log.info(
                'Deleting the domain {} from this hypervisor as it has been '
                'in state retired for {} days.'.format(
                    vms[int(object_id)]['domain_name'],
                    time_in_state_retired.days
                )
            )
            # TODO: Undefine VM, delete storage
            # TODO: Delete serveradmin object
            # Deleting a VM is a bit more complex so instead of reimplementing
            # everything we should reuse igvm.hypervisor.Hypervisor.undefine_vm
            # It's probably easiest to subclass it for now and create an igvmd
            # variant overwriting the conn method to something simple like
            # `return libvirt_open('qemu:///system')`
