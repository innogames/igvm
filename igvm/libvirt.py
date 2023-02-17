"""igvm - libvirt

Copyright (c) 2023 InnoGames GmbH
"""
import logging
from os import path, environ

import libvirt

from igvm.utils import get_ssh_config

log = logging.getLogger(__name__)
_conns = {}


class LibvirtConn:
    def __init__(self, fqdn: str, username: str):
        self._fqdn = fqdn
        self._username = username

        self._conn: libvirt.virConnect = libvirt.open(self._get_url())
        self._conn.registerCloseCallback(self._close_callback, fqdn)
        self._replaced = False

    def __getattr__(self, item):
        return self._wrap_call(item)

    def _close_callback(self, _: libvirt.virConnect, reason: int, fqdn: str):
        if reason == libvirt.VIR_CONNECT_CLOSE_REASON_CLIENT:
            # Intentional close
            return

        # Try to reconnect in case of unexpected errors
        reason_str = self._get_close_reason(reason)
        log.warning(
            f'Connection to {fqdn} has closed: {reason_str}. '
            f'Reconnecting..',
        )

        new_conn: libvirt.virConnect = libvirt.open(self._get_url())
        new_conn.registerCloseCallback(self._close_callback, fqdn)
        self._conn = new_conn
        self._replaced = True

    def _wrap_call(self, item: str) -> callable:
        def wrapped_call(*args, **kwargs):
            try:
                return getattr(self._conn, item)(*args, **kwargs)
            except libvirt.libvirtError:
                # Retry the call upon reconnect
                if self._replaced:
                    self._replaced = False
                    return getattr(self._conn, item)(*args, **kwargs)
                raise

        return wrapped_call

    @staticmethod
    def _get_close_reason(reason: int) -> str:
        if reason == libvirt.VIR_CONNECT_CLOSE_REASON_ERROR:
            return 'I/O error'
        elif reason == libvirt.VIR_CONNECT_CLOSE_REASON_EOF:
            return 'EOF'
        elif reason == libvirt.VIR_CONNECT_CLOSE_REASON_KEEPALIVE:
            return 'keepalive timeout'
        elif reason == libvirt.VIR_CONNECT_CLOSE_REASON_CLIENT:
            return 'closed by client'
        else:
            return 'unknown'

    def _get_url(self) -> str:
        host_uri = self._fqdn
        if self._username:
            host_uri = f'{self._username}@{host_uri}'
        scripts_dir = path.join(path.dirname(__file__), 'scripts')

        return (
            'qemu+ssh://{}/system?'
            'socket=/var/run/libvirt/libvirt-sock&'
            'command={}/ssh_wrapper'
        ).format(host_uri, scripts_dir)


def get_virtconn(fqdn: str) -> libvirt.virConnect:
    if fqdn not in _conns:
        if 'IGVM_SSH_USER' in environ:
            username = environ.get('IGVM_SSH_USER')
        else:
            ssh_config = get_ssh_config(fqdn)
            if 'user' in ssh_config:
                username = ssh_config['user']
            else:
                username = ''

        _conns[fqdn] = LibvirtConn(fqdn, username)

    return _conns[fqdn]


def close_virtconns():
    for fqdn in list(_conns.keys()):
        conn = _conns[fqdn]
        try:
            conn.close()
        except libvirt.libvirtError:
            pass
        del _conns[fqdn]
