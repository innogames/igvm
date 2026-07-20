"""igvm - Fabric compatibility shim

Copyright (c) 2026 InnoGames GmbH

igvm targets modern ``fabric`` 3.x (the ``fabric.Connection`` object API), but
some hosts still have old ``fabric3`` (a Py3 fork of Fabric 1.x, global
``fabric.api``).  Both import as ``fabric`` and can't be co-installed, so this
module detects the flavor at import and exposes one surface (``Connection``,
``make_fabric_config``, ``disconnect_all``, ``IS_LEGACY_FABRIC``) — a thin
passthrough on modern fabric, a small ``fabric.api`` adapter on fabric3.

Short-lived: all fabric3 code is gated by ``IS_LEGACY_FABRIC`` so it can be
deleted as a clean revert once every host runs modern fabric.
"""

from sys import stdout

import fabric

from igvm.exceptions import RemoteCommandError

# Sudo prompt the remote SSH_ORIGINAL_COMMAND whitelists expect.  fabric3 used
# 'sudo password:'; modern fabric defaults to '[sudo] password:', so force it
# back.  Kept out of settings.py so the shim needs no igvm.settings import.
FABRIC_SUDO_PROMPT = 'sudo password:'

# Modern fabric exposes fabric.Connection / fabric.Config; fabric3 does not.
IS_LEGACY_FABRIC = not hasattr(fabric, 'Connection')

# Registry of active connections, used by the modern disconnect_all().
_active_connections = set()


if not IS_LEGACY_FABRIC:
    # ----------------------------------------------------------------- modern
    Connection = fabric.Connection

    def make_fabric_config():
        return fabric.Config(overrides={'sudo': {'prompt': FABRIC_SUDO_PROMPT}})

    def disconnect_all():
        """Close all tracked SSH connections."""
        for conn in list(_active_connections):
            try:
                conn.close()
            except Exception:
                pass
        _active_connections.clear()

else:
    # ----------------------------------------------------------------- legacy
    import fabric.api
    import fabric.network
    import fabric.state

    # Make fabric3's sudo prompt match the whitelist format.
    fabric.api.env.sudo_prompt = FABRIC_SUDO_PROMPT

    # The global settings the pre-migration code used (git master settings.py).
    _COMMON_FABRIC_SETTINGS = dict(
        disable_known_hosts=True,
        use_ssh_config=True,
        always_use_pty=stdout.isatty(),
        forward_agent=True,
        shell='/bin/sh -c',
        timeout=5,
        connection_attempts=3,
        remote_interrupt=True,
    )

    class _LegacyConfig:
        """Stand-in for fabric.Config on fabric3 (no per-connection config; the
        sudo prompt is global, set at import).  Lets call sites keep passing
        ``config=make_fabric_config()`` uniformly."""

        def __init__(self):
            self.sudo = {'prompt': FABRIC_SUDO_PROMPT}

    def make_fabric_config():
        return _LegacyConfig()

    class _LegacyResult:
        """Map a fabric3 result (a str that *is* stdout, with .return_code/
        .failed/.succeeded/.stderr) onto the invoke.Result names igvm reads
        (.stdout/.stderr/.return_code/.ok/.failed)."""

        def __init__(self, result):
            self._result = result

        @property
        def stdout(self):
            return str(self._result)

        @property
        def stderr(self):
            return getattr(self._result, 'stderr', '') or ''

        @property
        def return_code(self):
            return self._result.return_code

        @property
        def ok(self):
            return self._result.succeeded

        @property
        def failed(self):
            return self._result.failed

    class _LegacyConnection:
        """Subset of modern fabric.Connection over fabric3, whose env and
        connections are global.  Each call opens a
        ``fabric.api.settings(host_string=...)`` block, as pre-migration
        Host.run() did."""

        def __init__(self, host, config=None, user=None, connect_timeout=None,
                     connect_kwargs=None, **_ignored):
            self.host = str(host)
            self.user = user
            self._closed = False

        @property
        def is_connected(self):
            if self._closed:
                return False
            try:
                return self.host in fabric.state.connections
            except Exception:
                return False

        def _base_settings(self):
            settings = dict(_COMMON_FABRIC_SETTINGS)
            if self.user:
                settings['user'] = self.user
            # Backstop for non-warn paths (get/put); run/sudo set warn_only.
            settings['abort_exception'] = RemoteCommandError
            return settings

        def _execute(self, func, command, hide, warn, pty, shell, extra_kwargs):
            settings = self._base_settings()
            settings['warn_only'] = bool(warn)

            ctx = []
            if hide:
                # igvm only ever passes hide=True (suppress everything).
                ctx.append(fabric.api.hide('everything'))

            call_kwargs = dict(extra_kwargs)
            if pty is not None:
                call_kwargs['pty'] = pty
            if shell == '':
                # Modern "don't wrap in a shell" -> fabric3 shell=False.
                call_kwargs['shell'] = False

            with fabric.api.settings(*ctx, host_string=self.host, **settings):
                result = func(command, **call_kwargs)

            self._closed = False
            return _LegacyResult(result)

        def run(self, command, hide=False, warn=False, pty=None, shell=None,
                **kwargs):
            return self._execute(
                fabric.api.run, command, hide, warn, pty, shell, kwargs,
            )

        def sudo(self, command, hide=False, warn=False, pty=None, shell=None,
                 **kwargs):
            return self._execute(
                fabric.api.sudo, command, hide, warn, pty, shell, kwargs,
            )

        def get(self, remote, local):
            settings = self._base_settings()
            with fabric.api.settings(
                fabric.api.hide('commands'), host_string=self.host, **settings,
            ):
                return fabric.api.get(remote, local)

        def put(self, local, remote):
            settings = self._base_settings()
            with fabric.api.settings(host_string=self.host, **settings):
                return fabric.api.put(local, remote)

        def close(self):
            self._closed = True
            try:
                connections = fabric.state.connections
                if self.host in connections:
                    connections[self.host].get_transport().close()
                    del connections[self.host]
            except Exception:
                pass

    Connection = _LegacyConnection

    def disconnect_all():
        """Close all SSH connections from fabric3's global pool."""
        try:
            fabric.network.disconnect_all()
        except Exception:
            pass
        _active_connections.clear()
