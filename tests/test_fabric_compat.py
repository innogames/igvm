"""igvm - Tests for the fabric compatibility shim

Copyright (c) 2026 InnoGames GmbH

These tests exercise igvm.fabric_compat against BOTH fabric flavors without
needing fabric3 installed (it cannot be co-installed with modern fabric):

* the modern path is asserted against the real, installed fabric;
* the legacy path is asserted by injecting a fake ``fabric`` module that mimics
  the fabric3 ``fabric.api`` surface and, crucially, has no ``Connection``
  attribute, then loading a fresh copy of the shim against it.

Neither test talks to the network or to serveradmin, so both run in plain CI.
"""

import importlib.util
import os
import sys
import types
from contextlib import contextmanager

import igvm
import igvm.fabric_compat as modern_compat


class FakeFabric3Result(str):
    """Mimics a fabric3 run()/sudo() result: a str that *is* stdout."""

    def __new__(cls, stdout, return_code=0, stderr='', succeeded=True):
        instance = super().__new__(cls, stdout)
        instance.return_code = return_code
        instance.stderr = stderr
        instance.succeeded = succeeded
        instance.failed = not succeeded
        return instance


def _build_fake_fabric(calls):
    """Build a fake fabric3-style ``fabric`` package recording every call."""
    fabric = types.ModuleType('fabric')
    api = types.ModuleType('fabric.api')
    state = types.ModuleType('fabric.state')
    network = types.ModuleType('fabric.network')

    class Env(dict):
        def __getattr__(self, key):
            return self.get(key)

        def __setattr__(self, key, value):
            self[key] = value

    api.env = Env()
    state.connections = {}

    @contextmanager
    def settings(*args, **kwargs):
        calls.append(('settings', args, kwargs))
        yield

    def hide(*groups):
        return ('hide', groups)

    def run(command, **kwargs):
        calls.append(('run', command, kwargs))
        return FakeFabric3Result('ran:' + command)

    def sudo(command, **kwargs):
        calls.append(('sudo', command, kwargs))
        return FakeFabric3Result('out:' + command)

    def get(remote, local):
        calls.append(('get', remote, local))

    def put(local, remote):
        calls.append(('put', local, remote))

    def disconnect_all():
        calls.append(('disconnect_all',))

    api.settings = settings
    api.hide = hide
    api.run = run
    api.sudo = sudo
    api.get = get
    api.put = put
    network.disconnect_all = disconnect_all

    fabric.api = api
    fabric.state = state
    fabric.network = network
    # No fabric.Connection / fabric.Config: this is what marks it as legacy.
    return fabric, api, state, network


def _load_legacy_compat(monkeypatch, calls):
    """Import a fresh copy of fabric_compat against the fake fabric3."""
    fabric, api, state, network = _build_fake_fabric(calls)
    monkeypatch.setitem(sys.modules, 'fabric', fabric)
    monkeypatch.setitem(sys.modules, 'fabric.api', api)
    monkeypatch.setitem(sys.modules, 'fabric.state', state)
    monkeypatch.setitem(sys.modules, 'fabric.network', network)

    path = os.path.join(os.path.dirname(igvm.__file__), 'fabric_compat.py')
    spec = importlib.util.spec_from_file_location(
        'igvm._fabric_compat_legacy_under_test', path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, api


# --------------------------------------------------------------------- modern

def test_modern_fabric_detected():
    import fabric

    assert modern_compat.IS_LEGACY_FABRIC is False
    assert modern_compat.Connection is fabric.Connection
    config = modern_compat.make_fabric_config()
    assert config.sudo['prompt'] == modern_compat.FABRIC_SUDO_PROMPT


# --------------------------------------------------------------------- legacy

def test_legacy_fabric_detected(monkeypatch):
    module, api = _load_legacy_compat(monkeypatch, [])

    assert module.IS_LEGACY_FABRIC is True
    # The sudo prompt is forced onto the global env to match the whitelist.
    assert api.env.sudo_prompt == module.FABRIC_SUDO_PROMPT
    # make_fabric_config() still returns a config-like carrier.
    assert module.make_fabric_config().sudo['prompt'] == \
        module.FABRIC_SUDO_PROMPT


def test_legacy_sudo_maps_kwargs_and_result(monkeypatch):
    calls = []
    module, _ = _load_legacy_compat(monkeypatch, calls)

    conn = module.Connection('host1', user='bob')
    result = conn.sudo('whoami', hide=True, warn=True, pty=False)

    # Result adapter exposes the modern attribute names.
    assert result.stdout == 'out:whoami'
    assert result.ok is True
    assert result.failed is False
    assert result.return_code == 0

    settings_calls = [c for c in calls if c[0] == 'settings']
    assert settings_calls, 'sudo() must open a settings() context'
    _, settings_args, settings_kwargs = settings_calls[-1]
    # warn=True -> warn_only=True; host_string + user are propagated.
    assert settings_kwargs['warn_only'] is True
    assert settings_kwargs['host_string'] == 'host1'
    assert settings_kwargs['user'] == 'bob'
    # hide=True -> a hide('everything') context manager is passed positionally.
    assert ('hide', ('everything',)) in settings_args

    sudo_calls = [c for c in calls if c[0] == 'sudo']
    assert sudo_calls == [('sudo', 'whoami', {'pty': False})]


def test_legacy_run_no_shell_wrap(monkeypatch):
    calls = []
    module, _ = _load_legacy_compat(monkeypatch, calls)

    conn = module.Connection('host2')
    # Modern shell='' ("don't wrap in a shell") -> fabric3 shell=False.
    conn.run('id', shell='')

    run_calls = [c for c in calls if c[0] == 'run']
    assert run_calls == [('run', 'id', {'shell': False})]


def test_legacy_disconnect_all(monkeypatch):
    calls = []
    module, _ = _load_legacy_compat(monkeypatch, calls)

    module.disconnect_all()
    assert ('disconnect_all',) in calls
