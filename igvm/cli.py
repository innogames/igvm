"""igvm - The command line interface

Copyright (c) 2017, InnoGames GmbH
"""

from __future__ import print_function
from argparse import ArgumentParser, _SubParsersAction
from logging import StreamHandler, root as root_logger
import time

from fabric.network import disconnect_all

from igvm.buildvm import buildvm
from igvm.migratevm import migratevm
from igvm.commands import (
    disk_set,
    host_info,
    mem_set,
    vcpu_set,
    vm_start,
    vm_stop,
    vm_rebuild,
    vm_restart,
    vm_delete,
    vm_sync,
    vm_rename,
)
from igvm.utils.virtutils import close_virtconns


class ColorFormatters():
    BOLD = '\033[1m{}\033[0m'
    WARNING = '\033[1;33m{}\033[0m'
    ERROR = '\033[1;31m{}\033[0m'
    CRITICAL = '\033[1;41m{}\033[0m'


class IGVMArgumentParser(ArgumentParser):
    def format_help(self):
        if not any(isinstance(a, _SubParsersAction) for a in self._actions):
            return super(IGVMArgumentParser, self).format_help()

        out = []
        out.append(ColorFormatters.BOLD.format(__doc__))
        out.append('Available commands:\n')

        subparsers_actions = [
            action for action in self._actions
            if isinstance(action, _SubParsersAction)
        ]

        # There will probably only be one subparser_action, but better safe
        # than sorry.
        for subparsers_action in subparsers_actions:
            # Get all subparsers and print help
            for choice, subparser in subparsers_action.choices.items():
                out.append(ColorFormatters.BOLD.format(choice))
                if subparser.get_default('func').__doc__:
                    out.append('\n'.join(
                        '\t{}'.format(l.strip()) for l in subparser
                        .get_default('func').__doc__.strip().splitlines()
                    ))
                out.append('\n\t{}'.format(subparser.format_usage()))

        return '\n'.join(out)


class IGVMLogHandler(StreamHandler):
    """Extend StreamHandler to format messages short-cutting Formatters"""

    def __init__(self, *args, **kwargs):
        super(IGVMLogHandler, self).__init__(*args, **kwargs)
        self.isatty = self.stream.isatty()

    def format(self, record):
        level = record.levelname
        msg = '{}: {}: {}'.format(level, record.name, record.getMessage())
        if self.isatty and level in vars(ColorFormatters):
            msg = getattr(ColorFormatters, level).format(msg)
        return msg


def parse_args():
    top_parser = IGVMArgumentParser('igvm')
    top_parser.add_argument('--silent', '-s', action='count', default=0)
    top_parser.add_argument('--verbose', '-v', action='count', default=0)

    subparsers = top_parser.add_subparsers(help='Actions')

    subparser = subparsers.add_parser(
        'build',
        description=buildvm.__doc__,
    )
    subparser.set_defaults(func=buildvm)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        '--localimage',
        help='Image file for for installation from local fs',
    )
    subparser.add_argument(
        '--postboot',
        metavar='postboot_script',
        help='Run postboot_script on the guest after first boot',
    )
    subparser.add_argument(
        '--nopuppet',
        action='store_true',
        help='Skip running puppet in chroot before powering up',
    )
    subparser.add_argument(
        '--ignore-reserved',
        dest='ignore_reserved',
        action='store_true',
        help='Force build on a Host which has the state online_reserved',
    )

    subparser = subparsers.add_parser(
        'migrate',
        description=migratevm.__doc__,
    )
    subparser.set_defaults(func=migratevm)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        'hypervisor_hostname',
        nargs='?',
        default=None,
        help='Hostname of destination hypervisor',
    )
    subparser.add_argument(
        '--newip',
        metavar='IP address',
        help='IP address to move VM to, in case you migrate between VLANs',
    )
    subparser.add_argument(
        '--runpuppet',
        action='store_true',
        help='Run puppet in chroot before powering up',
    )
    subparser.add_argument(
        '--maintenance',
        action='store_true',
        help='Set state to maintenance',
    )
    subparser.add_argument(
        '--offline',
        action='store_true',
        help='Force offline migration, also implies --maintenance',
    )
    subparser.add_argument(
        '--ignore-reserved',
        dest='ignore_reserved',
        action='store_true',
        help='Force migration to a Host which has the state online_reserved',
    )
    subparser.add_argument(
        '--offline-transport',
        default='drbd',
        help='Specify drbd (default) or netcat transport to migrate disk image',
    )
    subparser = subparsers.add_parser(
        'disk-set',
        description=disk_set.__doc__,
    )
    subparser.set_defaults(func=disk_set)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        'size',
        help=(
            'New disk size with an optional unit (default GiB). '
            'Can be specified relative with "+". Only integers are allowed'
        )
    )
    subparser.add_argument(
        '--ignore-reserved',
        dest='ignore_reserved',
        action='store_true',
        help='Force setting disk on a hypervisor with state online_reserved',
    )

    subparser = subparsers.add_parser(
        'mem-set',
        description=mem_set.__doc__,
    )
    subparser.set_defaults(func=mem_set)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        'size',
        help=(
            'New memory size with optional unit (default is MiB).'
            'Only integers are allowed.'
        ),
    )
    subparser.add_argument(
        '--offline',
        action='store_true',
        help='Shutdown VM, change memory, and restart VM',
    )
    subparser.add_argument(
        '--ignore-reserved',
        dest='ignore_reserved',
        action='store_true',
        help='Force setting memory on a hypervisor with state online_reserved',
    )

    subparser = subparsers.add_parser(
        'vcpu-set',
        description=vcpu_set.__doc__,
    )
    subparser.set_defaults(func=vcpu_set)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        'count',
        type=int,
        help='New number of CPUs',
    )
    subparser.add_argument(
        '--offline',
        action='store_true',
        help='Shutdown VM, change CPUs, and restart VM',
    )
    subparser.add_argument(
        '--ignore-reserved',
        dest='ignore_reserved',
        action='store_true',
        help='Force setting cpus on a hypervisor with state online_reserved',
    )

    subparser = subparsers.add_parser(
        'start',
        description=vm_start.__doc__,
    )
    subparser.set_defaults(func=vm_start)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )

    subparser = subparsers.add_parser(
        'stop',
        description=vm_stop.__doc__,
    )
    subparser.set_defaults(func=vm_stop)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        '--force',
        action='store_true',
        help='Do not wait for guest to shutdown gracefully',
    )

    subparser = subparsers.add_parser(
        'restart',
        description=vm_restart.__doc__,
    )
    subparser.set_defaults(func=vm_restart)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        '--force',
        action='store_true',
        help='Do not wait for guest to shutdown gracefully',
    )
    subparser.add_argument(
        '--no-redefine',
        action='store_true',
        help='Do not redefine the domain to use latest hypervisor settings',
    )

    subparser = subparsers.add_parser(
        'delete',
        description=vm_delete.__doc__,
    )
    subparser.set_defaults(func=vm_delete)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        '--force',
        action='store_true',
        help='Shutdown VM if running',
    )
    subparser.add_argument(
        '--retire',
        action='store_true',
        help='Set VM state to "retired" on Serveradmin instead of deleting',
    )

    subparser = subparsers.add_parser(
        'info',
        description=host_info.__doc__,
    )
    subparser.set_defaults(func=host_info)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )

    subparser = subparsers.add_parser(
        'sync',
        description=vm_sync.__doc__,
    )
    subparser.set_defaults(func=vm_sync)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )

    subparser = subparsers.add_parser(
        'rebuild',
        description=vm_rebuild.__doc__,
    )
    subparser.set_defaults(func=vm_rebuild)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        '--force',
        action='store_true',
        help='Shutdown VM, if running',
    )

    subparser = subparsers.add_parser(
        'rename',
        description=vm_rename.__doc__,
    )
    subparser.set_defaults(func=vm_rename)
    subparser.add_argument(
        'vm_hostname',
        help='Hostname of the guest system',
    )
    subparser.add_argument(
        'new_hostname',
        help='New hostname',
    )
    subparser.add_argument(
        '--offline',
        action='store_true',
        help='Shutdown VM, if running',
    )

    return vars(top_parser.parse_args())


def main():
    args = parse_args()
    configure_root_logger(args.pop('silent'), args.pop('verbose'))

    try:
        args.pop('func')(**args)
    finally:
        # Fabric requires the disconnect function to be called after every
        # use.  We are also taking our chance to disconnect from
        # the hypervisors.
        disconnect_all()
        close_virtconns()

        # The underlying library of Fabric, Paramiko, raises an error, on
        # destruction right after the disconnect function is called.  We are
        # sleeping for a little while to avoid this.
        time.sleep(0.1)


def configure_root_logger(silent, verbose):
    root_logger.addHandler(IGVMLogHandler())

    # We are summing up the silent and verbose arguments in here.  It
    # is not really meaningful to use them both, but giving an error is not
    # better.  See Python logging library documentation [1] for the levels.
    # Paramiko is overly verbose.  We configure it for one level higher.
    #
    # [1] https://docs.python.org/library/logging.html#logging-levels
    level = 20 + (silent - verbose) * 10
    root_logger.setLevel(level)
    root_logger.getChild('paramiko').setLevel(level + 10)
