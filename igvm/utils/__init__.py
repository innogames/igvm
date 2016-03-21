import os
from pipes import quote
from functools import update_wrapper, partial

from fabric.api import open_shell, prompt, warn
from fabric.colors import red
from fabric.contrib.files import upload_template

import igvm

class ManageVMError(Exception):
    pass

def get_installdir():
    return os.path.dirname(igvm.__file__)

def cmd(cmd, *args, **kwargs):
    escaped_args = [quote(str(arg)) for arg in args]

    escaped_kwargs = {}
    for key, value in kwargs.iteritems():
        escaped_kwargs[key] = quote(str(value))

    return cmd.format(*escaped_args, **escaped_kwargs)
