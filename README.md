# Usage

The project consist of a Python library called "igvm", and a single executable
to call the functions in this library.

The library functions can be included like this:

```python
from igvm.commands import vm_build, vm_migrate
```

The functions are to be called like this:

```python
def vm_migrate(vm_hostname, hypervisor_hostname=None,
               run_puppet=False, debug_puppet=False,
               offline=False, offline_transport='drbd', ignore_reserved=False):
```

* Mandatory:
    * vm_hostname - string, hostname of virtual machine
    * hypervisor_hostname - string, hostname of destination hypervisor
* Optional:
    * run_puppet - boolean, run chrooted puppet after VM image is extracted
    * debug_puppet - boolean, run puppet with --debug
    * offline - boolean, allow offline migration, default is to attempt online
      migration and fail if it is impossible due to hypervisor of network
      configuration
    * offline_transport - choose between the fast `drbd` or the simple `netcat`
      offline transport methods
    * ignore_reserved - boolean, allow migration to an online_reserved
      hypervisor

```python
def vm_build(vm_hostname, run_puppet=True, debug_puppet=False, postboot=None,
             ignore_reserved=False):
```

* Mandatory:
    * vm_hostname - string, hostname of virtual machine
* Optional:
    * run_puppet - boolean, run chrooted puppet after VM image is extracted
    * debug_puppet - boolean, run puppet with --debug
    * postboot - extra command to run after machine is booted
    * ignore_reserved - boolean, allow build of VM on a online_reserved
      hypervisor

TODO: Document vcpu_set, mem_set, disk_set, vm_rebuild, vm_stop, vm_start,
vm_restart, vm_delete, vm_rename and vm_sync

# License

The project is released under the MIT License.  The MIT License is registered
with and approved by the Open Source Initiative [1].

[1] https://opensource.org/licenses/MIT
