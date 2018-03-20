# Usage

The project consist of a Python library called "igvm", and a single binary
to call the functions in this library.  The binary is "bin/igvm".

The library functions can be included like this:

```python
from igvm.commands import vm_build, vm_migrate
```

The functions are to be called like this:

```python
vm_migrate(vm_hostname, dsthv_hostname, newip=None, nopuppet=False, maintenance=False, offline=False, ignore_reserved=False)
```

* Mandatory:
 * vm_hostname - string, hostname of virtual machine
 * hypervisor_hostname - string, hostname of destination hypervisor
* Optional:
 * newip - string, new IP address if you migrate to different vlan
 * nopuppet - boolean, disable running chrooted puppet after VM image is extracted
 * maintenance - boolean, set VM to maintenance state, thus downtiming it in Testtool and Nagios
 * offline - boolean, allow offline migration, default is to attempt online migration and fail if it is impossible due to hypervisor of network configuration, also implies --maintenance
 * ignore_reserved - boolean, allow migration to an online_reserved hypervisor

```python
vm_build(vm_hostname, localimage=None, nopuppet=False, postboot=None, ignore_reserved=False)
```

* Mandatory:
 * vm_hostname - string, hostname of virtual machine
* Optional:
 * localimage - image on filesystem of HV to use as base for VM, if no image is given, one based on os Admintool parameter will be used
 * nopuppet - boolean, disable running chrooted puppet after VM image is extracted
 * postboot - extra command to run after machine is booted
 * ignore_reserved - boolean, allow build of VM on a online_reserved hypervisor

# Development

1. Clone this repo to your local workspace.
2. Create your own branch.
1. Write the code.
2. In order to test the code, you can use `run.py`
3. Commit to your branch.
4. Go to 2.1. until you are finished.
3. Checkout master branch again.
4. Merge your development branch.
5. Push to remote master branch.
6. Jenkins notifies the change after a minute or so and builds .deb package
   and puts it into repositories. Version is always 1.0-${BUILD_NUMBER}.
7. Wait for puppet to upgrade package on control servers or do it manually

# License

The project is released under the MIT License.  The MIT License is registered
with and approved by the Open Source Initiative [1].

[1] https://opensource.org/licenses/MIT
