# Usage

The Python library is called managevm. There are also two scripts you can use from command line: buildvm and migratevm. Scipts are installed in /usr/bin and Python library somewhere in PYTHONPATH. Just import the functions like this:

```python
from managevm.buildvm import buildvm
from managevm.migratevm import migratevm
```

The functions are to be called like this:

```python
migratevm(vm_hostname, dsthv_hostname, newip=None, nopuppet=False, nolbdowntime=False, offline=False)
```

* Mandatory:
 * vm_hostname - string, hostname of virtual machine
 * dsthv_hostname - string, hostname of destination hypervisor
* Optional:
 * newip - string, new IP address if you migrate to different segment
 * nopuppet - boolean, disable running chrooted puppet after VM image is extracted
 * nolbdowntime - boolean, don't downtime LB Pool for time of migration
 * offline - boolean, allow offline migration, default is to attempt online migration and fail if it is impossible due to hypervisor of network configuration

```python
buildvm(vm_hostname, image=None, nopuppet=False, postboot=None)
```

* Mandatory:
 * vm_hostname - string, hostname of virtual machine
* Optional:
 * image - image to use as base for VM, if no image is given, one based on os Admintool parameter will be used
 * nopuppet - boolean, disable running chrooted puppet after VM image is extracted
 * postboot - extra command to run after machine is booted

# Developement

1. Clone this repo to your local workspace.
2. Create your own branch.
 1. Write the code.
 2. In order to test the code, run it the following way:
    `python setup.py install --user && bin/(buildvm|migratevm) --params and commands`
 3. Commit to your branch.
 4. Go to 2.1. until you are finished.
3. Checkout master branch again.
4. Merge your developement branch.
5. Push to remote master branch.
6. Jenkins notifies the change after a minute or so and builds .deb package
   and puts it into repositories. Version is always 1.0-${BUILD_NUMBER}.
7. Wait for puppet to upgrade package on control servers or do it manually
