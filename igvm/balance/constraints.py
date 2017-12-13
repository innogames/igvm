from threading import Thread


class Constraint(Thread):
    """Base Constraint Class

    This class is the base constraints class from which all concrete
    constraints should inherit to fulfil the base contract of how to access it.
    Within all classes the inherit from Constraint you have access to the
    following standard attributes as well as all key, values as attributes you
    attached in configuration.
    """

    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        self.result = None

        for key, value in kwargs.items():
            setattr(self, key, value)

    def fulfilled(self, vm, hv):
        """Constraint is fulfilled

        Return True if constraint is fulfilled or False if not.

        :param: vm: igvm.balance.models.VM object
        :param: hv: igvm.balance.models.Hypervisor object

        :return: bool
        """

        raise NotImplementedError()

    def run(self):
        """Wrapper for threading support"""

        self.result = self.fulfilled(self.vm, self.hv)


class DiskSpace(Constraint):
    """Disk Space Constraint

    Check if enough disk space is free on target hypervisor. Disk space is
    determined by used disk space of VM.
    """

    def __init__(self, *args, **kwargs):
        super(DiskSpace, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if enough disk space is available on target hypervisor

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """

        hv_disk_free = hv.get_disk_free(fast=True)
        vm_disk_size = vm.get_disk_size()

        if hv_disk_free - self.reserved <= vm_disk_size:
            return False

        return True


class Memory(Constraint):
    """Memory Constraint

    Check if hypervisor has enough memory free to move desired vm there
    """

    def __init__(self, *args, **kwargs):
        super(Memory, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if enough memory is available on target hypervisor

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """
        hv_memory_free = hv.get_memory_free()
        vm_memory_needed = vm.get_memory()

        if hv_memory_free <= vm_memory_needed:
            return False

        return True


class RouteNetwork(Constraint):
    """Route Network

    Check if target hypervisor is in same route_network
    """

    def __init__(self, *args, **kwargs):
        super(RouteNetwork, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if target hypervisor support route network

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """
        vm_rn = vm.get_serveradmin_data()['route_network']
        hv_rns = hv.get_serveradmin_data()['vlan_networks']

        if vm_rn in hv_rns:
            return True

        return False


class Bladecenter(Constraint):
    """Bladecenter

    Check if target is in the same bladecenter and return True if yes.
    """

    def __init__(self, *args, **kwargs):
        super(Bladecenter, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if target hypervisor is in same bladecenter as VM

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """
        if vm.get_bladecenter() == hv.get_bladecenter():
            return True

        return False


class EnsureFunctionDistribution(Constraint):
    """Game World / Function Distribution

    Ensure that redundant servers don't reside on the same hypervisor
    """

    def __init__(self, *args, **kwargs):
        super(EnsureFunctionDistribution, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if target hypervisor constrains a VM of the game (world)

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """
        for hv_vm in hv.get_vms():
            if hv_vm.get_identifier() == vm.get_identifier():
                if hv_vm.hostname != vm.hostname:
                    return False

        return True


class GameMasterDbDistribution(Constraint):
    """Game Master DB Distribution

    Check if there is already a game master from the same game or another on
    the hypervisor and return True if NOT.
    """

    def __init__(self, *args, **kwargs):
        super(GameMasterDbDistribution, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if target hypervisor contains a master db of that game

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """

        # If we don't try to migrate a master db we can safely ingore further
        # evaluation.
        if vm.get_world() != 0 and vm.get_function() not in self.db_types:
            return True

        for hv_vm in hv.get_vms():
            if (
                hv_vm.get_world() == 0 and
                hv_vm.get_function() in self.db_types
            ):
                return False

        return True


class HypervisorMaxVcpuUsage(Constraint):
    """Hypervisor Max vCPU usage

    Checks the maximum vCPU usage (95 percentile) of the given hypervisor for
    the given time_range and dismisses it as target when it is over the value
    of threshold.
    """

    def __init__(self, *args, **kwargs):
        super(HypervisorMaxVcpuUsage, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if 95% of hypervisor CPU usage is above threshold

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """

        max_usage = hv.get_max_vcpu_usage()

        if max_usage == -1.0:
            return False

        if max_usage >= self.threshold:
            return False

        return True


class ServeradminAttribute(Constraint):
    """Serveradmin attribute has value

    Check if the serveradmin attribute for the target hypervisor has the given
    value specified in configuration. You specify the serveradmin attribute and
    value as "serveradmin_attribute" and "serveradmin_value" in config.
    """

    def __init__(self, *args, **kwargs):
        super(ServeradminAttribute, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        """Check if serveradmin attribute has given value

        :param vm: igvm.balance.models.VM object
        :param hv: igvm.balance.models.Hypervisor object

        :return: bool
        """
        key = self.serveradmin_attribute
        value = self.serveradmin_value

        if key in hv.get_serveradmin_data().keys():
            if hv.get_serveradmin_data()[key] == value:
                return True

        return False
