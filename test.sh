#!/bin/bash
# Test script to validate core functionality of igvm.
set -e

IP1="10.20.6.16"
IP2="10.20.6.21"
VM=stelter-hv55-1-test
HV1=aw-hv-055
HV2=aw-hv-057

function test_vm {
    IP=$1
    HV=$2

    # Wait some time, VM doesn't always boot quickly enough
    echo Waiting for VM to boot up...
    sleep 10

    ping -c 4 $IP || (echo "$IP unreachable?!" >&2 ; exit 1)
    ssh -o ConnectTimeout=4 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $IP "echo Hello from VM."
    echo "VM on $IP works."

    ssh $HV "virsh list | grep -E ' $VM +running'" >/dev/null || (echo "$VM not running on $HV" >&2; exit 1)
    echo "VM is running on $HV."
}

function test_absent {
    HV=$1
    ssh $HV "test ! -b /dev/xen-data/$VM" || (echo "LV still defined on $HV!" >&2; exit 1)
    ssh $HV "virsh list --all | grep $VM && exit 1 || exit 0" || (echo "VM still defined on $HV!" >&2; exit 1)
}

function cleanup {
    ssh $HV1 "virsh destroy $VM ; virsh undefine $VM ; umount /dev/xen-data/$VM ; lvremove -f /dev/xen-data/$VM" || true
    ssh $HV2 "virsh destroy $VM ; virsh undefine $VM ; umount /dev/xen-data/$VM ; lvremove -f /dev/xen-data/$VM" || true

    test -z "$(adminapi_query "intern_ip=$IP1" | grep -v $VM)" || (echo $IP1 already in use!; exit 1)
    test -z "$(adminapi_query "intern_ip=$IP2" | grep -v $VM)" || (echo $IP2 already in use!; exit 1)

    python -c 'import adminapi; adminapi.auth(); from igvm.host import get_server; srv = get_server("'"$VM"'"); srv["intern_ip"] = "'"$IP1"'"; srv["xen_host"] = "'"$HV1"'"; srv.commit();'
    echo $VM reset to ${IP1}@${HV1}
}

cleanup

# buildvm
./bin/igvm create $VM
test_vm $IP1 $HV1
echo "VM building works."

# Online migration
./bin/igvm migrate $VM $HV2
test_vm $IP1 $HV2
test_absent $HV1
echo "Online migration works."

# Offline migration
./bin/igvm migrate --offline $VM $HV1
test_vm $IP1 $HV1
test_absent $HV2
echo "Offline migration works."


# Offline migration with new IP
./bin/igvm migrate --offline --runpuppet --newip $IP2 $VM $HV2
test_vm $IP2 $HV2
test_absent $HV1
echo "Offline migration with new IP works."

# buildvm with local image
cleanup
./bin/igvm create --localimage /root/jessie-localimage.tar.gz $VM
test_vm $IP1 $HV1
ssh -o ConnectTimeout=4 -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $IP "md5sum /root/local_image_canary | grep df60e346faccb1afa04b50eea3c1a87c" || (echo "Local image canary broken" >&2; exit 1)
echo "Local image works."

# All done.
cleanup

echo "igvm seems functional."

