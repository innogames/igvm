import typing
import logging

from hashlib import sha256
from igsg.aws_sg_permission import AwsSgPermission
from igsg.service_group import Direction, ServiceGroupsDict
from igsg.service_groups import ServiceGroups
from igvm.exceptions import HypervisorError
from igvm.utils import lock_file, unlock_file

if typing.TYPE_CHECKING:
    from mypy_boto3_ec2.service_resource import EC2ServiceResource, Vpc, SecurityGroup
    from mypy_boto3_ec2.type_defs import TagTypeDef
else:
    EC2ServiceResource = object
    SecurityGroup = object
    TagSpecificationTypeDef = object
    TagTypeDef = object
    Vpc = object


log = logging.getLogger(__name__)


def sync_consolidated_sg(
    ec2r: EC2ServiceResource,
    vpc: Vpc,
    sg_names: typing.Iterable[str],
) -> SecurityGroup:

    sgs = ServiceGroups(sg_names)
    verified_sa_sgs = sgs.verified_sa_sgs

    # Sort participating SGs as they must be identical on every run.
    sg_names = typing.cast(typing.Tuple[str], tuple(sorted(sg_names)))

    sg_name = (
        'consolidated-' +
        sha256((','.join(sg_names)).encode()).hexdigest()
    )

    # XXX: This will work only if all people using IGVM will run it on the
    # same control server. We must find a better way to lock AWS objects.
    lfd = lock_file(f'/tmp/{sg_name}')
    log.info('Obtaining lock for AWS SG sync')
    if not lfd:
        raise HypervisorError("Can't obtain lock for AWS SG sync!")
    sg = create_sg(ec2r, vpc, sg_name)
    sync_consolidated_aws_sg(sg_name, sg, sg_names, verified_sa_sgs) # type: ignore
    unlock_file(f'/tmp/{sg_name}', lfd)

    return sg

def create_sg(
    ec2r: EC2ServiceResource,
    vpc: Vpc,
    sg_name: str,
) -> SecurityGroup:
    log.info(f'Creating AWS SG {sg_name}')
    ret: typing.Optional[SecurityGroup] = None

    # Check if requested SG already exists in AWS
    # for sg in vpc.security_groups.filter(GroupNames=[sg_name]): # How old is boto3 on control-test!?
    for sg in ec2r.security_groups.filter(
            Filters=[
            {
                'Name': 'group-name',
                'Values': [sg_name],
            },
            {
                'Name': 'vpc-id',
                'Values': [vpc.vpc_id],
            },
        ],
    ):
        ret = sg

    if not ret:
        ret = vpc.create_security_group(
            Description='Automatically synchronized',
            GroupName=sg_name,
            # TagSpecifications=[ts], # How old is boto3 on control-test!?
            DryRun=False,
        )

    ret.create_tags(Tags=[set_name_tag(sg_name)])

    return ret


def sync_consolidated_aws_sg(
    sg_name: str,
    aws_sg: SecurityGroup,
    consolidated_sgs_names: typing.Tuple[str],
    verified_sa_sgs: ServiceGroupsDict,
) -> None:

    log.info(
        f'Synchronizing consolidated SG {sg_name}: {consolidated_sgs_names}'
    )
    for direction in Direction:
        current_permissions: typing.Set[AwsSgPermission] = set()
        wanted_permissions: typing.Set[AwsSgPermission] = set()

        current_permissions.update(get_aws_sg_permissions(
            aws_sg, sg_name, direction
        ))

        for sg_name in consolidated_sgs_names:
            sa_sg = verified_sa_sgs[sg_name]
            wanted_permissions.update(
                sa_sg.to_aws_permissions(direction)
            )

        wanted_permissions = AwsSgPermission.deduplicate(wanted_permissions)

        log.debug(f'Current {direction} permissions: {current_permissions}')
        log.debug(f'Wanted {direction} permissions: {wanted_permissions}')

        for permission in current_permissions - wanted_permissions:
            permission.revoke(aws_sg, direction)

        for permission in wanted_permissions - current_permissions:
            permission.authorize(aws_sg, direction)



def get_aws_sg_permissions(
    aws_sg: SecurityGroup, sg_name: str, direction: Direction,
) -> typing.Set[AwsSgPermission]:
    log.debug(f'Reading AWS SG {direction} {sg_name}')
    ret: typing.Set[AwsSgPermission] = set()

    if direction == Direction.ingress:
        ip_permissions = aws_sg.ip_permissions
    else:
        ip_permissions = aws_sg.ip_permissions_egress

    # A single permission might have multiple IP address ranges.
    # We group them by description, as AWS would group them all into a
    # single permission for us.
    for permission in ip_permissions:
        log.debug(f'Current {direction} AWS permissions: {permission}')
        for awssgp in AwsSgPermission.from_aws_sg(permission):
            log.debug(f'Current {direction} obj permission: {awssgp}')
            ret.add(awssgp)

    return ret


def get_tag_obj(y: object, tag: str) ->  typing.Optional[str]:
    ret = [x['Value'] for x in y.tags if x['Key'] == tag] # type: ignore
    if len(ret): # type: ignore
        return ret[0] # type: ignore
    return None


def set_name_tag(name: str) -> TagTypeDef:
    return {
        'Key': 'Name',
        'Value': name,
    }
