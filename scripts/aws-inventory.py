"""Collect AWS configuration through read-only CLI calls; never collect credentials."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--region', default='eu-north-1')
    parser.add_argument('--instance-id', default='i-093fc1b9280b981f0')
    parser.add_argument('--bucket', default='paysim-lakehouse-968077439279-eu-north-1-an')
    parser.add_argument('--output', type=Path, default=Path('data/infrastructure/inventory.json'))
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists; choose a new file to preserve the previous inventory.')
    results = {}

    def read(name, *command):
        result = subprocess.run(['aws', *command, '--region', args.region, '--output', 'json', '--no-cli-pager'], capture_output=True, text=True, timeout=90)
        if result.returncode:
            results[name] = {'ok': False, 'error': result.stderr.strip()}
            return None
        value = json.loads(result.stdout or '{}')
        results[name] = {'ok': True, 'value': value}
        return value

    identity = read('identity', 'sts', 'get-caller-identity')
    if not identity:
        raise SystemExit('AWS identity unavailable; authenticate before collecting configuration.')
    instances = read('instances', 'ec2', 'describe-instances', '--instance-ids', args.instance_id)
    read('volumes', 'ec2', 'describe-volumes', '--filters', 'Name=attachment.instance-id,Values=' + args.instance_id)
    if instances:
        for reservation in instances['Reservations']:
            for instance in reservation['Instances']:
                read('subnet', 'ec2', 'describe-subnets', '--subnet-ids', instance['SubnetId'])
                read('vpc', 'ec2', 'describe-vpcs', '--vpc-ids', instance['VpcId'])
                read('routes', 'ec2', 'describe-route-tables', '--filters', 'Name=vpc-id,Values=' + instance['VpcId'])
                read('security_groups', 'ec2', 'describe-security-groups', '--group-ids', *[g['GroupId'] for g in instance['SecurityGroups']])
                profile = instance.get('IamInstanceProfile', {}).get('Arn')
                if profile:
                    read('instance_profile', 'iam', 'get-instance-profile', '--instance-profile-name', profile.rsplit('/', 1)[-1])
    for operation in ['get-bucket-location', 'get-bucket-encryption', 'get-public-access-block', 'get-bucket-ownership-controls', 'get-bucket-versioning', 'get-bucket-lifecycle-configuration', 'get-bucket-policy', 'get-bucket-tagging']:
        read(operation, 's3api', operation, '--bucket', args.bucket)
    for role in ['PaySimLakehouseEC2Role', 'PaySimDatabricksReadRole']:
        read(role, 'iam', 'get-role', '--role-name', role)
        attached = read(role + '_attached', 'iam', 'list-attached-role-policies', '--role-name', role)
        inline = read(role + '_inline', 'iam', 'list-role-policies', '--role-name', role)
        if inline:
            for name in inline['PolicyNames']:
                read(role + '_inline_' + name, 'iam', 'get-role-policy', '--role-name', role, '--policy-name', name)
        if attached:
            for policy in attached['AttachedPolicies']:
                name, arn = policy['PolicyName'], policy['PolicyArn']
                metadata = read(role + '_policy_' + name, 'iam', 'get-policy', '--policy-arn', arn)
                if metadata:
                    read(role + '_document_' + name, 'iam', 'get-policy-version', '--policy-arn', arn, '--version-id', metadata['Policy']['DefaultVersionId'])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as output:
        json.dump({'region': args.region, 'instance_id': args.instance_id, 'bucket': args.bucket, 'results': results}, output, indent=2)
    failed = [name for name, result in results.items() if not result['ok']]
    print('Inventory saved:', args.output)
    print('Review missing/denied/absent configuration:', ', '.join(failed) or 'none')
    print('No infrastructure was modified. Do not commit this local inventory.')


if __name__ == '__main__':
    main()
