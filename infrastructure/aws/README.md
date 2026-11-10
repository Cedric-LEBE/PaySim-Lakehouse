# AWS infrastructure adoption

Scope: data engineering and infrastructure only. Terraform now tracks the existing project resources; Databricks jobs and dashboards remain managed by DAB.

## Inventory before Terraform

Run from an authenticated AWS CLI session (CloudShell is suitable):

```bash
python3 scripts/aws-inventory.py
```

The collector only calls read APIs. It does not read credentials, download data objects, start/stop instances or modify permissions. Output defaults to ignored `data/infrastructure/inventory.json`; retain it privately. A failed call is recorded explicitly: an absent optional setting and an access denial must be distinguished during review. Do not grant administrator access just to run this collector.

Known references from the completed project (not a live inventory):

- Region: `eu-north-1`.
- Instance: `i-093fc1b9280b981f0`, Ubuntu, `m7i-flex.large`, encrypted 100 GiB gp3 root volume.
- Bucket: `paysim-lakehouse-968077439279-eu-north-1-an`.
- Dedicated roles: `PaySimLakehouseEC2Role`, `PaySimDatabricksReadRole`.
- Databricks reads `exports/` using Unity Catalog; file events are disabled.

## Adoption gates

1. Review current AMI, instance state, volume identity/encryption/deletion settings, network and complete IAM policies against the inventory.
2. Reference the shared/default VPC and unrelated resources as data sources; do not adopt another project's resources.
3. Write pinned Terraform/provider configuration and import declarations for the confirmed dedicated resources. Decide state storage and recovery before importing. Never commit state, saved plans or credentials.
4. Run formatting and validation, then a plan. Import declarations must not trigger resource creation, deletion, replacement or permission changes. Review differences before any apply.
5. Protect existing data-bearing resources from accidental destruction and document deliberate teardown separately. Preserve EC2/EBS and S3 data until backup and restore have been verified.

On 2026-11-10, the user applied the reviewed bootstrap plan (6 created) and adoption plan (12 imported, 0 added, 0 changed, 0 destroyed). Both subsequent plans reported `No changes`. The current SSH address did not respond during the initial inventory attempt; use the console to verify the current address and instance state rather than starting/recreating resources speculatively.

## Prepared adoption configuration (inventory received 2026-11-10)

`main.tf.json` uses native Terraform JSON syntax. It records this existing environment, not a generic reusable module. Terraform 1.17.x and AWS provider 6.72.0 are pinned; provider `allowed_account_ids` guards against the wrong AWS account. `imports.tf.json` declares 12 existing resources. Do not run this configuration as a new installation in another account.

The instance's root volume is managed through `aws_instance.root_block_device`; do not import it a second time as `aws_ebs_volume`. The existing key pair, AWS-managed KMS key, default VPC, subnet, route table and internet gateway are not adopted. No S3 objects are managed by Terraform. Existing IAM documents are preserved, including Unity Catalog's self-assumption and external ID condition. The EC2 role's managed policy has one attachment in the inventory; ownership by another project was not observed.

The missing S3 lifecycle, bucket policy and tags are confirmed absent (`NoSuchLifecycleConfiguration`, `NoSuchBucketPolicy`, `NoSuchTagSet`), not access denied. Versioning has never been enabled according to the supplied response. Encryption preserves AES256, bucket key enabled and SSE-C blocked. No new bucket policy, lifecycle or versioning setting is introduced during adoption.

Inventory findings requiring later operational decisions:

- EC2 was `running` at collection time; verify active processes before stopping it.
- SSH is restricted to `37.166.3.84/32`; if the user's public address changes, access can time out. No Kafka port is exposed by this group.
- The encrypted root volume is 100 GiB gp3 (3000 IOPS, 125 MiB/s); `delete_on_termination=true` is preserved. `prevent_destroy` protects planned destruction while the resource remains configured, but does not protect deletion in the AWS console or deletion of the Terraform resource block.
- The snapshot of configuration is not a backup of EBS data or a bill. Storage restore and actual costs still require separate checks.

## Validation and next gate

```bash
cd infrastructure/aws
terraform init -backend=false
terraform fmt -check
terraform validate
```

These commands initialize providers and validate configuration without adopting or changing AWS resources. After a successful initialization, commit the generated provider lock file so verified provider hashes remain under version control.

The durable state storage was established before adoption. The adoption configuration declares an S3 backend configured through `backend.hcl`; its bucket was created using the separate [bootstrap configuration](bootstrap/README.md). Do not scatter state across CloudShell, Mac and CI. Do not run an apply in ephemeral CloudShell storage. The separate private, versioned and encrypted state bucket with native S3 locking was created in the separate bootstrap step; do not reuse the data bucket's `exports/` path or grant Databricks access to state.

Once the state location is configured and AWS authentication is available:

```bash
terraform init -backend-config=backend.hcl
terraform plan -out=adoption.tfplan
terraform show adoption.tfplan
```

The acceptance target is **12 to import, 0 to add, 0 to change, 0 to destroy**. Provider-derived defaults may reveal differences: resolve them against AWS before applying. The live adoption plan met this target and was applied successfully on 2026-11-10. Applying imports writes state and is a separate step after review of this exact plan. Preserve the configured account, region, IDs and access restrictions.

References: [Terraform import workflow](https://developer.hashicorp.com/terraform/language/import/single-resource), [S3 encryption resource](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_server_side_encryption_configuration).

Local validation status: `terraform fmt -check` passed. Both adoption and bootstrap configurations passed `terraform validate` using the previously downloaded provider in isolated local directories. Provider lock files now include the pinned provider archive checksums. The subsequent user-provided apply and no-change plan confirm successful adoption.

## Delivery verification

Both configurations have an S3 backend: bootstrap uses `paysim/bootstrap/terraform.tfstate`, adoption uses `paysim/aws/terraform.tfstate`. The user ran the migration workflow; retain a check of the remote object version as a recovery record. Do not publish state or plan files. Provider lock files include the official AWS 6.72.0 archive hashes for cross-platform installation and the validated local package hash.

GitHub CI validates both configurations with `init -backend=false -lockfile=readonly`, formatting checks and `validate`, without AWS credentials or infrastructure changes. The manual Databricks deployment gate now requires all three CI jobs. Live drift detection, authenticated plans and apply are not automated yet.
