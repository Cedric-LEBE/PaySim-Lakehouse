# Bootstrap the Terraform state bucket

This separate configuration creates only a state bucket and five security/versioning settings. It does not import or modify the PaySim data infrastructure. The proposed globally unique bucket name is `paysim-tfstate-968077439279-eu-north-1`; availability and ownership must be checked by an authenticated plan before creation.

Properties: private access, ACLs disabled, versioning enabled, SSE-S3 encryption, SSE-C blocked, HTTPS enforced and native S3 state locking for clients. No EC2/Databricks role is granted access. S3 storage and request charges apply; no DynamoDB table, NAT gateway or customer-managed KMS key is added.

## First creation

Use a durable working directory and an authenticated operator identity. Do not use ephemeral CloudShell storage as the only copy of initial bootstrap state.

```bash
terraform init
terraform validate
terraform plan -out=bootstrap.tfplan
terraform show bootstrap.tfplan
```

Expected plan: **6 to add, 0 to change, 0 to destroy**, restricted to the new state bucket and its settings. Review the exact plan before `terraform apply bootstrap.tfplan`. The user applied this reviewed plan on 2026-11-10: 6 added, 0 changed, 0 destroyed. If the bucket already exists, stop and inspect ownership/configuration rather than attempting an alternative creation or overwrite.

## Migrate bootstrap state after creation

The initial local state contains the newly created bucket resources. Immediately migrate it to a separate key inside that bucket:

```bash
# backend.tf is already versioned; keep it present after migration
terraform init -migrate-state -backend-config=backend.hcl
terraform state list
terraform plan
```

Confirm the migration prompt only after checking the bucket and key. Expected result: six resources and no changes. Preserve the local state/backup securely until remote state and versioning are verified. Never commit state, saved plans or credentials. Do not run bootstrap concurrently during migration. The local bootstrap directory must keep `backend.tf` after migration, otherwise backend initialization will request another migration.

## Initialize PaySim adoption separately

From the parent directory, use `terraform init -backend-config=backend.hcl` and inspect the plan. Its key `paysim/aws/terraform.tfstate` differs from bootstrap's `paysim/bootstrap/terraform.tfstate`. The acceptance target is 12 imports and zero additions, changes or destructions.

## State access and recovery

The operator needs bucket listing and GetObject/PutObject on the applicable state key, plus GetObject/PutObject/DeleteObject on its `.tflock` key. Initial provisioning also requires the bucket configuration actions. Terraform infrastructure management needs its own resource permissions; do not attach state access to pipeline runtime roles. Credentials stay in the AWS authentication chain, never backend files.

For recovery, stop all Terraform writers, inspect S3 object versions and restore a verified earlier state version deliberately. Keep bucket versioning enabled. A state restore does not restore EC2 files or PaySim data. Never delete an active lock to bypass a running writer; investigate ownership and process state first.

Local validation: `terraform validate` passed for both configurations. The user subsequently created the bucket, ran the migration workflow and reported six state resources with a no-change plan.

Reference: https://developer.hashicorp.com/terraform/language/backend/s3
