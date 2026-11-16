# Validation record

Evidence below comes from repository checks and execution output supplied by the project operator. Cloud results are point-in-time observations, not continuous monitoring. Last recorded validation: **2026-11-16**.

## Complete dataset

The replay cursor reached row **6,362,620**, with no pending interval. The final batch report passed. The verified export `20261109T205841Z-8ed6f88b` was uploaded to S3 and imported into managed Databricks Delta tables. Published dashboard screenshots showed 6,354,407 non-fraud transactions and 8,213 fraud-labeled transactions.

| Metric | Result |
| --- | ---: |
| Facts | 6,362,620 |
| Fraud-labeled transactions | 8,213 |
| Total amount | 1,144,392,944,759.77 |
| Fraud amount | 12,056,415,427.84 |
| Simulation time dimension | 743 members |
| Account dimension | 9,073,900 members |

Amounts have no assigned currency. The account dimension retains historical members. This project analyses simulated labels; it does not predict fraud.

## Full snapshot configuration

Historical bundle defaults are not the full dataset. For a full-snapshot deployment, explicitly select:

```text
catalog=workspace
schema=paysim_20261109t205841z_8ed6f88b
export_path=s3://paysim-lakehouse-968077439279-eu-north-1-an/exports/20261109T205841Z-8ed6f88b/
s3_volume=workspace.paysim_uploads.s3_exports
```

Supply an existing warehouse ID and authenticated profile separately. Do not use the publication-test schema as the dashboard source.

## Live publication and retry

On 2026-11-16, the runner published the isolated `publication-test-20261116T181351Z` export containing 10,000 facts. Databricks returned `TERMINATED` / `SUCCESS`; a second invocation returned the same run ID `742170842340643` and `complete` status.

Manifest SHA-256: `84795cade16b47d9b6f3b40f4d4a4d4f89c8273089b1a35ce2019f85c85ee1ea`.

This exercised local validation, S3 publication, job submission, monitoring and same-run reuse. It did not repoint the full dashboard. A live lost-response or remote-failure drill was not performed; those paths are tested with mocks.

## Backup and restore drill

The operator stopped Kafka after the local health check reported the complete dataset. A snapshot of the root EBS volume was created and reported `Completed`. A separate volume restored from that snapshot was attached to the existing instance.

The restored root partition was mounted read-only with ext4 journal replay disabled (`ro,noload`, displayed as `norecovery`). Checks passed:

- CSV SHA-256 matched `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`.
- Replay cursor: 6,362,620, no pending interval; its stored snapshot equalled the star manifest.
- Silver reference version: 129. Star versions: type 0, time 127, account 127, fact 127.
- Spark scanned all columns of the four restored star tables at their pinned versions; row counts matched the manifest. Final message: `PASS: quatre tables Delta restaurées lisibles, effectifs conformes.` Spark exited normally.

Duplicate filesystem labels/UUIDs caused the restored boot partitions to mount automatically at `/boot` and `/boot/efi`. They were unmounted and the original volume partitions explicitly remounted. The restored data partition was subsequently unmounted; original root/boot mounts were confirmed before cleanup instructions.

This is a **star-data read-recovery test**, not proof of a complete disaster-recovery failover. It did not boot a replacement instance, replay Kafka from restored checkpoints, scan every Bronze/Silver/Gold file, rerun all key/amount checks, or establish RPO/RTO. Cleanup of the temporary volumes still requires operator confirmation. See the [recovery runbook](recovery.md).

## Performance and infrastructure

The [baseline](performance/ec2-baseline.md) uses 128 passed reports: 96,207.10 seconds summed across campaigns, excluding pauses. Peak memory, billed cost and optimized speedup were not measured by those reports.

Terraform adoption and bootstrap plans reported no changes after import/application. CI validates configuration without cloud credentials; it does not continuously detect live AWS drift. See [AWS infrastructure](../infrastructure/aws/README.md).

### Adoption plan rechecked on 2026-11-16

After CloudShell temporary provider/backend metadata was reinitialized against the existing S3 backend, the state listed 12 managed resources and two data sources. A normal plan detected the stale declared SSH CIDR. The configuration was aligned with the existing AWS rule; the subsequent plan reported `No changes. Your infrastructure matches the configuration.` No infrastructure apply was performed for this correction. The repository configuration received the same CIDR update.
