# Backup and restore runbook

The immutable star export in S3 is an analytics delivery artifact. It does not include all ingestion tables, Kafka storage and recovery checkpoints. An EBS snapshot preserves the host filesystem state separately.

## Scope and prerequisites

This deployment uses one encrypted 100 GiB gp3 root volume. Keep the snapshot private and encrypted: a full host backup may also contain cached authentication material, configuration and personal data. Do not publish an AMI or share the snapshot for the portfolio.

Before backup, stop new ingestion/publication and any direct Spark writers. Run the local health command from the EC2 repository and stop Kafka. Verify that no other process is writing the lakehouse; the health command alone is not proof that every writer has stopped. Stop the EC2 instance normally and confirm `Stopped` before snapshotting the root volume.

In the AWS console, use the instance's current Storage tab to identify its root volume. Create an EBS snapshot, record the source volume, time, region and snapshot ID in a private operations record, and wait for `Completed`. Preserve the original volume. Fast Snapshot Restore and provisioned initialization rates are unnecessary for this drill.

## Restore to a separate volume

1. Create a gp3 volume from the completed snapshot in the **same Availability Zone as the inspection instance**. An EBS snapshot is regional; the restored volume must match the attachment zone. Keep the original size and baseline IOPS/throughput.
2. Attach it as a secondary disk; never replace or format the original root disk. Record both volume IDs before starting the instance.
3. Inspect `lsblk -o NAME,SIZE,FSTYPE,MOUNTPOINTS,SERIAL`. Match each NVMe serial to its EBS volume ID. NVMe numbering can change; do not assume `nvme1n1` always means the restore.
4. Check `findmnt /`, `findmnt /boot` and `findmnt /boot/efi`. Cloned labels/UUIDs can cause the restored boot partitions to mount automatically. Stop package updates and resolve those mounts before removing any disk. Unmount EFI before boot, then explicitly mount the verified original boot partition and original EFI partition. Preserve mount options from `/etc/fstab` (including EFI `umask=0077`). Do not change filesystem identifiers or `/etc/fstab` just to perform the read test.
5. Mount only the verified restored ext4 data partition at a separate directory using `mount -t ext4 -o ro,noload <VERIFIED_PARTITION> /mnt/paysim-restore`. Confirm `ro` and `noload`/`norecovery` in `findmnt`. If mounting fails, investigate without running repair tools on the backup.

These steps are deliberately identity-based. Never paste a previous session's `/dev/nvme...` name without inspecting the current device serials.

## Validation performed on 2026-11-16

Read the restored CSV and compare its SHA-256 with the source fingerprint. Parse the restored replay cursor and star manifest: no pending interval, matching snapshot, expected validated row count. Use their pinned Delta versions to read **the restored mount paths**, not the absolute original paths stored inside the manifest.

A Spark read scanned all columns of each restored star table and compared its count with the saved manifest. Use the active repository's dependencies with temporary work directories on the original disk; do not start Kafka or write into the restored mount. The four tables passed. See the [validation record and explicit limits](validation-record.md#backup-and-restore-drill).

## Cleanup and shutdown

After all readers stop, leave any restored working directory and unmount the restored data partition normally. Confirm root, boot and EFI are on the original volume. If unmount reports busy, diagnose open files/processes; do not force detach.

Detach only the recorded temporary restored volume. Once it is `Available` and unattached, delete that temporary volume. Remove any accidental extra restore volume only after verifying its ID, zone and snapshot source. Preserve the original root volume and completed backup snapshot. Do not reboot while duplicate-label disks remain attached if mount selection has not been resolved.

Stop EC2 when unused. EBS volumes and retained snapshots remain billable while EC2 is stopped. Confirm temporary-resource deletion in the console; an instruction to delete is not evidence of deletion.

## Retention, cost and remaining guarantees

For this portfolio exercise, keep one known-good snapshot until the delivery and recovery evidence have been reviewed. Set a review date; do not configure automatic expiration or delete the only recovery point without a replacement policy. The original root volume currently has delete-on-termination enabled; stopping and terminating are materially different operations.

No measured final AWS bill or recovery time objective is available. Review Billing by service and region for EC2, EBS/snapshots, S3 requests/storage and public IPv4; credits offset charges but do not establish zero resource cost. Capture totals and the billing period privately before publishing redacted evidence.

A future full failover must validate a replacement host and the absolute-path bindings in checkpoints, source markers and incremental state. Do not edit those files to force a resume. This drill validates readable restored star data and metadata, not an executed end-to-end ingestion recovery.

References: [EBS snapshots](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-creating-snapshot.html), [EBS NVMe device identification](https://docs.aws.amazon.com/ebs/latest/userguide/identify-nvme-ebs-device.html).
