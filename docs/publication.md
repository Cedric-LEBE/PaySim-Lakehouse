# Resumable S3 → Databricks publication

The existing ingestion and validated star export are reused. `banking.publication` starts from an already verified export directory; it does not run Kafka, rebuild Delta tables, recreate an export, deploy a bundle or repoint/publish dashboards. Export generation must still occur separately after the pipeline has stopped writing.

## Plan without remote operations

On EC2 in the repository, after `git pull --ff-only`:

```bash
PYTHONPATH=src .venv/bin/python -m banking.publication \
  --export data/exports/20261109T205841Z-8ed6f88b
```

This default path through the command checks the manifest, the four exact filenames, byte lengths and local SHA-256 values, then prints its destination/job plan. No AWS or Databricks command is executed without `--execute`. The existing complete export is reused; no CSV replay is required.

## Execution prerequisites

Use a host where both the existing export and authenticated AWS CLI v2 and Databricks CLI are available. EC2 has its existing instance role for S3; do not add static AWS keys. Databricks authentication must be configured independently for the stated workspace. OAuth CLI authentication on EC2 was validated on 2026-11-16 using the `paysim-ec2` profile. Do not copy a personal `.databrickscfg` or paste tokens into commands/logs as a shortcut. The runner checks Databricks CLI authentication before writing any S3 object. CLI 1.24.0 and the existing import job are the project's current baseline.

Execution is explicit by adding `--execute`. It can perform S3 writes and launch a Databricks run, using the job's existing compute/time limits and quotas. A live 10,000-row execution and a retry were validated on 2026-11-16; both reported the same successful run. See the [validation record](validation-record.md). For the already imported full dataset, execution is unnecessary for checking its plan.

## Steps and guarantees

1. Validate local manifest/file hashes before remote calls.
2. List only the immutable `exports/<export_id>/` prefix. Read existing objects into a temporary file one at a time and compare contents to the local export. This can download about 2 GiB for the complete existing snapshot; allow temporary disk space at least as large as the biggest file. These reads and any uploads consume AWS requests/network resources. No speedup claim is made.
3. Refuse any content conflict or unexpected objects. If a published manifest is missing one of its files, stop instead of repairing it automatically. Upload missing files using conditional `put-object --if-none-match '*'`. Upload `manifest.json` last. A concurrent writer causes an error; rerun to inspect the resulting objects, without overwriting them. IAM can still allow other tools to overwrite objects; this is a runner guarantee, not S3 Object Lock.
4. Verify the selected job has the expected single notebook task. Trigger the existing job with all four notebook parameters: export_path, catalog, s3_volume, target_schema. The notebook checks the hashes/schema/keys/totals and resumes equivalent existing tables without overwriting them.
5. Persist the run ID and poll it. A deterministic idempotency token binds the manifest, workspace, job and destinations: retrying submission after a lost response retrieves the same run. A confirmed failed run is not automatically retriggered. Investigate/repair it explicitly; rerunning this command monitors the same ID. Never remove state just to force a new run.
6. `complete` requires Databricks `TERMINATED` + `SUCCESS`. A monitor timeout leaves the remote run active; the next execution reconnects to it. Interrupting this runner does not cancel the remote job. Any other terminal result is a failure.

## Supervision and recovery

Local state lives in ignored `data/publications/<export_id>.json`; an export-specific flock prevents concurrent local controllers for the same export. Identity mismatch blocks reuse of state. Writes are atomic and include a UTC update time. The state records the phase, remote run state, job URL and failure type, without storing credentials. Do not edit it manually. The per-export lock does not coordinate different machines; immutable S3 objects and Databricks idempotency provide additional protection for the same identity.

The local `banking.health` command remains limited to the ingestion cursor and star manifest; it does not claim that remote publication succeeded. Publication reports are separate and require this runner's remote checks. Saved publication status is a historical observation, not a continuous availability check.

Mocked unit tests cover content mismatches, manifest ordering, conditional writes, refusal of partial published exports, existing-run reuse, parameter/idempotency submission and wrong-job refusal. The separate live test validates the successful publication and same-run retry path; injected remote failures remain covered by mocked tests rather than a live outage drill. The full production dataset is not republished by CI.

References: [S3 conditional object writes](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object.html), [Databricks run-now and idempotency](https://docs.databricks.com/api/jobs/v2/run-now), [run status](https://docs.databricks.com/api/jobs/v2/get-run).
