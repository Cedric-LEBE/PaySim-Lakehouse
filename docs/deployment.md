# Manual GitHub deployment — prepared, not activated

[Back to README](../README.md)

## Current status and Free Edition limitation

The local `dev` deployment works with interactive Databricks CLI authentication. The manual [deploy workflow](../.github/workflows/deploy.yml) is prepared for GitHub-hosted runners using workload identity federation (OIDC), but **GitHub-to-Databricks authentication is not activated or tested end to end**.

Databricks' documented GitHub federation setup requires a service principal and an account-level federation policy. Free Edition does not expose the account console or account-level APIs. This blocks configuring that standard route with the current Free Edition account. No service principal, federation policy, GitHub secret, new account or paid resource has been created. Do not enable the workflow flag to bypass this prerequisite; it cannot grant missing access.

Continue deploying locally with the existing `dev` profile while deciding whether to use a supported Databricks account later. The workflow deliberately fails with an explanation when configuration is incomplete. It never exports local OAuth credentials, copies `.databrickscfg`, falls back to a personal token or installs a runner on the personal computer.

## Workflow behavior

1. Only **Run workflow** on `main` is accepted. Push and pull-request events do not deploy.
2. An explicit configuration check runs before Databricks authentication.
3. The gate checks that the selected SHA is still current `main`, waits up to 15 minutes for the latest push-triggered `ci.yml` run for that exact SHA, and requires both named jobs to succeed. Failed, cancelled, skipped or different-commit results do not qualify.
4. The deploy job uses the `databricks-ci` GitHub environment and requests a short-lived GitHub OIDC token. Only this job has `id-token: write`.
5. The pinned Databricks CLI 1.24.0 verifies the generated dashboard and strictly validates target `ci`.
6. Main and CI are rechecked immediately before `bundle deploy`. A newer main commit blocks the old deployment.
7. Only dashboard and job definitions are deployed. There is no `bundle run`, job execution, table import, dashboard publication or warehouse creation in this workflow.

Deployments are serialized rather than cancelling an active deployment. The initial gate has an 18-minute timeout, the deploy job a 15-minute timeout. GitHub access is limited to reading repository contents and Actions results.

## Isolated target

`dev` retains the existing interactive deployment. The new `ci` target has its own state under:

```text
/Workspace/Users/<deployment-identity>/.bundle/paysim-lakehouse/ci
```

Its resources use the `[ci]` prefix. It uses bundle `production` mode for a stable deployment configuration; this is still a portfolio CI environment, not a claim of production banking readiness. The root remains in the deployment identity's own folder, not a folder writable by all workspace users.

The first future CI deployment will create an isolated dashboard and job; it will **not take ownership of or replace the current `dev` resources**. Reuse the same deployment principal thereafter. Moving existing resources between identities or targets requires an explicit state/ownership migration and is outside this setup.

## Configuration for a supported account

Before activation, an account administrator must configure a Databricks service principal, assign it to the destination workspace, and create a federation policy restricted to **this repository and the `databricks-ci` environment**. Use the actual GitHub OIDC `sub` claim: newer repositories can include immutable owner/repository IDs. Do not copy a guessed name-only subject from an old example. The audience must match the Databricks account's configuration.

Grant only the workspace and data access needed to deploy these resources: the identity's deployment folder, the existing warehouse where applicable, and the appropriate catalog/schema permissions. Running the import job later requires separate access to the export volume and target tables. Never substitute workspace-wide administrator access as a shortcut.

Create the GitHub environment `databricks-ci`, restrict its deployment branches to `main`, and configure a reviewer if desired and supported. Set the following **repository variables** under Settings → Secrets and variables → Actions → Variables. They are identifiers/configuration, not authentication secrets:

| Variable | Purpose |
| --- | --- |
| `DATABRICKS_CD_ENABLED` | Set exactly `true` only after federation is configured and the destination is confirmed |
| `DATABRICKS_HOST` | HTTPS workspace origin, without URL parameters |
| `DATABRICKS_CLIENT_ID` | Service principal application UUID |
| `DATABRICKS_WAREHOUSE_ID` | Existing SQL warehouse ID |
| `DATABRICKS_CATALOG` | Optional; defaults to `workspace` |
| `DATABRICKS_SCHEMA` | Optional; defaults to the currently validated PaySim snapshot schema |
| `DATABRICKS_EXPORT_PATH` | Optional; defaults to the export already uploaded to the Unity Catalog volume |

The variables used by the preflight must be repository variables, not environment-only values: the preflight runs before entering the deployment environment. No `DATABRICKS_TOKEN` or client secret is used. OIDC does not remove the account's ordinary compute/storage charges; the workflow does not run compute jobs itself.

After pushing all files and observing a green CI run for the same commit, open **Actions → Deploy Databricks bundle → Run workflow → main**. An activation test remains required on the supported account. Inspect the deployment logs and bundle summary before treating continuous delivery as operational.

## Local validation and tests

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
databricks bundle validate --target ci --profile dev --strict --var warehouse_id=<EXISTING_ID>
```

The second command validates the CI target using the local interactive profile, without deploying it; it does not validate GitHub OIDC. The gate is unit-tested against stale main, unrelated commits, missing configuration, pending CI, skipped jobs and a newer failed CI run. It has also accepted the existing successful public main run using read-only API responses. The workflow YAML and pinned action references are checked locally. The default, unconfigured path is verified to stop before deployment.

References: [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [GitHub OIDC setup](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github), [GitHub workflow-run API](https://docs.github.com/en/rest/actions/workflow-runs), [workflow-job API](https://docs.github.com/en/rest/actions/workflow-jobs).
