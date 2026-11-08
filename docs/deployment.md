# Manual GitHub deployment — prepared, not activated

[Back to README](../README.md)

## Current status and Free Edition limitation

The local `dev` deployment works with interactive Databricks CLI authentication. The manual [deploy workflow](../.github/workflows/deploy.yml) is prepared for GitHub-hosted runners with either explicit OIDC federation or an explicitly selected PAT secret. **GitHub-to-Databricks authentication is not activated or tested end to end**.

Databricks' documented GitHub federation setup requires a service principal and an account-level federation policy. Free Edition does not expose the account console or account-level APIs. This blocks configuring that standard OIDC route with the current Free Edition account. Read-only checks on the current workspace confirmed `enableTokensConfig=true` and access to token metadata/permission APIs; PAT creation and GitHub authentication still require an activation test. No service principal, federation policy, GitHub secret, new account or paid resource has been created. Do not enable the workflow flag to bypass this prerequisite; it cannot grant missing access.

Continue deploying locally with the existing `dev` profile while deciding whether to use a supported Databricks account later. The workflow deliberately fails with an explanation when configuration is incomplete. It never exports local OAuth credentials, copies `.databrickscfg`, automatically falls back to a personal token or installs a runner on the personal computer. PAT mode must be explicitly selected and marked ready; its token belongs only in the protected GitHub environment secret.

## Workflow behavior

1. Only **Run workflow** on `main` is accepted. Push and pull-request events do not deploy.
2. An explicit configuration check runs before Databricks authentication.
3. The gate checks that the selected SHA is still current `main`, waits up to 15 minutes for the latest push-triggered `ci.yml` run for that exact SHA, and requires both named jobs to succeed. Failed, cancelled, skipped or different-commit results do not qualify.
4. The deploy job uses the `databricks-ci` GitHub environment. In OIDC mode it requests a short-lived GitHub token; in PAT mode it reads the environment secret without logging it or the current user profile. Only this job has `id-token: write`.
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

## Optional PAT activation for the current workspace

The workflow defaults to OIDC and is disabled. To opt into PAT mode, the workspace owner must create a short-lived personal access token (for example 30 days), then enter it **directly in GitHub** under Settings → Environments → `databricks-ci` → Environment secrets → `DATABRICKS_TOKEN`. Do not paste it into chat, source files, repository variables or CLI arguments. The token acts within the owner's workspace permissions, further restricted by its API scopes when supported. It is a personal credential, not a dedicated deployment identity. Choose only the scopes needed for bundle files, jobs, dashboards and warehouse metadata; do not grant the `authentication` scope just to deploy resources. The initial scoped configuration still needs an end-to-end test. Revoke or rotate it when no longer needed.

Set repository variables `DATABRICKS_AUTH_TYPE=pat`, `DATABRICKS_PAT_READY=true`, the workspace host and existing warehouse ID; enable `DATABRICKS_CD_ENABLED=true` only after the protected environment and its secret are configured. `DATABRICKS_CLIENT_ID` is not required in PAT mode. Restrict the environment to `main` before storing the token. Authentication is verified before deployment; an absent or expired token fails the workflow, without any OAuth-cache fallback. The read-only capability checks do not prove token creation or the full deployment flow; those remain to be verified by the owner during activation.

The current repository contains no token, and no token was created or uploaded while preparing this workflow. The existing local OAuth deployment remains an option if a personal GitHub credential is not desired. See [Databricks personal access tokens](https://docs.databricks.com/aws/en/dev-tools/auth/pat).

## OIDC configuration for a supported account

Before activation, an account administrator must configure a Databricks service principal, assign it to the destination workspace, and create a federation policy restricted to **this repository and the `databricks-ci` environment**. Use the actual GitHub OIDC `sub` claim: newer repositories can include immutable owner/repository IDs. Do not copy a guessed name-only subject from an old example. The audience must match the Databricks account's configuration.

Grant only the workspace and data access needed to deploy these resources: the identity's deployment folder, the existing warehouse where applicable, and the appropriate catalog/schema permissions. Running the import job later requires separate access to the export volume and target tables. Never substitute workspace-wide administrator access as a shortcut.

Create the GitHub environment `databricks-ci`, restrict its deployment branches to `main`, and configure a reviewer if desired and supported. Set the following **repository variables** under Settings → Secrets and variables → Actions → Variables. They are identifiers/configuration, not authentication secrets:

| Variable | Purpose |
| --- | --- |
| `DATABRICKS_CD_ENABLED` | Set exactly `true` only after the selected authentication method and destination are configured |
| `DATABRICKS_AUTH_TYPE` | `github-oidc` (default) or explicitly selected `pat` |
| `DATABRICKS_PAT_READY` | Must be exactly `true` for PAT mode; no effect on OIDC setup |
| `DATABRICKS_HOST` | HTTPS workspace origin, without URL parameters |
| `DATABRICKS_CLIENT_ID` | Service principal application UUID, required only for OIDC |
| `DATABRICKS_WAREHOUSE_ID` | Existing SQL warehouse ID |
| `DATABRICKS_CATALOG` | Optional; defaults to `workspace` |
| `DATABRICKS_SCHEMA` | Optional; defaults to the currently validated PaySim snapshot schema |
| `DATABRICKS_EXPORT_PATH` | Optional; verified export folder under `/Volumes/` or `s3://`; defaults to the existing volume export |
| `DATABRICKS_S3_VOLUME` | Required for S3 export URLs; existing external volume as `catalog.schema.volume` |

The variables used by the preflight must be repository variables, not environment-only values: the preflight runs before entering the deployment environment. No client secret is used. OIDC mode does not need a `DATABRICKS_TOKEN`; the explicit PAT alternative uses only the protected environment secret. OIDC does not remove the account's ordinary compute/storage charges; the workflow does not run compute jobs itself.

After pushing all files and observing a green CI run for the same commit, open **Actions → Deploy Databricks bundle → Run workflow → main**. An activation test remains required on the supported account. Inspect the deployment logs and bundle summary before treating continuous delivery as operational.

## Local validation and tests

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
databricks bundle validate --target ci --profile dev --strict --var warehouse_id=<EXISTING_ID>
```

The second command validates the CI target using the local interactive profile, without deploying it; it does not validate GitHub OIDC. The gate is unit-tested against implicit PAT fallback, unknown auth modes, stale main, unrelated commits, missing configuration, pending CI, skipped jobs and a newer failed CI run. It has also accepted the existing successful public main run using read-only API responses. The workflow YAML and pinned action references are checked locally. The default, unconfigured path is verified to stop before deployment.

References: [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [GitHub OIDC setup](https://docs.databricks.com/aws/en/dev-tools/auth/provider-github), [GitHub workflow-run API](https://docs.github.com/en/rest/actions/workflow-runs), [workflow-job API](https://docs.github.com/en/rest/actions/workflow-jobs).
