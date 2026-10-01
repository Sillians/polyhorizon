# CI/CD Audit

Audit date: 2026-07-22

## Clean-checkout configuration fix (2026-10-01)

Run `36875635333` failed in Python test collection. A clean copy reproduced
two import-time problems that the developer's ignored `.env` concealed:
core logging called `.upper()` on an unset level, and acquiring a training
logger loaded the entire training configuration, including database and MLflow
settings.

Core logging now has built-in defaults and defaults to console output. Training
logger acquisition no longer loads configuration. Training and retraining flows
explicitly configure their service logger with the validated `config.logging`,
including custom config paths, without replacing Prefect/root handlers. Actual
training configuration remains strict: missing required settings still fail when
the flow loads its configuration.

The validation job sets `PYTHON_DOTENV_DISABLED=1`. Tests must supply their own
settings or mocks; CI does not copy a deployment template into `.env` and does
not need production credentials. Regression tests exercise logging in a child
process with no service environment and verify explicit logging configuration.

Commit `.env.example` and `.env.prod.template` with placeholders only. Keep `.env`
and `.env.prod` ignored. Both templates are tracked as of commit `74a585d`; the
production template is required by Compose and the deployment contract checks.
The Compose validation step copies the sanitized template to the ephemeral
runner's `.env.prod` because service-level `env_file` entries still require that
file even when `--env-file .env.prod.template` supplies interpolation values.

Verification used an archive of the current commit with these changes overlaid,
without the developer's `.env`, and with `PYTHON_DOTENV_DISABLED=1`: 244 tests
passed, 5 opt-in integration tests skipped, and the separate forecast release
gate passed. Ruff, deployment shell syntax, production Compose rendering, and
all six architecture contract validators passed. These were local checks using
the installed Python 3.11 environment; the updated workflow still needs a GitHub
Actions run after the changes are pushed.

## Implemented in this audit

Image build follow-up (2026-10-01): run `36882179700` passed validation but all
seven image jobs rejected `ghcr.io/Sillians/polyhorizon/...` before building.
Docker requires lowercase image repository names. The build job now normalizes
GitHub's repository value before creating tags; release manifests, their bundled
environment template, and deployment image-existence checks use the same lowercase
path. Commit SHA tags and source-repository labels retain their original values.
The regression test exercises the workflow normalization, manifest generation,
and deployment inspection commands with `Sillians/PolyHorizon`, verifying that
all seven image references agree. It passes alongside Ruff, shell syntax, and the
release contract validator; publishing still requires the next GitHub Actions run.

| Area | Previous state | Implemented state |
|---|---|---|
| Pull-request gate | Python checks only; no Compose, shell, release-contract, or frontend gate | Blocking Python tests/lint, frontend test/build, shell syntax, Compose rendering, and release-contract validation |
| Image release | Independent service workflows published incompatible tags and could leave a partial release | One matrix publishes every release-owned image under one `sha-<full commit>` tag only after CI passes |
| Supply-chain metadata | No release-level inventory | BuildKit provenance and SBOM attestations plus a retained `release.json` inventory |
| Deployment input | Optional tag could resolve to a commit without a complete release | Required full immutable tag; every GHCR image is checked before deployment |
| Deployment control | No environment approval or deployment serialization | Protected `production` environment and a non-cancelling production concurrency group |
| Runtime image consistency | Compose, service workflows, and Prefect disagreed on tags; ingestion used `latest` | Automated contract check keeps Compose, Prefect, deploy scripts, and the image matrix aligned |
| Prefect | Work pools and deployment definitions were not part of production deployment | Idempotent pool creation/validation and deployment synchronization from the immutable orchestration image |
| Verification | External checks included a nonexistent Traefik health endpoint; frontend health used unavailable `curl` | Valid external checks, native Traefik ping, portable frontend health check, and container-state verification |
| Rollback | Documented as a manual tag edit | Failed deployments restore the previous tag, application containers, and Prefect deployments automatically |

## Verified

- Workflow files parse as YAML.
- All deployment shell files pass `bash -n`.
- `docker-compose.prod.yaml` renders successfully using the production template.
- The release image contract validator passes.
- Ruff passes.
- Python tests pass: 21 passed and 2 skipped. The skipped tests require external integration services.
- Release manifest generation produces valid JSON and a correctly pinned environment template.

## Remaining work and operational prerequisites

1. **Type-safety baseline:** mypy currently reports 150 pre-existing errors across 48 files. It remains advisory until those errors are fixed or a justified incremental baseline is established.
2. **Vulnerability policy:** SBOM/provenance generation is enabled, but the workflow does not yet fail releases on a defined CVE threshold. Add an organization-approved scanner and exception policy.
3. **Action immutability:** third-party and GitHub Actions use major/version tags. Pin actions to reviewed commit SHAs and automate controlled updates (for example, with Dependabot).
4. **Database safety:** PostgreSQL now has checksummed logical backups and a
   restore-verification command. Configure the documented host schedule and
   off-host replication before enabling unattended deployment. Other stateful
   volumes still require equivalent policies.
5. **End-to-end staging:** add a staging environment that boots the Compose stack, seeds representative data/model artifacts, exercises Kafka → features → model → serving → frontend, and promotes the exact tested digests.
6. **Image signing/admission:** attestations exist, but production does not verify signatures or provenance before running images.
7. **Notifications and audit export:** add deployment/failure notifications and forward release/deployment records to the organization’s operational system.
8. **Host source synchronization:** the deployment host must already contain a compatible Compose file and deployment scripts. A future packaging step should ship the deployment bundle with the release and verify its checksum on-host.
9. **Frontend local toolchain:** the current workstation’s Homebrew Node binary is missing its `simdjson` dynamic library. GitHub Actions uses a clean Node 20 installation and is not affected.

## Required GitHub configuration

- Protect `main` and require the `validate` and `frontend` jobs.
- Create a protected `production` environment with required reviewers.
- Configure production secrets: `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY`, and `DEPLOY_PATH`.
- Configure production variable `POLYHORIZON_DOMAIN`.
- Allow GitHub Actions to publish and read repository packages.
- Keep production deployment manual until staging, backups, and vulnerability policy are in place.
