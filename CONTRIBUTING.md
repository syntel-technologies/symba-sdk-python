# Contributing to Symba Python SDK

Use https://github.com/syntel-technologies/symba-sdk-python. Branch from `dev` and open a focused PR back to `dev`. Describe the problem, changed behavior, compatibility impact and validation. Use a Conventional Commit title (`feat:`, `fix:`, `docs:`, `ci:`); mark breaking changes with `!` and explain the migration.

Run the checks in [the release and development guide](docs/releasing.md). Never commit credentials, generated build caches or customer data. Do not weaken a test or a quality threshold to mask a failing behavior. Protocol changes must be checked against the paired engine/SDK.

Promote `dev` to protected `main` through a reviewed merge PR. Do not force-push published history. Release Please proposes versions and release notes; review the generated release PR before it can merge. Maintainers require an independent reviewer; an author cannot approve their own PR.
