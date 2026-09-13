# Releases and repository workflow

The canonical repository is https://github.com/syntel-technologies/symba-sdk-python. Use `origin` for Syntel. Development goes to `dev`; `main` only changes through a reviewed PR. Published history and `v*` tags must never be rewritten.

## Normal development

1. Branch from updated `dev`, make a focused change and open a PR back to `dev`. Use a meaningful Conventional Commit title: `fix: reconnect an idle worker`, `feat: add task capabilities`, or `feat!: change a public contract`.
2. Squash short-lived feature PRs so their reviewed titles become meaningful commits. Promote `dev` to `main` with a **merge commit**, preserving the long-lived branch ancestry. The main rules allow merge commits only; short-lived feature PRs into dev can still use squash. Never repeatedly squash `dev` into `main`.
3. The `main` rules require a PR, resolved review conversations, passing checks on an up-to-date branch, and no blocking CodeQL findings. Required approvals are **zero**: the owner can review and merge their own PR once checks pass; GitHub does not permit approving your own PR. Repository administrators have no bypass. `Required checks` aggregates the mandatory CI jobs so a skipped/failed dependency cannot look green. `PR title` validates the review title.
4. For commits pushed directly to `dev`, use Conventional Commit messages too. The title check cannot retroactively rename arbitrary commits inside a long-lived branch promotion. Keep changes cohesive; do not use `fixes` or `gooo`.

Existing historical commit messages are preserved. The migration commits explain the consolidated changes, and `docs/first-release-notes.md` provides a curated account of the first release. Review and add that summary to the first generated release PR; old vague commit messages cannot produce good notes automatically.

## Automated release sequence

After CI succeeds for a push to the current `main`, Release Please opens or updates a release PR with the next version, `CHANGELOG.md`, package metadata and the lockfile. An outdated CI completion cannot start a release for a newer untested main revision.

- `fix` and `perf`: patch release; `feat`: minor release; `!` / `BREAKING CHANGE:`: major release. This configuration deliberately applies these rules even before 1.0. The initial version is 0.1.0. Pure maintenance commits do not force a release.
- Review the proposed notes and compatibility impact. The bot never approves its own changes or bypasses rules. Enable auto-merge on the release PR if you want it merged after you have reviewed it and the required checks finish.
- After that PR merges and main CI passes, Release Please creates the matching `vMAJOR.MINOR.PATCH` tag and GitHub Release. The tag starts `Release`, which checks tag/package agreement, ancestry in reviewed main history and the release notes, then reruns the complete CI workflow on the **tagged commit** before publishing artifacts.
- Release Please updates `uv.lock` along with the package version. Its TOML updater represents strings as tagged values; the JSONPath intentionally uses `@.name.value`. The migration dry run tested the pinned action's Release Please 17.3.0 implementation and `uv lock --check`. Revalidate this behavior when updating the action. The engine console's two npm version records and runtime version also move together. Wire protocol versions are independent compatibility declarations and are not blindly rewritten by packaging automation.
- The bot opens a `main` → `dev` synchronization PR when needed. Merge it with a merge commit before the next promotion, preserving release metadata and avoiding conflicts. It never force-pushes `dev`.

A GitHub Release can exist while its artifact workflow is still running or has failed. A release is consumable only when **Release is green and its documented artifacts are attached**. Never claim that a tag alone proves a successful publication. Fix a publishing failure through a reviewed change and a new version when code changes; do not move a published tag. Existing release assets are never silently replaced with different bytes.

## Release bot activation (organization owner)

Verified on 13 September 2026: `Syntel Release Bot` is installed, its exact Contents/PRs/Issues permissions were accepted, and **Release bot check** passed in both repositories. `RELEASE_APP_CLIENT_ID` is set per repository; the private key is available through the organization secret. New `v*` tags are restricted to this App by a separate creation rule. The main and immutable-tag rules grant it no bypass. The steps below are retained for key rotation or rebuilding the setup.

Create one private GitHub App owned by `syntel-technologies`, named `Syntel Release Bot` (choose a unique slug if GitHub requires one):

- Start at https://github.com/organizations/syntel-technologies/settings/apps/new.
- Homepage: https://github.com/syntel-technologies. Disable webhooks; no callback or user authorization URL is needed.
- Repository permissions: **Contents: read/write**, **Pull requests: read/write**, **Issues: read/write** (release labels), Metadata read. No organization permissions. Install it only on `symba` and `symba-sdk-python` initially.
- Generate its private key. Put the key directly into the organization Actions secret `RELEASE_APP_PRIVATE_KEY`, restricted to these two repositories. Do not paste it into chat or commit it. Put the App **Client ID** into the organization Actions variable `RELEASE_APP_CLIENT_ID`, also restricted to these repositories. Alternatively use repository variables/secrets with the same names.
- Workflows mint short-lived installation tokens restricted to their own repository and the listed permissions. A personal admin token is not needed. Keep the existing main protection and immutable-tag rules. Once the App is installed, add a separate tag-creation ruleset for `v*` allowing only this App; give it no main or immutable-tag bypass.

Run **Release bot check** after activation (or rerun its initial dev check). It validates the key, the requested release permissions and single-repository token scope without creating a release.

Until the App variable exists, release automation records an explicit setup notice and creates no release. This is deliberate: GitHub's built-in token does not produce normal unattended downstream tag workflows, and its bot-created PR workflows can require manual approval. An installation token supports the reviewed release flow without a personal access token. See [GitHub's triggering rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow) and [Release Please](https://github.com/googleapis/release-please-action).

No App subscription is required. These are public repositories, so standard GitHub-hosted Actions runners and public-repository protections are available without GitHub Team. Do not add paid review bots to establish this baseline.

## Dependency and security checks

Every PR CI run also checks workflow syntax with Actionlint and scans committed history with Gitleaks. Both CLIs use pinned releases verified against fixed SHA256 digests; Gitleaks redacts secret values. These checks are included in `Required checks`.

Dependabot sends grouped compatible dependency updates to `dev` weekly; major updates remain separately reviewable. GitHub also reports known vulnerabilities and security updates on the default branch. CodeQL, secret scanning and push protection cover the public repositories once configured. The nightly engine load suite is a separate performance signal, not a substitute for PR CI; never lower its thresholds merely to make it green.

## Reproducing checks

```sh
uv sync --frozen --all-extras --python 3.13
uv run --no-sync ruff check src tests tools
uv run --no-sync ruff format --check src tests tools
uv run --no-sync pyright
uv run --no-sync pytest tests/unit tests/conformance -q
uv run --no-sync python tools/generate_proto.py --check
uv build
uvx --from twine==7.0.0 twine check dist/*
```

CI additionally covers Python 3.11–3.14, minimum direct dependencies and absent/5.x/8.x Redis. `--no-sync` is essential after adjusting an environment for a matrix case. Live conformance starts the engine commit in `src/symba/_proto/ENGINE_REF` and requires every engine case to execute successfully; a missing service cannot be reported as a green skip. To change the engine contract, update the 40-character commit pin, run `make proto-gen`, review the generated Python **and typing** files, and pass live compatibility CI. `src/symba/_proto/VERSION` records the wire release line, not a checkout ref.


## Artifacts and publication

`Release` attaches the validated wheel and source distribution to GitHub. On 13 September 2026 the owner confirmed creating the account and pending Trusted Publisher for `syntel-symba`; the repository variable `PYPI_PUBLISH_ENABLED` is now `true`. Publication still requires the reviewed tagged-release workflow to pass. No first publication has run yet. There is no stored PyPI API token.

1. Create a free account at https://pypi.org/account/register/ (or use your existing account), verify your email, and enable two-factor authentication. Save your recovery codes securely. A paid PyPI organization is not needed.
2. Open https://pypi.org/manage/account/publishing/ and add a **pending publisher** with these exact values:

| Field | Value |
| --- | --- |
| PyPI project | `syntel-symba` |
| GitHub owner | `syntel-technologies` |
| Repository | `symba-sdk-python` |
| Workflow filename | `release.yml` |
| Environment | `pypi` |

3. A pending publisher does **not** reserve the name or immediately create a project. The first successful GitHub OIDC publication creates `syntel-symba` under your account. Its name was unpublished when checked on 2026-09-13; the existing unrelated `symba` project must not be used. Python imports and the `symba` CLI remain unchanged.
4. Once the pending publisher is configured and the first release is approved, enable publication as described below. No PyPI password or API token goes into GitHub.

The GitHub `pypi` environment is restricted to `v*` tags. Repository Actions variable `PYPI_PUBLISH_ENABLED=true` enables publication after a reviewed release; set it to `false` to pause future publication. The publishing job receives OIDC permission; ordinary tests do not. The first release still needs review; enabling the variable alone creates no tag or package. If the PyPI name is unavailable, change the distribution name, metadata, workflow URLs and release configuration together in a PR before publishing. See [GitHub/PyPI OIDC setup](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-pypi).


Official PyPI guidance: [create a project through OIDC](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/), [add a publisher](https://docs.pypi.org/trusted-publishers/adding-a-publisher/), and [two-factor authentication](https://pypi.org/help/#twofa).

## Dependency update reviews

Dependabot proposes changes into `dev`; an open update PR is not a supported release.
Compiler/runtime protobuf packages are grouped separately from general Python
updates. Review generated-code changes and required runtime floors together; do
not disable drift or minimum-dependency checks to accept a compiler upgrade.

The SDK pins `grpcio-tools` exactly for reproducibility. On an update, run
`uv run --no-sync python tools/generate_proto.py`, update `grpcio`/`protobuf` minimums
from the generated guards, and run the drift, packaging, minimum-dependency and
live-engine checks. Do not change `ENGINE_REF` unless the wire contract is also
being updated. The generated-floor regression test checks installed package
metadata, protecting consumers who resolve versions outside our lockfile.
