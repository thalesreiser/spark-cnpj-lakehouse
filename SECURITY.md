# Security policy

## Supported versions

This is a portfolio project with no releases. Only the `main` branch is supported; fixes land there and nothing is backported.

| Version | Supported |
|---|---|
| `main` | Yes |
| Anything else (old commits, forks) | No |

## Reporting a vulnerability

Please **do not open a public issue, pull request or discussion** for a security problem.

Report it privately through GitHub's **private vulnerability reporting**: open the repository's **Security** tab and click **Report a vulnerability**. Include what you found, how to reproduce it and the impact you expect.

Response time is **best effort**: this is a personal project maintained in spare time, so there is no SLA. You will get an acknowledgement on the advisory, and the fix and disclosure will be coordinated there.

## Scope

In scope:

- The code in this repository (`src/`, `scripts/`, `tests/`, `Makefile`, `infra/`).
- The GitHub Actions workflows in `.github/workflows/` (for example, permission or injection problems).
- Secrets or personal data accidentally committed to the repository.

Out of scope:

- The data itself. It comes from public sources (Receita Federal do Brasil CNPJ open data, see [`docs/data-source.md`](docs/data-source.md)); problems with the source or its servers should be reported to the source.
- Vulnerabilities in third-party dependencies that are already public: Dependabot tracks them (see below). A report is still welcome if this project uses a dependency in a way that makes it exploitable.
- The local development stack (MinIO with the default `minioadmin` credentials, Spark in containers), which is meant to run only on a developer machine or an ephemeral CI runner.

## How the repository is protected

- Secret scanning with push protection, plus **gitleaks** in the pre-commit hooks.
- Dependabot alerts and weekly version updates for `pip` and GitHub Actions ([`.github/dependabot.yml`](.github/dependabot.yml)).
- CodeQL code scanning.
- `main` is protected: changes go through pull requests with the `lint-test`, `smoke` and `bronze` checks.
- Workflows run with a read-only `GITHUB_TOKEN` (`permissions: contents: read`). First-party `actions/*` actions are referenced by major tag; any third-party action must be pinned to a full commit SHA with a `# vX.Y.Z` comment.
