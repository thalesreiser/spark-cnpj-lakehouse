# 007 - Download the CNPJ data through the Nextcloud public WebDAV endpoint

- **Status:** accepted
- **Date:** 2026-10-16

## Context

The Receita Federal moved the CNPJ open data again (risk R-04). On 2026-10-16, `dadosabertos.rfb.gov.br/CNPJ/` no longer answers and `arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/<YYYY-MM>/` returns 404. The host root now redirects to a Nextcloud public share whose web UI is a JavaScript application, so there are no plain directory listings to scrape. The same share exposes a public WebDAV endpoint (`public.php/dav/files/<share token>/...`) that serves the files with `Range` support and lists folders with `PROPFIND`. Details in [`docs/data-source.md`](../data-source.md).

## Decision

1. Download each file from `https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/<YYYY-MM>/<file>.zip`.
2. Keep the list of the 37 files of a release in code (`FILE_GROUPS` in `download.py`) instead of discovering it with `PROPFIND`: the layout has been stable for years, the list drives the `--files` groups, and tests do not depend on WebDAV.
3. Make the base URL configurable (`--base-url` and `CNPJ_BASE_URL`), so the next move needs only a new value.
4. Since the source publishes no checksums, compute the SHA-256 of every zip locally and record it in `manifest.json`.

## Alternatives considered

- **Nextcloud web download links** (`index.php/s/<token>/download?path=...&files=...`): an undocumented UI route that changes between Nextcloud versions; on this server it only answers `303` and redirects to the WebDAV endpoint anyway, so WebDAV (the documented API for public shares) is used directly.
- **Discover files with `PROPFIND`:** adapts automatically to new files, but couples the code to WebDAV XML and to this hosting choice; it can be added later if the layout changes.
- **Third-party mirrors:** easier to reach, but not the official source and with no provenance guarantee.

## Consequences

- The share token is part of the URL; if the Receita Federal recreates the share, the default must be updated (or overridden through `CNPJ_BASE_URL`). The `Download` workflow detects that on the next run.
- Resume (`Range`) and retries make large files (`Estabelecimentos0.zip`, 2.2 GB) practical on unstable connections.
- The checksum protects integrity between runs and makes re-runs idempotent, but it cannot prove the files match what the publisher intended.
