# Data source: Receita Federal CNPJ open data

Findings recorded for RF-01 and risk **R-04** (the open data changes format or URL). Checked on **2026-10-16**.

## Where the data lives

The Receita Federal moved the dataset again. The old locations are gone:

| URL | Status on 2026-10-16 |
|---|---|
| `https://dadosabertos.rfb.gov.br/CNPJ/` | connection timeout (host down) |
| `https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/<YYYY-MM>/` | HTTP 404 |
| `https://arquivos.receitafederal.gov.br/` | **302 to a Nextcloud public share** `index.php/s/gn672Ad4CF8N6TK` |

The share holds every Receita Federal open dataset (`Dados/`, `Documentos/`, `Programas/`). The CNPJ releases are under `Dados/Cadastros/CNPJ/`, one folder per reference month (`2023-05/` to `2026-10/` on the check date), plus a 64 GB `cnpj.tar.gz` with the history. The dados.gov.br catalog page ("Cadastro Nacional da Pessoa Jurídica - CNPJ") is rendered with JavaScript and exposes no static link; the share was found from the redirect of the host root.

Nextcloud exposes public shares through **WebDAV** with no credentials, which gives stable, scriptable URLs (unlike the web UI, which is a JavaScript application):

```
# list the reference months (or the files of one month)
curl -X PROPFIND -H "Depth: 1" https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/
curl -X PROPFIND -H "Depth: 1" https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/2026-09/

# download one file
curl -O https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/2026-09/Cnaes.zip
```

`cnpj_lakehouse.download` uses `https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ` as the default base URL and builds `<base>/<YYYY-MM>/<file>.zip`. If the source moves again, override it with `--base-url` or the `CNPJ_BASE_URL` environment variable; no code change is needed ([ADR 007](adr/007-cnpj-source-nextcloud-webdav.md)).

## Server behaviour

| Topic | Finding |
|---|---|
| HTTP `Range` | Supported: `Range: bytes=100-` returns `206 Partial Content` with `Content-Range: bytes 100-1179/1180` (the `Accept-Ranges` header is not advertised). Downloads resume from the partial file. |
| Published checksums | **None.** There are no `.md5`/`.sha256` files, and the WebDAV `oc:checksums` property returns 404. The `ETag` is a Nextcloud identifier, not the MD5 of the content. The SHA-256 is therefore computed locally and recorded in `manifest.json` (it proves integrity between runs, not against the publisher). |
| Metadata | `PROPFIND` gives `getcontentlength` and `getlastmodified` for every file. |
| Stability | The server occasionally drops a kept-alive connection (`RemoteDisconnected`) during a series of requests; downloads retry with exponential backoff. |
| Reachability from GitHub runners | Proven by the `Download` workflow ([`download.yml`](../.github/workflows/download.yml)) on the pull request that introduced it. |

## Layout of a monthly release

37 zip files, each holding a single headerless `;`-separated CSV (Latin-1) with a cryptic name such as `K3241.K03200Y0.D60912.EMPRECSV`. The names of the extracted files are recorded per zip in `manifest.json`, so the bronze layer does not need to guess them. Columns, types and the exact CSV format (quoting, dates, decimals) are in the [data dictionary](data-dictionary.md).

Sizes of the **2026-09** release (from `PROPFIND`; uncompressed sizes read from each zip's central directory with `Range` requests, without downloading the data):

| Group (`--files`) | Files | Zipped | Unzipped |
|---|---|---:|---:|
| `empresas` | `Empresas0.zip` … `Empresas9.zip` | 1.38 GB | 5.48 GB |
| `estabelecimentos` | `Estabelecimentos0.zip` … `Estabelecimentos9.zip` | 5.38 GB | 17.12 GB |
| `socios` | `Socios0.zip` … `Socios9.zip` | 0.69 GB | 2.90 GB |
| `simples` | `Simples.zip` | 0.31 GB | 3.17 GB |
| `lookups` | `Cnaes.zip`, `Motivos.zip`, `Municipios.zip`, `Naturezas.zip`, `Paises.zip`, `Qualificacoes.zip` | 72 KB | < 1 MB |
| **`all`** | **37 files** | **7.76 GB** (7,758,926,262 bytes) | **28.67 GB** |

Per file (bytes, zipped): `Cnaes` 22,078 · `Motivos` 1,180 · `Municipios` 43,443 · `Naturezas` 1,563 · `Paises` 2,745 · `Qualificacoes` 980 · `Simples` 308,027,792 · `Empresas0` 563,070,974, `Empresas1-9` 77.9–98.8 MB each · `Estabelecimentos0` 2,243,389,254, `Estabelecimentos1-9` 336–369 MB each · `Socios0` 247,683,729, `Socios1-9` 49.0–49.6 MB each. File `0` of each group is the largest. The 2026-10 release has the same 37 files and 7.81 GB zipped.

Keeping the zips and the extracted CSVs of a full release takes about 36 GB, which does not fit on a GitHub runner (14 GB of SSD) or on the development laptop. Runs on CI download a subset (e.g. `lookups` plus `Empresas1`) and the pipeline samples it (RNF-03).

## Download output

`make download COMPETENCIA=2026-09 FILES=lookups` writes:

```
data/raw/2026-09/
├── Cnaes.zip, Motivos.zip, ...          # downloaded zips (kept to verify the checksum)
├── F.K03200$Z.D60912.CNAECSV, ...       # extracted CSVs, in the same folder
└── manifest.json
```

```json
{
  "competencia": "2026-09",
  "base_url": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ",
  "generated_at": "2026-10-16T20:57:59Z",
  "algorithm": "sha256",
  "files": [
    {
      "name": "Cnaes.zip",
      "url": "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ/2026-09/Cnaes.zip",
      "size_bytes": 22078,
      "sha256": "6036941a40ff0ec664c61d844f77d5c0189fa149e9a4cc068a26d367cf764dac",
      "downloaded_at": "2026-10-16T20:57:57Z",
      "extracted": ["F.K03200$Z.D60912.CNAECSV"]
    }
  ]
}
```

Re-running is idempotent: a file whose zip still matches the manifest checksum (and whose extracted files exist) is skipped, and downloading another subset of the same month adds entries to the same manifest.
