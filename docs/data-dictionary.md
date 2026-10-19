# Data dictionary: CNPJ open data tables

Columns, target types and file format of every table of a monthly release. The code is the source of truth: [`src/cnpj_lakehouse/schemas.py`](../src/cnpj_lakehouse/schemas.py) (`SCHEMA_VERSION = "1"`); this page mirrors it. Design decision in [ADR 008](adr/008-bronze-all-strings-typed-in-silver.md).

## Source of the layout

| Item | Value |
|---|---|
| Official document | "Novo Layout para os DADOS ABERTOS do CNPJ" (`cnpj-metadados.pdf`, 6 pages), published by the Receita Federal |
| URL | <https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf> (HTTP 200, 59,315 bytes, SHA-256 `0a52d6bdfb61a07425e352a0e692932306a5ec9ecad682f5f9e28059e1a5fce0`, retrieved on 2026-10-16) |
| Version | PDF created and last modified on **2021-12-02** (document properties; original title `NOVOLAYOUTDOSDADOSABERTOSDOCNPJ`). No newer layout was found. |
| Not found in | the Nextcloud share of the data (`Dados/Cadastros/CNPJ/` holds only the monthly folders and `cnpj.tar.gz`; `Documentos/` has no CNPJ layout); the older `gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/...` pages now require a login |
| Checked against | the first rows of every file of the **2026-09** release (see [Evidence](#evidence-from-the-2026-09-release)) |

The PDF lists the columns in file order with a description, but **no data types or sizes**. The target types below are a project decision based on the descriptions and on the real values. Gaps in the PDF:

- **`Motivos`** (reasons of the registration status) is not described; its two columns (`codigo`, `descricao`) were taken from the file itself.
- The registration status codes are printed as `01, 2, 3, 4, 08`; the files use two digits (`01, 02, 03, 04, 08`).
- The column names are free text in Portuguese; the snake_case names here follow the descriptions (and the names commonly used by public projects that load this dataset).

## File format

| Fact | Value | Evidence |
|---|---|---|
| Encoding | **ISO-8859-1 (Latin-1)** | accented bytes are invalid UTF-8 (e.g. `Naturezas`: `Natureza Jurídica não informada`) |
| Separator | `;` | the layout says so; every row splits into the expected field count |
| Quoting | **every field quoted** with `"`, empty fields are `""` | all inspected rows start and end with `"` |
| Quotes inside values | **doubled** (`"NIGTH CLUB ""TUTU"" LTDA"`) | 22 of 61,939 `Empresas1` rows; requires `escape='"'` in Spark (its default escape is `\`) |
| `;` inside values | present inside quoted values (free-text fields such as names and address complements) | 117 of 16,783 `Estabelecimentos1` rows: never split on `;` without honouring quotes |
| Header row | **none** | first row of every file is data (`"00000000";"BANCO DO BRASIL SA";...`) |
| Line ending | `\n` (no `\r`) | |
| Dates | `yyyyMMdd`; no date is `00000000`, `0` or empty | `Simples`: `00000000` in 59,039 of 156,278 `data_exclusao_simples`; `Estabelecimentos`: `0` in 51 of 16,783 `data_situacao_cadastral`, empty in most `data_situacao_especial` |
| Decimal separator | `,` in `capital_social` (`120000000000,00`, `0,00`), no thousands separator | all 61,939 values match `\d+,\d+` |
| Lists | `cnae_fiscal_secundaria` holds several CNAE codes separated by `,` (`9493600,9499500`) | |
| Placeholders | e.g. `cnae_fiscal_principal = 8888888` (listed in `cnaes` as "economic activity not informed") | kept as is in bronze; handled in silver |
| Extracted file names | cryptic, one CSV per zip: `K3241.K03200Y1.D60912.EMPRECSV`, `...ESTABELE`, `...SOCIOCSV`, `F.K03200$W.SIMPLES.CSV.D60912`, `F.K03200$Z.D60912.CNAECSV`, `MOTICSV`, `MUNICCSV`, `NATJUCSV`, `PAISCSV`, `QUALSCSV` | the table is resolved from the zip name (`table_for_file`) and `manifest.json` maps each zip to its CSV |

Spark reader options (`CSV_READ_OPTIONS`): `header=false`, `sep=;`, `encoding=ISO-8859-1`, `quote="`, `escape="`, `mode=PERMISSIVE`, `multiLine=false`. With these options Spark reads `""` (empty field) as `null`.

## Type conventions

Bronze reads **every column as a nullable string** (ADR 008); the target type is applied in silver (RF-03). Each bronze field carries its `description` and `target_type` as Spark field metadata.

Every bronze table also has three metadata columns, added by the ingestion (`cnpj_lakehouse/bronze.py`, RF-02):

| Column | Type | Content |
|---|---|---|
| `_ingest_ts` | timestamp | Start of the ingestion run (UTC); the same for every row of a run |
| `_source_file` | string | Path of the source CSV the row was read from (Spark `_metadata.file_path`) |
| `competencia` | string | Reference month of the release (`YYYY-MM`); partition column ([ADR 002](adr/002-partitioning-by-month-and-state.md)) |

| Target type | Used for | Silver conversion |
|---|---|---|
| `string` | names, free text and **every code or identifier** (CNPJ parts, CNAE, municipality, legal nature, qualification, country, status, CEP, phones) | trim; codes keep their leading zeros (`0111301`, `05`, `013`) |
| `date` | every `data_*` column | `yyyyMMdd` parsed from validated parts (no `to_date`, see below); `""`, `0`, `00000000` and impossible dates become `null` |
| `decimal(20,2)` | `capital_social` | replace `,` with `.` and cast; 18 integer digits leave room for outliers |
| `array<string>` | `cnae_fiscal_secundaria` | split on `,`, each code normalized; empty or missing becomes an empty array |

"Nullable = no" marks the business key columns, which must be present in silver.

## Silver normalization rules

Implemented as pure `Column -> Column` functions in `cnpj_lakehouse/normalize.py` (RNF-04; tests in `tests/test_normalize.py`); `apply_silver_types(df, table)` applies them to a bronze DataFrame column by column ([ADR 009](adr/009-alphanumeric-cnpj-and-invalid-values.md)). An invalid value becomes `null` in silver; the original stays in bronze.

| Function | Rule | Examples |
|---|---|---|
| `clean_string` | trim, collapse inner whitespace, empty -> `null` (display value, accents kept) | `"  BANCO   DO BRASIL "` -> `BANCO DO BRASIL`; `"  "` -> `null` |
| `normalize_key` | `clean_string` + accents stripped (`ÁÀÂÃÄ ÉÈÊË ÍÌÎÏ ÓÒÔÕÖ ÚÙÛÜ Ç Ñ`, both cases) + upper case, for joins and comparisons | `" São  Paulo"` -> `SAO PAULO` |
| `normalize_cnpj_basico` / `_ordem` / `_dv` | masks removed, upper case; numeric values left-padded to 8 / 4 / 2; alphanumeric values (base and order only) must have full width | `191` -> `00000191`; `12abc345` -> `12ABC345`; `ABC` -> `null`; DV `3A` -> `null` |
| `build_cnpj` | RN-01 key: base + order + DV, `null` if a part is invalid (DV not checked) | (`00000000`, `0001`, `91`) -> `00000000000191` |
| `normalize_cnpj` / `split_cnpj` | full CNPJ, masked or not -> 14 characters (or its 3 parts); check digits validated by default | `12.abc.345/01de-35` -> `12ABC34501DE35`; `12ABC34501DE36` -> `null` |
| `cnpj_check_digits` / `is_valid_cnpj` | module 11 over `ascii(char) - 48` (numeric and alphanumeric CNPJ); the flag is `false` for invalid values, `null` only for `null` | `12ABC34501DE` -> `35`; `000000000001` -> `91` |
| `normalize_cnae` | masks removed, digits only, left-padded to 7 | `0111-3/01` -> `0111301`; `111301` -> `0111301`; `47A1302` -> `null` |
| `normalize_cnae_list` | split on `,`, each code normalized, invalid and duplicate codes dropped, source order kept; `null`/empty -> `[]` | `"4711302,111301,,XX"` -> `[4711302, 0111301]` |
| `parse_date` | `yyyyMMdd` -> `date`; `""`, `0`, `00000000`, wrong length, non-digits and impossible dates -> `null` (leap years honoured) | `20240229` -> 2024-02-29; `20251340`, `20250230` -> `null` |
| `parse_decimal` | `-?\d{1,18}(,\d{1,2})?` -> `decimal(20,2)`; thousand separators, `.` decimals and more than 2 decimals -> `null` | `120000000000,00` -> 120000000000.00; `1.000,50` -> `null` |

**Alphanumeric CNPJ.** From July 2026 the Receita Federal assigns CNPJs whose first 12 characters may contain upper-case letters; the 2 check digits stay numeric and are computed with module 11 over `ascii(char) - 48` (official example `12.ABC.345/01DE-35`). Letters are therefore never stripped from CNPJ values, and the CNPJ is never cast to a number.

**ANSI mode.** In Spark 3.5 `to_date` and `try_to_timestamp` raise on `20251340` when `spark.sql.ansi.enabled=true`, and on `2025011` with the default `spark.sql.legacy.timeParserPolicy=EXCEPTION`. `parse_date` validates year, month and day with arithmetic and only then calls `make_date`, and `parse_decimal` only casts values that match its pattern, so no function raises in either mode (the tests run with ANSI on and off).


## Silver keys and current record

`cnpj_lakehouse/silver.py` builds one silver table per bronze table (RF-03; tests in `tests/test_silver.py`). After the types are applied, rows whose key columns marked "not nullable" below are empty or malformed are discarded and counted (`rows_without_key`). Within each business key a window function (`row_number() over (partition by key order by ...)`) keeps the **current record**: the first row of the ordering below, every criterion descending (most recent first) with nulls last.

| Table | Business key | Current record: ordering after `competencia` |
|---|---|---|
| `estabelecimentos` | `cnpj_basico` + `cnpj_ordem` + `cnpj_dv` (RN-01; all required) | `data_situacao_cadastral`, `data_inicio_atividade` |
| `empresas` | `cnpj_basico` | (common tie-breakers only) |
| `simples` | `cnpj_basico` | latest of `data_opcao_simples`, `data_exclusao_simples`, `data_opcao_mei`, `data_exclusao_mei` |
| `socios` | `cnpj_basico` + `identificador_socio` + `cpf_cnpj_socio` + `nome_socio_razao_social` (only `cnpj_basico` required; an empty `cpf_cnpj_socio`, as for foreigners, is one group) | `data_entrada_sociedade` |
| lookups (`cnaes`, `motivos`, `municipios`, `naturezas`, `paises`, `qualificacoes`) | `codigo` | (common tie-breakers only) |

Every ordering starts with `competencia` (a later release wins) and ends with the common tie-breakers `_ingest_ts`, `_source_file` and `_row_hash` (SHA-256 of the business columns), so the choice never depends on the physical order of the rows. The `socios` key is a natural key, since the source has no partner identifier: the CPF is masked (`***999999**`), so the name is needed to tell partners apart; the same person with two qualifications in one company collapses to one row (the most recent entry date).

`estabelecimentos` gets two extra columns: `cnpj` (14 characters, base + order + DV) and `cnpj_valido` (check digits match and the CNPJ is not `00000000000000`). By default an invalid CNPJ is kept with `cnpj_valido = false`; `--invalid-cnpj drop` discards it. Both switches and their counts are described in [ADR 010](adr/010-invalid-and-all-zero-cnpj-in-silver.md) (proposed).

**Write strategy (RF-04, [ADR 011](adr/011-silver-current-state-merge.md)).** Each silver table holds the current state of every business key across months, not partitioned. A run merges the month's current records into it with a Delta `MERGE` on the business key (null-safe): new keys are inserted, a known key is updated only when its stored `_row_hash` (SHA-256 of the business columns, without `competencia`, `_ingest_ts`, `_source_file`) changed and the month is not older, and keys absent from the month are kept. `competencia`, `_ingest_ts` and `_source_file` are those of the key's last change; the first run creates the table, and re-running a month changes nothing. Each run writes counts per table to `build/silver-<competencia>.json`: rows in and out, duplicates removed, rows without key, invalid and all-zero CNPJs (and how many were dropped), per date column the non-empty source values and those that failed to parse (a non-empty value that became `null`), and the Delta operation metrics (`operation`, `table_version`, `rows_inserted`, `rows_updated`, `rows_unchanged`, `table_rows`).


## Tables

Files per release: `Empresas0-9`, `Estabelecimentos0-9`, `Socios0-9`, `Simples` and one zip per lookup table (37 files; see [`data-source.md`](data-source.md)).

### `empresas` (`Empresas*.zip`, 7 columns)

One row per CNPJ root (the first 8 digits), shared by the head office and all branches.

| # | Column | Target type | Nullable | Description |
|---|---|---|---|---|
| 1 | `cnpj_basico` | string | no | CNPJ root: first 8 digits of the CNPJ |
| 2 | `razao_social` | string | yes | Legal name of the company |
| 3 | `natureza_juridica` | string | yes | Legal nature code (lookup `naturezas`) |
| 4 | `qualificacao_responsavel` | string | yes | Qualification code of the person responsible for the company (lookup `qualificacoes`) |
| 5 | `capital_social` | decimal(20,2) | yes | Share capital in BRL; comma as decimal separator in the source |
| 6 | `porte_empresa` | string | yes | Company size: `00` not informed, `01` micro, `03` small (EPP), `05` other |
| 7 | `ente_federativo_responsavel` | string | yes | Responsible federative entity, only for legal natures `1xxx` (public bodies), e.g. `UNIAO`, `BAHIA`, `SAO PAULO - SP` |

### `estabelecimentos` (`Estabelecimentos*.zip`, 30 columns)

One row per full CNPJ (`cnpj_basico` + `cnpj_ordem` + `cnpj_dv`), head office or branch.

| # | Column | Target type | Nullable | Description |
|---|---|---|---|---|
| 1 | `cnpj_basico` | string | no | CNPJ root: first 8 digits |
| 2 | `cnpj_ordem` | string | no | Establishment number: 9th to 12th digits |
| 3 | `cnpj_dv` | string | no | Check digits: last 2 digits |
| 4 | `identificador_matriz_filial` | string | yes | `1` head office (matriz), `2` branch (filial) |
| 5 | `nome_fantasia` | string | yes | Trade name |
| 6 | `situacao_cadastral` | string | yes | Registration status: `01` annulled, `02` active, `03` suspended, `04` unfit (inapta), `08` closed (baixada) |
| 7 | `data_situacao_cadastral` | date | yes | Date of the registration status event |
| 8 | `motivo_situacao_cadastral` | string | yes | Reason code of the registration status (lookup `motivos`) |
| 9 | `nome_cidade_exterior` | string | yes | City name abroad |
| 10 | `pais` | string | yes | Country code (lookup `paises`) |
| 11 | `data_inicio_atividade` | date | yes | Date the activity started |
| 12 | `cnae_fiscal_principal` | string | yes | Main economic activity, CNAE (lookup `cnaes`) |
| 13 | `cnae_fiscal_secundaria` | array&lt;string&gt; | yes | Secondary CNAE codes, comma-separated in the source |
| 14 | `tipo_logradouro` | string | yes | Street type (`RUA`, `AVENIDA`...) |
| 15 | `logradouro` | string | yes | Street name |
| 16 | `numero` | string | yes | Street number; `S/N` when there is none |
| 17 | `complemento` | string | yes | Address complement |
| 18 | `bairro` | string | yes | Neighbourhood |
| 19 | `cep` | string | yes | Postal code (CEP), 8 digits |
| 20 | `uf` | string | yes | State (UF) abbreviation |
| 21 | `municipio` | string | yes | Municipality code of the Receita Federal (lookup `municipios`; **not** the IBGE code) |
| 22 | `ddd_1` | string | yes | Area code of phone 1 |
| 23 | `telefone_1` | string | yes | Phone 1 |
| 24 | `ddd_2` | string | yes | Area code of phone 2 |
| 25 | `telefone_2` | string | yes | Phone 2 |
| 26 | `ddd_fax` | string | yes | Area code of the fax |
| 27 | `fax` | string | yes | Fax |
| 28 | `correio_eletronico` | string | yes | E-mail of the taxpayer |
| 29 | `situacao_especial` | string | yes | Special status of the company |
| 30 | `data_situacao_especial` | date | yes | Date the company entered the special status |

### `socios` (`Socios*.zip`, 11 columns)

Partners (QSA), one row per partner of a company. CPFs come masked by the source (`***240659**`), as required by Law 13,473/2017 (art. 129 § 2).

| # | Column | Target type | Nullable | Description |
|---|---|---|---|---|
| 1 | `cnpj_basico` | string | no | CNPJ root: first 8 digits |
| 2 | `identificador_socio` | string | yes | Partner type: `1` legal entity, `2` individual, `3` foreigner |
| 3 | `nome_socio_razao_social` | string | yes | Name of the individual partner or legal name of the partner company |
| 4 | `cpf_cnpj_socio` | string | yes | Masked CPF or CNPJ of the partner; empty for foreigners |
| 5 | `qualificacao_socio` | string | yes | Partner qualification code (lookup `qualificacoes`) |
| 6 | `data_entrada_sociedade` | date | yes | Date the partner joined the company |
| 7 | `pais` | string | yes | Country code of a foreign partner (lookup `paises`) |
| 8 | `representante_legal` | string | yes | Masked CPF of the legal representative (`***000000**` when there is none) |
| 9 | `nome_do_representante` | string | yes | Name of the legal representative |
| 10 | `qualificacao_representante_legal` | string | yes | Qualification code of the legal representative (lookup `qualificacoes`) |
| 11 | `faixa_etaria` | string | yes | Age group: `1` 0-12 years, `2` 13-20, `3` 21-30, `4` 31-40, `5` 41-50, `6` 51-60, `7` 61-70, `8` 71-80, `9` over 80, `0` not applicable |

### `simples` (`Simples.zip`, 7 columns)

Simples Nacional and MEI tax regime options, one row per CNPJ root.

| # | Column | Target type | Nullable | Description |
|---|---|---|---|---|
| 1 | `cnpj_basico` | string | no | CNPJ root: first 8 digits |
| 2 | `opcao_pelo_simples` | string | yes | Opted for Simples Nacional: `S` yes, `N` no, empty other |
| 3 | `data_opcao_simples` | date | yes | Date of the Simples Nacional option |
| 4 | `data_exclusao_simples` | date | yes | Date of the exclusion from Simples Nacional |
| 5 | `opcao_mei` | string | yes | Opted for MEI (individual micro-entrepreneur): `S` yes, `N` no, empty other |
| 6 | `data_opcao_mei` | date | yes | Date of the MEI option |
| 7 | `data_exclusao_mei` | date | yes | Date of the exclusion from MEI |

## Quality rules

`cnpj_lakehouse/quality.py` (`make quality`, RF-03; tests in `tests/test_quality.py`) checks the silver tables (the whole current state, ADR 011) after the run of a month with declarative rules and writes `build/quality-<competencia>/quality.md` and `quality.json` (rows checked and failures per rule, never data). An `error` failure blocks the pipeline (exit code 1); a `warn` failure is reported only. Null values are counted only by `not_null` rules; the other row rules ignore them.

| Rule | Tables and columns | Valid values (source) | Severity |
|---|---|---|---|
| `not_null` | business key columns (nullable = no above): `cnpj_basico`, `cnpj_ordem`, `cnpj_dv`, `codigo` | - | error |
| `not_null` | `empresas.razao_social`, `natureza_juridica`; `estabelecimentos.identificador_matriz_filial`, `situacao_cadastral`, `uf`, `municipio`, `cnae_fiscal_principal`; `socios.identificador_socio`, `nome_socio_razao_social`; lookups `descricao` | - | warn |
| `unique` | the silver business key of every table (see "Silver keys" above) | - | error |
| `pattern` | `cnpj_basico` (`^[0-9A-Z]{8}$`), `cnpj_ordem` (`^[0-9A-Z]{4}$`), `cnpj_dv` (`^[0-9]{2}$`) | layout + alphanumeric CNPJ (ADR 009) | error |
| `pattern` | `estabelecimentos.cep` (`^[0-9]{8}$`) | layout | warn |
| `domain` | `estabelecimentos.uf` | 26 states + `DF` + `EX` (abroad) | error |
| `domain` | `estabelecimentos.situacao_cadastral` (`01`, `02`, `03`, `04`, `08`), `identificador_matriz_filial` (`1`, `2`); `empresas.porte_empresa` (`00`, `01`, `03`, `05`); `socios.identificador_socio` (`1`, `2`, `3`) | this dictionary (official layout) | error |
| `domain` | `socios.faixa_etaria` (`0`-`9`); `simples.opcao_pelo_simples`, `opcao_mei` (`S`, `N`) | this dictionary | warn |
| `reference` | `empresas.natureza_juridica` -> `naturezas`, `qualificacao_responsavel` -> `qualificacoes`; `estabelecimentos.municipio` -> `municipios`, `cnae_fiscal_principal` -> `cnaes`, `motivo_situacao_cadastral` -> `motivos`, `pais` -> `paises`; `socios.qualificacao_socio`, `qualificacao_representante_legal` -> `qualificacoes`, `pais` -> `paises` | silver lookup table of the same month (`codigo`); skipped when it has no rows | warn |
| `is_true` | `estabelecimentos.cnpj_valido` | ADR 010: invalid CNPJs are kept flagged, so a metric, not a blocker | warn |

Closed code lists come from this dictionary; open code lists (municipalities, CNAEs, legal natures, qualifications, countries, reasons) come from the lookup tables, which the Receita Federal updates with each release.

### Lookup tables (2 columns each)

All six share the same layout: `codigo` (string, not nullable: the code, with leading zeros) and `descricao` (string: its name).

| Table | File | Rows (2026-09) | Code example | Referenced by |
|---|---|---:|---|---|
| `cnaes` | `Cnaes.zip` | 1,359 | `0111301` Cultivo de arroz | `estabelecimentos.cnae_fiscal_principal`, `cnae_fiscal_secundaria` |
| `motivos` | `Motivos.zip` | 63 | `01` EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA | `estabelecimentos.motivo_situacao_cadastral` |
| `municipios` | `Municipios.zip` | 5,572 | `0001` GUAJARA-MIRIM | `estabelecimentos.municipio` |
| `naturezas` | `Naturezas.zip` | 91 | `2038` Sociedade de Economia Mista | `empresas.natureza_juridica` |
| `paises` | `Paises.zip` | 255 | `013` AFEGANISTAO | `estabelecimentos.pais`, `socios.pais` |
| `qualificacoes` | `Qualificacoes.zip` | 68 | `05` Administrador | `empresas.qualificacao_responsavel`, `socios.qualificacao_socio`, `socios.qualificacao_representante_legal` |

## Evidence from the 2026-09 release

Checked on 2026-10-16 without downloading the large files: the six lookup zips were downloaded whole (72 KB) to a temporary folder outside the repository; for `Empresas1`, `Estabelecimentos1`, `Socios1` and `Simples` only the first 256 KB to 1 MB were requested with an HTTP `Range` header and the beginning of the deflate stream after the zip local file header was decompressed. The last (truncated) line was discarded.

| File | Rows inspected | Fields per row | UTF-8 valid? | Notes |
|---|---:|---|---|---|
| `Empresas1.zip` | 61,939 | 7 (all rows) | no | `capital_social` always `\d+,\d+`; 22 rows with doubled quotes, 4 with `;` inside a value |
| `Estabelecimentos1.zip` | 16,783 | 30 (all rows) | no | 17 rows with doubled quotes, 117 with `;` inside a value |
| `Socios1.zip` | 43,000 | 11 (all rows) | yes (ASCII only in the sample) | masked CPFs `***999999**` |
| `Simples.zip` | 156,278 | 7 (all rows) | yes (ASCII only) | `00000000` for missing dates |
| `Cnaes.zip` | 1,359 (all) | 2 | no | |
| `Motivos.zip` | 63 (all) | 2 | yes (ASCII only) | |
| `Municipios.zip` | 5,572 (all) | 2 | yes (ASCII only) | |
| `Naturezas.zip` | 91 (all) | 2 | no | |
| `Paises.zip` | 255 (all) | 2 | no | |
| `Qualificacoes.zip` | 68 (all) | 2 | no | |

Field counts were taken by splitting each row on `";"` after removing the outer quotes, so a `;` inside a value does not inflate them. No raw data is stored in the repository: the unit tests use synthetic rows in the same format (`tests/test_schemas.py`).
