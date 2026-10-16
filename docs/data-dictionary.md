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

| Target type | Used for | Silver conversion |
|---|---|---|
| `string` | names, free text and **every code or identifier** (CNPJ parts, CNAE, municipality, legal nature, qualification, country, status, CEP, phones) | trim; codes keep their leading zeros (`0111301`, `05`, `013`) |
| `date` | every `data_*` column | `to_date(value, 'yyyyMMdd')`; `""`, `0` and `00000000` become `null` |
| `decimal(20,2)` | `capital_social` | replace `,` with `.` and cast; 18 integer digits leave room for outliers |
| `array<string>` | `cnae_fiscal_secundaria` | split on `,`; empty becomes an empty array or `null` (silver decision) |

"Nullable = no" marks the business key columns, which must be present in silver.

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
