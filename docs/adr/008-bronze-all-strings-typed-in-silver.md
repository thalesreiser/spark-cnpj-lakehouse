# 008 - Bronze reads every column as a string; the documented types are applied in silver

- **Status:** accepted
- **Date:** 2026-10-16

## Context

RF-02 asks for the CSV files to be ingested into bronze "with an explicit schema", and RF-03 for a typed silver layer where "100% of valid dates are typed as `date`". The official layout of the CNPJ open data ([`cnpj-metadados.pdf`](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf), 2021-12-02) gives the columns in file order with descriptions, but no data types. The files are headerless, Latin-1, `;`-separated, with every field quoted, and their values do not fit native types directly (details in [`data-dictionary.md`](../data-dictionary.md)):

- dates are `yyyyMMdd` with `0`, `00000000` or empty meaning "no date";
- `capital_social` uses a comma as the decimal separator (`120000000000,00`);
- codes carry leading zeros (`0111301`, `05`, `013`) and placeholders such as `8888888`;
- `cnae_fiscal_secundaria` is a comma-separated list.

If Spark casts while reading (e.g. `DateType` with `dateFormat=yyyyMMdd`), a value that does not parse is silently turned into `null` in `PERMISSIVE` mode, or fails the whole job in `FAILFAST` mode. Either way the bronze layer would stop being a faithful copy of the source, and the RF-02 check (source row count = target row count, with the same content) could not tell a source `null` from a cast failure.

## Decision

1. Describe every table once, column by column, in `cnpj_lakehouse.schemas`: a `ColumnSpec` with the snake_case name, the documented **target type**, a description and whether the typed column may be null. The order of the columns is the order of the file.
2. Derive two views from that single description:
   - **bronze:** `TableSpec.bronze_schema()` returns a `StructType` where every field is a **nullable `StringType`**; each field also carries its `description` and `target_type` as Spark field metadata, so they travel with the Delta table;
   - **silver:** `TableSpec.silver_types()` returns the target type of every column, which RF-03 applies with explicit, testable conversions (date parsing with the empty-date values, comma decimal, list split) and quality checks that count rejected values.
3. Target types: `string` for names, free text and **every code or identifier** (leading zeros, no arithmetic); `date` for the `data_*` columns; `decimal(20,2)` for `capital_social`; `array<string>` for `cnae_fiscal_secundaria`.
4. Keep the CSV reader options (`header=false`, `sep=;`, `encoding=ISO-8859-1`, `quote="`, `escape="`, `mode=PERMISSIVE`, `multiLine=false`) and the mapping from release file prefix (`Empresas`, `Cnaes`...) to table in the same module, so the bronze job has one place to read them from.
5. Version the schemas with the code (`SCHEMA_VERSION`, bumped when a table changes) and record the layout source and date (`LAYOUT_SOURCE`, `LAYOUT_DATE`).

## Alternatives considered

- **Typed schema already in bronze:** less code in silver, but cast failures become silent `null`s (or failed jobs) and bronze no longer reproduces the source; the comma decimal and the `0`/`00000000` dates would need pre-processing anyway.
- **`inferSchema=true`:** an extra pass over 28 GB of CSV, types that change between releases (a code column becomes `int` and loses its leading zeros), and no names, since the files have no header.
- **Schemas as JSON/YAML files:** language-neutral, but they would need their own loader and validation; Python dataclasses are type-checked by mypy, testable and still produce plain `StructType` objects.
- **Two hand-written `StructType`s per table (bronze and silver):** duplicated column lists that can drift apart; one spec with two derived views cannot.

## Consequences

- Bronze is lossless: every row and value of the source is kept as text (empty fields become `null`), and the row-count reconciliation of RF-02 is meaningful.
- Typing, and the decision of what to do with invalid values, moves to silver, where it can be unit-tested and measured by the quality checks (RF-03).
- Bronze storage is slightly larger than with native types; it is a raw layer read once per release, so this is acceptable.
- A layout change by the Receita Federal (risk R-04) is **not** reported by Spark in `PERMISSIVE` mode with a fixed schema: extra fields are dropped and missing ones become `null`. The bronze ingestion (task 2.3) should therefore check the field count of the source rows (e.g. a sample of raw lines, or `columnNameOfCorruptRecord`). Fixing a change means editing one `TableSpec` and bumping `SCHEMA_VERSION`.
- `escape="` is required: the source doubles quotes inside values, and Spark's default escape character (`\`) would mis-parse those rows.
