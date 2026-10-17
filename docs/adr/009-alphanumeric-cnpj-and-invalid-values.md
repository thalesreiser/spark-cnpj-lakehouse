# 009 - Silver normalization supports the alphanumeric CNPJ; invalid values become null

- **Status:** accepted
- **Date:** 2026-10-17

## Context

RF-03 types and normalizes the silver layer, and RN-01 defines the establishment business key as CNPJ base (8) + order (4) + check digits (2). Bronze keeps every value as a string (ADR 008), so silver must decide what to do with masked, short or invalid identifiers, `0`/`00000000` dates and comma decimals.

The Receita Federal introduced the **alphanumeric CNPJ**, assigned progressively to new registrations from July 2026 ([news, 2024-10](https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2024/outubro/cnpj-tera-letras-e-numeros-a-partir-de-julho-de-2026); [Q&A](https://www.gov.br/receitafederal/pt-br/centrais-de-conteudo/publicacoes/perguntas-e-respostas/cnpj/cnpj-alfanumerico.pdf); [Serpro, check-digit calculation](https://www.serpro.gov.br/menu/noticias/videos/calculodvcnpjalfanaumerico.pdf)):

- the CNPJ keeps 14 characters; the first 12 (base + order) accept `0-9` and upper-case `A-Z`; the last 2 (check digits) stay numeric;
- the check digits keep the module 11 rule (weights 2 to 9 from right to left), with each character worth `ascii(char) - 48` (digits keep their value, `A` = 17, `B` = 18...); a remainder of 0 or 1 gives 0;
- the official example is `12.ABC.345/01DE-35`; existing numeric CNPJs do not change.

Code that strips "non-digits" or casts the CNPJ to a number would silently destroy these keys.

In Spark 3.5, `to_date(value, 'yyyyMMdd')` and even `try_to_timestamp` **raise** on impossible dates (`20251340`) when `spark.sql.ansi.enabled=true`, and raise `SparkUpgradeException` on values such as `2025011` with the default `spark.sql.legacy.timeParserPolicy=EXCEPTION` (checked locally with PySpark 3.5.3).

## Decision

1. Normalization lives in `cnpj_lakehouse.normalize` as pure `Column -> Column` functions (RNF-04); the silver job composes them.
2. CNPJ: remove masks (`.`, `/`, `-`, whitespace), upper-case, **keep letters** in the first 12 characters, require numeric check digits, compute them with `ascii - 48` and module 11. Purely numeric values are left-padded with zeros (sources that lost leading zeros); alphanumeric values must already have full width, since padding them would be a guess.
3. Invalid values become `null` in the normalized column; `is_valid_cnpj` returns a boolean flag (null only for a null input) so the quality checks of RF-03 can count them. `normalize_cnpj` validates the check digits by default; `build_cnpj` (from the three bronze parts) only checks the format, leaving the decision to keep or reject rows with wrong check digits to the silver job and its quality report.
4. Dates are built from validated year/month/day parts with `make_date`, without any datetime parser, so the result is the same with ANSI mode on or off and with any parser policy.
5. Secondary CNAE lists: missing or empty becomes an empty array (the source cannot tell "none" from "not informed"); invalid codes are dropped and duplicates removed.

## Alternatives considered

- **Numeric-only CNPJ (`regexp_replace('[^0-9]', '')`, `bigint`):** simpler, but corrupts every alphanumeric CNPJ issued from July 2026.
- **Fail the job on invalid values:** one bad row would stop a 60-million-row load; counting them in the quality checks is more useful.
- **`to_date` with `spark.sql.ansi.enabled=false` forced in the session:** works only as long as nobody changes the configuration, and still depends on the parser policy.

## Consequences

- Silver keys are correct for numeric and alphanumeric CNPJs; the tests include the official alphanumeric example.
- Invalid values are not lost silently: bronze keeps the original and the flag functions let silver count rejections.
- The CNPJ check-digit expression is a sum over 12 characters; it is evaluated per row, without UDFs, so it stays in the JVM (no Python serialization).
- If the Receita Federal restricts the set of letters (some technical notes mention excluding letters such as I, O, U, Q, F), only the `[0-9A-Z]` patterns in `normalize.py` need to change.
