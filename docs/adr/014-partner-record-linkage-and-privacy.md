# 014 - Partner record linkage: blocking, Jaro-Winkler and privacy of the outputs

- **Status:** accepted
- **Date:** 2026-10-25

## Context

RF-10 (Could) asks for candidate pairs of partners (`socios`) with similar names, a similarity score and a manually reviewed sample. The `socios` table holds natural-person names next to CPFs that the source masks (`***123456**`: six middle digits visible). The repository, its CI logs, job summaries and artifacts are public, and RST-01 / LGPD forbid re-identifying people or republishing raw rows. Comparing every pair of the millions of partner rows is impossible (a cartesian product), and a name alone is a weak key: many people share common first and last names.

## Decision

1. **Scope:** individual partners only (`identificador_socio = 2`); the question is technical ("do these two rows look like the same record?"), never "who is this person". No CPF is unmasked and no other dataset is joined.
2. **Normalization:** upper case, accents stripped, non-letters to spaces, particles `DA DE DO DAS DOS E D` dropped, spaces collapsed; token sorting is an option (`--sort-tokens`).
3. **Blocking:** first token + last token of the normalized name + the six masked CPF digits the source already publishes (`--no-cpf-block` drops the digits). Pairs only inside a block (self-join on the key with `left.row_id < right.row_id`: no cartesian product, no self-pair, each pair once). Blocks over 200 rows (`--max-block-size`) are skipped and counted.
4. **Score:** Jaro-Winkler on the normalized names (a small pure-Python function applied as a Spark UDF; no new dependency, pandas/pyarrow are not runtime dependencies), with the Levenshtein ratio from Spark's built-in `levenshtein` stored alongside. Jaro-Winkler favours a shared prefix and tolerates a dropped middle name better than edit distance, which suits Portuguese names whose first and last tokens are already equal inside a block.
5. **Bands:** `match` >= 0.92, `review` in [0.85, 0.92), `non_match` below; both configurable (`--match`, `--review`).
6. **Outputs and privacy:**
   - Delta table `gold/linkage_candidates` in the local lakehouse (MinIO, never uploaded): `competencia`, `block_id` (SHA-256 of the block key), `block_size`, `left_row_id` / `right_row_id` (SHA-256 of the silver business key), `left_cnpj_basico` / `right_cnpj_basico`, `score`, `levenshtein_ratio`, `band`. **No names, no CPF digits.** Overwritten on each run (derived data).
   - Report `build/linkage/linkage-<competencia>.json|md`: aggregates only (partners, blocks, skipped blocks, candidate pairs per band, score histogram). This is the only real-data output that may be published.
   - Review sample (`--review-sample N`): N seeded random pairs of a band **with names** (no CPF), written only below `build/linkage-review/`. The writer refuses any other path and, when Git is available, any path `git check-ignore` does not confirm as ignored. Thales labels it locally (`match` / `no_match`); `linkage precision` publishes only the aggregate precision with a 95% Wilson interval.
   - CI never runs linkage on real data (the `bronze` job has no `Socios` file); tests use synthetic names, synthetic masked CPFs and the verified synthetic CNPJ bases.

## Alternatives considered

- **Cartesian product or sorted-neighbourhood on all names:** quadratic or still too many pairs; blocking on first + last token + masked digits keeps blocks small.
- **Blocking without the masked digits:** far more false candidates (common names); kept as an option for comparison only.
- **A record-linkage library (Splink, recordlinkage, jellyfish):** a heavier dependency for a Could item; a 30-line Jaro-Winkler is enough and is unit-tested against reference values.
- **pandas UDF / `mapInPandas`:** faster, but makes pandas and pyarrow runtime dependencies of the Spark image; the plain UDF runs only on the (small) candidate pairs.
- **Names in the gold table:** convenient for review, but a second copy of personal data; the row keys join back to silver locally when needed.

## Consequences

- Linkage is reproducible with `make linkage` (after `make silver` with `socios`); scores and bands are deterministic, the review sample is seeded.
- Only aggregates leave the machine; the published RF-10 evidence is the synthetic tests, the method and, once Thales reviews a local sample, the aggregate precision.
- Pairs whose first or last token differs (typo in the first name, a dropped last name) are never compared: recall is traded for small blocks and fewer false matches. Precision on real data is unknown until the manual review is done.
- The thresholds are a starting point taken from the synthetic examples; they should be revisited with the real-data precision.
