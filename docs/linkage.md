# Partner record linkage (RF-10)

A simplified, **technical** record-linkage exercise on the `socios` table: do two partner rows with similar names (and the same masked CPF digits) look like the same record? It never tries to find out who a person is (RST-01, [`USAGE-POLICY.md`](../USAGE-POLICY.md)). Decision and alternatives: [ADR 014](adr/014-partner-record-linkage-and-privacy.md). Code: `src/cnpj_lakehouse/linkage.py`; tests: `tests/test_linkage.py`.

## Method

| Step | Rule | Option |
|---|---|---|
| Scope | Individual partners only (`identificador_socio = 2`); identical rows collapse to one `row_id` (SHA-256 of the silver business key) | - |
| Normalization | Upper case, accents stripped, non-letters to spaces, particles `DA DE DO DAS DOS E D` dropped, spaces collapsed: `" Maria  da Silva-Teste "` → `MARIA SILVA TESTE` | `--sort-tokens` (word order ignored) |
| Blocking | First token \| last token \| six masked CPF digits (`***000111**` → `000111`); pairs only inside a block, each pair once, never a row with itself; blocks over 200 rows skipped and counted | `--no-cpf-block`, `--max-block-size` |
| Score | Jaro-Winkler on the normalized names (`score`); Levenshtein ratio `1 - distance / max length` stored alongside | - |
| Bands | `match` ≥ 0.92; `review` 0.85–0.92; `non_match` below | `--match`, `--review` |

## Outputs and privacy rules

| Output | Where | Content | Publishable |
|---|---|---|---|
| `gold/linkage_candidates` (Delta) | local lakehouse (MinIO) | row keys (hashes), `cnpj_basico` of each side, block id (hash), block size, scores, band; **no names, no CPF digits** | no (stays local) |
| `linkage-<competencia>.json` / `.md` | `build/linkage/` | partners, blocks, skipped blocks, pairs per band, score histogram | **yes** (aggregates only) |
| Review sample | `build/linkage-review/` (git-ignored) | N random pairs with **names** (no CPF), empty `label` column | **never** |
| `precision.json` | `build/linkage/` | sampled, labeled, matches, precision, 95% Wilson interval | **yes** |

- The review writer refuses any path outside `build/linkage-review/` and any path Git does not ignore.
- CI never runs linkage on real data: the `bronze` job has no `Socios` file, and tests use synthetic names, synthetic masked CPFs and the verified synthetic CNPJ bases (80300910–80300929).
- Never paste names from the review sample into an issue, PR, commit, job summary or chat; do not unmask CPFs or join the sample with other data.

## Running it

```bash
make up
make download COMPETENCIA=2026-09 FILES="lookups Socios1"
make bronze COMPETENCIA=2026-09 TABLES=socios
make silver COMPETENCIA=2026-09 TABLES=socios
make linkage COMPETENCIA=2026-09                        # gold/linkage_candidates + build/linkage/
make linkage COMPETENCIA=2026-09 LINKAGE_ARGS="--review-sample 50"   # + local review sample
```

## Manual review procedure (local, by Thales)

1. Run `make linkage ... LINKAGE_ARGS="--review-sample 50"` (add `--review-band match` to audit the match band instead). The sample lands in `build/linkage-review/review-<competencia>.csv` (`;`-separated, UTF-8).
2. Open the file locally and fill `label` with `match` (the two rows look like the same record: same name up to typos, abbreviations or a dropped middle name) or `no_match` (clearly different names). Leave it blank when unsure; blank rows are not counted. Judge only from the two strings: no lookups elsewhere.
3. Run `make linkage-precision REVIEW=build/linkage-review/review-<competencia>.csv`. It prints and writes `build/linkage/precision.json` with counts and precision only.
4. Publish only that aggregate (e.g. "review band, 50 pairs, precision 0.72, 95% CI 0.58–0.83") in [05 - Validation](05-validation.md); delete the sample when done if you like (`build/` is never committed).

## Worked example (synthetic)

Five pairs of the review band from synthetic partners (fake names, masked CPF `***000111**`, bases 80300910–80300929). Scores are the real output of `jaro_winkler` on the normalized names; the labels are what a reviewer would write.

| Pair | Left (normalized) | Right (normalized) | Score | Band | Label |
|---|---|---|---|---|---|
| 1 | PAULO TESTE NUNES | PAULO NUNES | 0.8930 | review | match (middle name dropped) |
| 2 | ANA MODELO PEREIRA | ANA PEREIRA | 0.8859 | review | match (middle name dropped) |
| 3 | ANA MODELO PEREIRA | ANA MODELO FICTICIA PEREIRA | 0.9000 | review | match (extra middle name) |
| 4 | PAULO TESTE NUNES | PAULO AMOSTRA NUNES | 0.8621 | review | no_match (other middle name) |
| 5 | JOSE EXEMPLO SOUZA | JOSE ENSAIO SOUZA | 0.8743 | review | no_match (other middle name) |

`linkage precision` on these labels gives 3 / 5 = **0.60** (95% Wilson interval ≈ 0.23–0.88: five pairs say little, which is why the real review should label at least 50). For reference, the same synthetic names score `MARIA TESTE SILVA` × `MARIA TESTI SILVA` = 0.9577 (`match`, one-letter typo) and `MARIA TESTE SILVA` × `MARIA FICTICIA SILVA` = 0.8291 (`non_match`).

## Status and limitations

- Proven on synthetic data by `tests/test_linkage.py` (normalization, blocking, scores, thresholds, oversized blocks, no names in the table or report, private-path guard, precision); CI `lint-test`.
- **Real-data precision is pending** Thales's local review; until then RF-10 has no real-data quality figure.
- Recall is limited by design: pairs whose first or last token differs, or whose masked digits differ, are never compared.
