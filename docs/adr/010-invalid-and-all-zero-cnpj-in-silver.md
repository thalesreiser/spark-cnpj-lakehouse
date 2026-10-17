# 010 - Silver keeps establishments with an invalid CNPJ flagged; the all-zero CNPJ is invalid

- **Status:** proposed
- **Date:** 2026-10-17

## Context

RF-03 builds a deduplicated silver layer, and RN-01 defines the establishment business key as CNPJ base + order + check digits. ADR 009 left two questions to the silver job:

1. **Check digits that do not match.** `is_valid_cnpj` (task 3.1) tells whether the two check digits of a well-formed CNPJ match the module 11 rule. The Receita Federal is the authoritative registry, so a mismatch is either a source anomaly or a bug in our parsing. Silver can keep such rows (with a flag) or drop them.
2. **All-zero CNPJ.** `00000000000000` passes module 11 (every sum is 0, so both check digits are 0), but it is a placeholder, not a registration: establishment orders start at `0001`. Note that the all-zero **base** is legitimate: `00000000` is Banco do Brasil, whose head office is `00.000.000/0001-91`. Only the full 14-character all-zero value is in question.

Rows whose key is **missing** after normalization (an empty or malformed `cnpj_basico`, `cnpj_ordem`, `cnpj_dv` or lookup `codigo`) are not part of this decision: they cannot be deduplicated nor joined, so silver always discards them and counts them (`rows_without_key`). Only `estabelecimentos` carries the full CNPJ; `empresas`, `socios` and `simples` carry only the base, which has no check digits.

## Decision

Proposed defaults, each a one-line switch in `cnpj_lakehouse/silver.py`:

1. `INVALID_CNPJ_POLICY = InvalidCnpjPolicy.FLAG`: an establishment whose CNPJ fails validation is **kept**, with `cnpj_valido = false`, and counted (`invalid_cnpj`). `--invalid-cnpj drop` (or `InvalidCnpjPolicy.DROP`) discards those rows instead and counts them (`invalid_cnpj_dropped`).
2. `ZERO_CNPJ_IS_INVALID = True`: `00000000000000` is treated as invalid (`cnpj_valido = false`, counted in `invalid_cnpj` and separately in `zero_cnpj`), so it follows the policy of item 1.

Both outcomes are always counted in the run summary (`build/silver-<competencia>.json`), whatever the switches.

## Alternatives considered

- **Drop invalid CNPJs (quarantine count only):** silver would hold only valid keys, but it would silently diverge from the official registry, break the reconciliation of gold counts with the source (RF-05) and hide our own parsing bugs behind a "rejected" number. Can be enabled with one line if the reviewer prefers it.
- **Quarantine table (`silver/<table>_rejected`):** keeps the rejected rows queryable, but adds a table and a retention question for a case that, so far, has a count of zero in the data checked; bronze already keeps the original rows.
- **Treat the all-zero CNPJ as valid:** it passes module 11, but no establishment has order `0000`; keeping it valid would let a placeholder join with real data.

## Consequences

- `estabelecimentos` in silver has two extra columns: `cnpj` (14 characters, RN-01) and `cnpj_valido` (boolean). Consumers that need only valid keys filter `cnpj_valido`.
- Changing the decision is a one-line change (or a CLI flag) and does not change the schema.
- The quality report shows how many rows each choice affects, so the decision can be revisited with real numbers when `Estabelecimentos` runs on the full release.
