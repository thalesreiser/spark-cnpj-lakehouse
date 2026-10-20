# 012 - Gold aggregations: counted unit, invalid CNPJs, nulls and full overwrite

- **Status:** accepted
- **Date:** 2026-10-19

## Context

RF-05 (Must) asks for gold tables of companies by CNAE/state/year, size and registration status whose totals reconcile with silver. The source splits a company (`empresas`, one row per `cnpj_basico`) from its establishments (`estabelecimentos`, head office and branches, one row per full CNPJ). CNAE, state, opening date and status are establishment attributes; size is a company attribute. Silver keeps invalid CNPJs flagged (ADR 010) and holds the current state of every key (ADR 011). The CI `bronze` job loads `lookups` + `Empresas1` only: no `Estabelecimentos` file (each one is 336-369 MB zipped, `Estabelecimentos0` 2.2 GB).

## Decision

1. **Unit.** `gold_empresas_cnae_uf_ano` and `gold_situacao_setor` count **establishments** (plus `estabelecimentos_ativos`, status `02`); `gold_distribuicao_porte` counts **companies**. The `empresas` column of an establishment table is a distinct count of `cnpj_basico` within the row and is documented as non-additive.
2. **Invalid CNPJs are counted** by default, consistent with ADR 010 (silver keeps them so gold reconciles with the source); `--exclude-invalid-cnpj` leaves them out, and the run summary always reports how many there are.
3. **Nulls are kept as their own group**: a null code or opening date is a null key in the grouping, and a code missing from a lookup has a null description. No row is dropped, so gold sums equal the eligible silver rows.
4. **Sum checks in every run.** The job compares each additive gold total with the count of eligible silver rows it aggregates (and the size shares with 1) and exits with code 1 on any mismatch. Establishments whose company is not in silver `empresas` are left out of `gold_distribuicao_porte` and reported (`without_company`), so that check is "eligible minus without company".
5. **Write strategy.** Each gold table is a whole-table Delta overwrite (`mode("overwrite")`, `overwriteSchema`), not partitioned: gold is derived from the silver current state, a re-run gives the same table, and the tables are orders of magnitude smaller than silver. Each run is one Delta version, so earlier results stay readable by time travel.
6. **CI data.** The CI `bronze` job runs `make gold` on the real subset, which builds `gold_distribuicao_porte` (the establishment tables are skipped with a reason); the two establishment tables run end to end through the real Delta stack in `tests/test_gold_delta.py` (synthetic silver: the fixture release plus a few synthetic establishments). Adding `Estabelecimentos1` to the CI subset was rejected for now: about four times the download and processing of `Empresas1` in a job already at ~25 of its 60 minutes (plus its silver re-run), and nothing guarantees that `Estabelecimentos1` holds the establishments of the companies in `Empresas1`.

## Alternatives considered

- **Count companies everywhere** (e.g. by the head office's CNAE and state): loses branches in other states and sectors, and needs a head office rule for companies without one in the loaded files.
- **Drop invalid CNPJs or null groups:** cleaner tables, but the totals would no longer match silver nor the source, and the loss would be invisible in gold.
- **Incremental gold with MERGE:** more code for no gain at this size; a full recomputation from the current state is simple and idempotent.
- **`Estabelecimentos1` in CI:** real numbers for every table, at the cost of ~15-20 more minutes per run; can be done in a manual workflow (ADR 006).

## Consequences

- Consumers must not sum `empresas` across the rows of an establishment table; the dictionary marks additive and non-additive measures.
- The silver x gold reconciliation test (task 5.2) can rely on these rules and on the counts of the run summary.
- On CI, the establishment tables are proven on synthetic data only; on real data they need a run that loads `Estabelecimentos` (local Docker or a manual workflow).
