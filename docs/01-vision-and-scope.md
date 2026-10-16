# 01 - Vision and scope

**Problem:** process large volumes of company registry data in a distributed, reliable and performant way, with an auditable history.

**Goal:** build a layered lakehouse (bronze, silver, gold) with Spark and Delta Lake on the Brazilian Federal Revenue Service (Receita Federal) CNPJ open data, documenting the performance gain obtained through optimization.

**Data source:** Receita Federal CNPJ open data: companies, establishments, partners and lookup tables. Confirm the current volume, layout and URL before starting (R-04).

| In scope | Out of scope |
|---|---|
| Batch ingestion, Delta layers, incremental MERGE, optimization, benchmark, tests | Streaming, dashboards, deployment on a managed cluster |

## Success metrics (targets)
- Complete pipeline reproducible with a single command
- At least 30% reduction in the main job runtime after tuning, proven with Spark UI data
- At least 80% test coverage on the transformations

## Stakeholders (simulated roles)
| Role | Main interest |
|---|---|
| Technical recruiter / manager | Clear evidence of stack, quality and autonomy within 5 minutes of reading |
| Code reviewer (engineer) | Tested, organized, reproducible code with justified decisions |
| Data consumer (analyst) | Reliable, documented data that is easy to query |
| Thales (developer and operator) | Low cost, reproducible runs, simple maintenance |
