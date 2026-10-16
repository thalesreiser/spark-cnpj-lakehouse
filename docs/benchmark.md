# Benchmark (RF-08, RNF-01)

Runs execute on GitHub Actions runners (see [ADR 006](adr/006-ci-as-execution-environment.md)): each measurement is repeated 3 times and the median is reported, together with the runner specification.

| Metric | Baseline | After tuning | Change |
|---|---|---|---|
| Main job runtime | | | |
| Shuffle read/write | | | |
| Spill (memory/disk) | | | |
| Files per partition | | | |
