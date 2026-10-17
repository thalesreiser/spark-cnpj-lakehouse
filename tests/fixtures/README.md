# Test fixtures

`release/2026-09/` is a tiny **synthetic** CNPJ release laid out like a `make download` output
(extracted CSVs + `manifest.json`), used by `tests/test_bronze_counts.py`. The CSVs keep the real
file format byte for byte (ISO-8859-1, `;`, every field quoted, LF line endings; see
`.gitattributes`). Do not open and re-save them in an editor that changes the encoding.

## No personal data (RST-01)

- Every record is invented: company names are fictitious business names and no field holds a
  CPF, a person's name or an address.
- The CNPJ roots (`cnpj_basico`) are digits-only and taken from the block **`80300910`–`80300929`**
  (in use: `80300911`–`80300916` and `80300921`–`80300924`). The block was chosen from the free
  blocks reported by `scripts/check_fixture_cnpjs.py` after the previous `9000xxxx` roots were
  found to collide with the real registry.
- Absence was **verified against release `2026-09` on 2026-10-17** by the `Fixture privacy`
  workflow (`.github/workflows/fixture-privacy.yml`): it downloads every real `Empresas*` file,
  which lists every root of the registry, and fails if any fixture root is present. Only a
  counts summary is uploaded as an artifact.
- The Receita Federal could assign these roots in the future. The check runs again on every pull
  request that touches `tests/fixtures/**`, on demand (`workflow_dispatch`) and monthly
  (`schedule`, against the previous month's release). Scheduled workflows only run from the
  default branch, so the monthly check is active once the workflow is on `main`. If it fails,
  pick another block from the suggested free blocks in its log and update the fixture.

Run the check locally after `make download COMPETENCIA=<YYYY-MM> FILES=empresas` (~1.4 GB):

```
python scripts/check_fixture_cnpjs.py --competencia <YYYY-MM>
```

## Manifest

`manifest.json` uses the `cnpj_lakehouse.download` format, so `load_manifest` and
`FileRecord.from_dict` read it unchanged. Unlike a real download, where `size_bytes` and
`sha256` describe each zip, here they describe the **extracted CSV** of each entry (the fixture
has no zips). Recompute them whenever a CSV changes.
